"""FastAPI wiring for the registry service."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import RegistryConfig
from .models import AssetRecord
from .service import RegistryService


class ReorgRequest(BaseModel):
    rollback_count: int
    replay: list[dict] = []


def create_app(config: RegistryConfig | None = None,
               service: RegistryService | None = None) -> FastAPI:
    if service is None:
        service = RegistryService(config or RegistryConfig())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.start()
        yield
        service.stop()

    app = FastAPI(title="chain-metadata-registry", lifespan=lifespan)
    app.state.service = service

    @app.get("/assets")
    def list_assets() -> dict:
        snap = service.list_assets()
        return {
            "version": snap.version,
            "assets": {aid: {"fields": dict(m.fields),
                             "provenance": dict(m.provenance)}
                       for aid, m in snap.assets.items()},
            "missing_sources": list(snap.missing_sources),
        }

    @app.get("/assets/{asset_id}")
    def get_asset(asset_id: str) -> dict:
        result = service.get_asset(asset_id)
        if result.asset is None:
            raise HTTPException(status_code=404, detail="asset not found")
        return {
            "asset_id": asset_id,
            "fields": dict(result.asset.fields),
            "provenance": dict(result.asset.provenance),
            "snapshot_version": result.snapshot_version,
            "stale": result.stale,
            "missing_sources": list(result.missing_sources),
        }

    @app.post("/refresh")
    def refresh() -> dict:
        snap = service.refresh()
        return {"version": snap.version,
                "missing_sources": list(snap.missing_sources)}

    @app.post("/reorg")
    def reorg(req: ReorgRequest) -> dict:
        records = [
            AssetRecord(asset_id=str(r["asset_id"]),
                        fields=dict(r.get("fields", {})),
                        source=str(r.get("source", "chain")),
                        entry_id=str(r.get("entry_id", "")))
            for r in req.replay
        ]
        try:
            record = service.reorg(req.rollback_count, records)
        except Exception as exc:
            from .store import RollbackWindowExceeded
            if isinstance(exc, RollbackWindowExceeded):
                raise HTTPException(status_code=409, detail=str(exc))
            raise
        return {
            "rolled_back": [e.record.entry_id for e in record.rolled_back],
            "replayed": [e.record.entry_id for e in record.replayed],
        }

    @app.get("/reorgs")
    def reorgs() -> dict:
        return {"reorgs": [
            {"rolled_back": [e.record.entry_id for e in r.rolled_back],
             "replayed": [e.record.entry_id for e in r.replayed],
             "at_height": r.at_height}
            for r in service.reorg_history()
        ]}

    @app.get("/health")
    def health() -> dict:
        return {"sources": {k: v.value for k, v in
                            service.source_status().items()}}

    return app
