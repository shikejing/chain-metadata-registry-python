"""FastAPI HTTP surface for the registry."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException

from .config import ServiceConfig
from .engine import DefaultRefreshEngine
from .service import RegistryService
from .store import FileSnapshotStore


def create_app(config: ServiceConfig, *, background_refresh: bool = True) -> FastAPI:
    store = FileSnapshotStore(config.store.path)
    engine = DefaultRefreshEngine(config, store)
    service = RegistryService(config, engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task: asyncio.Task | None = None
        if background_refresh and config.refresh.interval_seconds > 0:
            async def _loop() -> None:
                while True:
                    try:
                        await engine.refresh()
                    except Exception:
                        pass  # isolation: a failed loop never kills the service
                    await asyncio.sleep(config.refresh.interval_seconds)

            task = asyncio.create_task(_loop())
        try:
            yield
        finally:
            if task is not None:
                task.cancel()

    app = FastAPI(title="Chain Metadata Registry", lifespan=lifespan)

    def _stale_check() -> None:
        if config.refresh.stale_policy == "error":
            try:
                view = service.read_view()
            except LookupError:
                return
            if view.stale:
                raise HTTPException(status_code=503, detail="snapshot is stale")

    @app.get("/")
    def root() -> dict[str, Any]:
        return {"service": "chain-metadata-registry", "health": "/health"}

    @app.get("/health")
    def health() -> dict[str, Any]:
        return service.health()

    @app.get("/v1/snapshot")
    def get_snapshot() -> dict[str, Any]:
        try:
            return service.read_view().to_dict()
        except LookupError:
            raise HTTPException(status_code=404, detail="no committed snapshot")

    @app.get("/v1/assets")
    def list_assets() -> dict[str, Any]:
        _stale_check()
        try:
            return service.list_assets()
        except LookupError:
            raise HTTPException(status_code=404, detail="no committed snapshot")

    @app.get("/v1/assets/{asset_id}")
    def get_asset(asset_id: str) -> dict[str, Any]:
        _stale_check()
        try:
            payload = service.get_asset(asset_id)
        except LookupError:
            raise HTTPException(status_code=404, detail="no committed snapshot")
        if payload is None:
            raise HTTPException(status_code=404, detail=f"asset not found: {asset_id}")
        return payload

    @app.get("/v1/entries/{entry_id}")
    def get_entry(entry_id: str) -> dict[str, Any]:
        status = service.entry_status(entry_id)
        return status.to_dict()

    @app.get("/v1/rollbacks")
    def list_rollbacks() -> dict[str, Any]:
        try:
            return service.list_revocations()
        except LookupError:
            raise HTTPException(status_code=404, detail="no committed snapshot")

    @app.post("/v1/refresh")
    async def trigger_refresh() -> dict[str, Any]:
        outcome = await engine.refresh()
        return {
            "changed": outcome.changed,
            "rebuilt_assets": outcome.rebuilt_assets,
            "carried_assets": outcome.carried_assets,
            "revoked_count": outcome.revoked_count,
            "view": service.read_view().to_dict(),
        }

    return app


def load_config_and_app(config_path: str | Path) -> tuple[ServiceConfig, FastAPI]:
    config = ServiceConfig.load(config_path)
    return config, create_app(config)
