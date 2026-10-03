"""Entrypoint: load configuration and run the registry HTTP service."""

from __future__ import annotations

import os
import sys

import uvicorn

from chain_metadata_registry.api import create_app
from chain_metadata_registry.config import ServiceConfig


def main() -> None:
    config_path = os.environ.get("REGISTRY_CONFIG", "config.json")
    config = ServiceConfig.load(config_path)
    app = create_app(config)
    host = os.environ.get("REGISTRY_HOST", "127.0.0.1")
    port = int(os.environ.get("REGISTRY_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
