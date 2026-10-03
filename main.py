import uvicorn

from registry.api import create_app
from registry.config import RegistryConfig

app = create_app(RegistryConfig())

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
