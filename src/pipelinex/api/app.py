"""FastAPI application factory.

The ``create_app(repository)`` factory injects a repository into app state
so callers (CLI, tests, ``uvicorn``) decide which implementation to use
without the routes ever importing a concrete class.
"""

from __future__ import annotations

from fastapi import FastAPI

from pipelinex import __version__
from pipelinex.api.routes import router
from pipelinex.core.interfaces import ILogRepository


def create_app(repository: ILogRepository) -> FastAPI:
    app = FastAPI(
        title="PipelineX",
        version=__version__,
        description="Query API for the PipelineX log analytics engine.",
    )
    app.state.repository = repository
    app.include_router(router)
    return app


# Used by ``uvicorn pipelinex.api.app:app`` (Dockerfile).
# When imported this way, we wire up an empty in-memory repo so the
# server can at least start; production would use ``create_app`` instead.
def _default_app() -> FastAPI:
    from pipelinex.persistence.memory_repo import InMemoryLogRepository

    return create_app(InMemoryLogRepository())


app = _default_app()
