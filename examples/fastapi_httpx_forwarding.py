"""FastAPI forwarding example with httpx trace propagation.

This example demonstrates inbound extraction via ``fastapi_request_span`` and
outbound injection via ``httpx_request``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
from fastapi import FastAPI, Request

# Add project root to Python path to enable imports from observability_client
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from observability_client import ObservabilityClient


app = FastAPI(title="Observability forwarding demo")
client = ObservabilityClient(
    service_name="fastapi-forwarder",
    env=os.getenv("OBSERVABILITY_ENV", "dev"),
    profile="service",
    enable_mlflow=False,
    enable_tracing=True,
)


@app.get("/downstream")
def downstream(request: Request) -> dict[str, str]:
    """Handle the downstream request and emit correlated logs."""
    with client.fastapi_request_span(request=request, span_name="downstream.handle") as request_id:
        scoped = client.bind(request_id=request_id, component="downstream")
        scoped.log_event("downstream_received", push=True)
        return {"status": "ok", "request_id": request_id}


@app.get("/proxy")
def proxy(request: Request) -> dict[str, object]:
    """Receive a request and forward it with propagated context."""
    with client.fastapi_request_span(request=request, span_name="proxy.handle") as request_id:
        scoped = client.bind(request_id=request_id, component="proxy")
        scoped.log_event("proxy_received")

        with httpx.Client(timeout=3.0) as http_client:
            response = scoped.httpx_request(
                http_client.request,
                method="GET",
                url="http://127.0.0.1:8010/downstream",
            )

        payload: dict[str, object] = response.json()
        scoped.log_event(
            "proxy_completed",
            status_code=response.status_code,
            push=True,
        )
        return {
            "proxy_request_id": request_id,
            "downstream": payload,
            "status_code": response.status_code,
        }
