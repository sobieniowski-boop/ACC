"""
GLS API connector package.

Provides:
  - OAuth2 authentication (GLSAuth)
  - Parcel tracking via Track And Trace V1 (GLSClient) — GLS Group (DE, etc.)
  - Cost Center Posting via SAP OData API (GLSCostCenterClient)
  - GLS Poland ADE WebAPI2 SOAP client (GLSADEClient) — PL parcels

Usage:
    from app.connectors.gls_api import GLSClient, GLSCostCenterClient, GLSADEClient

    client = GLSClient()
    status = client.track("12345678901")

    ade = GLSADEClient()
    result = ade.search_parcel("12345678901")
    ade.close()
"""

from app.connectors.gls_api.client import GLSClient
from app.connectors.gls_api.auth import GLSAuth
from app.connectors.gls_api.cost_center import GLSCostCenterClient
from app.connectors.gls_api.ade_client import GLSADEClient

__all__ = ["GLSClient", "GLSAuth", "GLSCostCenterClient", "GLSADEClient"]
