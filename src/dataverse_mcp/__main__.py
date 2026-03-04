"""Dataverse MCP Server entry point.

Usage:
    python -m dataverse_mcp
"""

from __future__ import annotations

import structlog

from dataverse_mcp.config import get_settings


def configure_logging(log_level: str) -> None:
    """Configures structured logging for the application."""
    import logging

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main() -> None:
    """Initializes and runs the MCP Server."""
    settings = get_settings()
    configure_logging(settings.log_level)

    log = structlog.get_logger("dataverse_mcp")
    log.info(
        "server_starting",
        name=settings.mcp_server_name,
        transport=settings.mcp_transport.value,
        dataverse_url=settings.dataverse_url or "(not configured)",
    )

    if not settings.validate_azure_config():
        log.warning(
            "azure_config_missing",
            message="Azure configuration is missing or incomplete. "
            "Please check your .env file. The server will start, but real Dataverse calls will fail.",
        )

    from dataverse_mcp.server import mcp

    # Run with the configured transport (stdio or sse)
    # Disable auth token for easy mock testing
    import os
    os.environ["FASTMCP_AUTH_TOKEN"] = ""
    
    mcp.run(transport=settings.mcp_transport.value)


if __name__ == "__main__":
    main()
