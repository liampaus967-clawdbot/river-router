"""
River Router API - Main FastAPI Application

National river routing engine for Paddleways.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.core.config import settings

# Create FastAPI app
app = FastAPI(
    title="River Router API",
    description="National river routing engine - Google Maps for water",
    version=__version__,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Load graph and initialize connections on startup."""
    # TODO: Load network graph into memory
    # TODO: Initialize Redis connection
    # TODO: Verify PostGIS connection
    pass


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    pass


@app.get("/")
async def root():
    """Root endpoint - API info."""
    return {
        "name": "River Router API",
        "version": __version__,
        "status": "ok",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {
        "status": "healthy",
        "version": __version__,
        # TODO: Add graph loaded status
        # TODO: Add NWM freshness check
        # TODO: Add database connectivity check
    }


# Import and include routers
# from app.api.routes import router as api_router
# app.include_router(api_router, prefix="/api/v1")
