# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a FastAPI-based REST API that provides modern HTTP endpoints for retrieving data from Qlik Sense Enterprise applications. The API wraps Qlik's native Engine API (WebSocket) and Repository API (HTTPS) to provide simpler, RESTful access to Qlik Sense data with features like pagination, filtering, sorting, and API key authentication.

## Development Commands

### Running the Server

```bash
# Development mode with auto-reload
python run.py

# Or using uvicorn directly
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

# Production mode with multiple workers
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Testing

```bash
# Run all tests
pytest tests/

# Run with coverage report
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_services.py

# Run specific test
pytest tests/integration/test_endpoints.py::TestHealthEndpoint::test_health_endpoint_success

# Run tests by marker
pytest -m unit
pytest -m integration
```

### Code Quality

```bash
# Format code with Black
black src/ tests/

# Lint with Ruff
ruff check src/ tests/

# Type checking with mypy
mypy src/
```

### Docker

```bash
# Build Docker image
docker build -t qlik-sense-api .

# Run with docker-compose
docker-compose up

# Run container with environment file
docker run -p 8000:8000 --env-file .env -v $(pwd)/certs:/app/certs qlik-sense-api
```

## Architecture Overview

The application follows a layered architecture with clear separation of concerns:

### Layer Structure

```
Client Request
    ↓
API Layer (FastAPI Endpoints) - src/api/api/v1/endpoints/
    ↓
Service Layer (Business Logic) - src/api/services/
    ↓
Repository Layer (Data Access) - src/api/repositories/
    ↓
Client Layer (Qlik APIs) - src/api/clients/
    ↓
Qlik Sense Server (Engine API via WebSocket, Repository API via HTTPS)
```

### Key Components

**API Layer** (`src/api/api/v1/endpoints/`)
- `health.py` - Health check endpoint
- `apps.py` - Application listing and metadata endpoints
- `data.py` - Data retrieval endpoints (tables, hypercubes, fields)
- Handles HTTP request/response, validation, and authentication

**Service Layer** (`src/api/services/`)
- `app_service.py` - Application business logic
- `data_service.py` - Data retrieval business logic
- Orchestrates repository calls, applies business rules, handles errors

**Repository Layer** (`src/api/repositories/`)
- `app_repository.py` - App metadata data access
- `data_repository.py` - Data extraction data access
- Constructs Qlik API requests, manages caching, query building

**Client Layer** (`src/api/clients/`)
- `qlik_engine.py` - WebSocket client for Engine API (port 4747)
- `qlik_repository.py` - HTTPS client for Repository API (port 4242)
- `base.py` - Base client with common functionality
- Manages low-level connections, SSL certificates, retries

**Core** (`src/api/core/`)
- `config.py` - Pydantic settings with environment variables
- `dependencies.py` - FastAPI dependency injection
- `exceptions.py` - Custom exception classes
- `events.py` - Application lifecycle events (startup/shutdown)

**Schemas** (`src/api/schemas/`)
- Pydantic models for request/response validation
- `app.py`, `data.py`, `common.py` - Domain models

**Middleware** (`src/api/middleware/`)
- `logging.py` - Request/response logging
- `error_handler.py` - Global exception handling

## Authentication & Authorization

### API Key Authentication

All endpoints (except `/health`) require the `X-API-Key` header:

```python
# In src/api/core/dependencies.py
# API key validation happens via dependency injection
```

API keys and permissions are configured via environment variables:
- Simple mode: Single `API_KEY` with full access
- Advanced mode: `API_KEYS_JSON` with granular app/table permissions per key

### Certificate Authentication

Qlik Sense requires client certificate authentication (mutual TLS):
- Certificates must be in PEM format
- Configured in `.env`: `QLIK_CERT_PATH`, `QLIK_KEY_PATH`, `QLIK_ROOT_CERT_PATH`
- Loaded in `src/api/clients/qlik_engine.py` and `qlik_repository.py`

## Qlik Sense Integration Details

### Engine API (WebSocket - Port 4747)

Used for data extraction via JSON-RPC protocol:
- Creates hypercubes (multi-dimensional data structures)
- Retrieves field values and metadata
- Supports filtering, sorting, pagination at the Qlik level

The Engine API client (`qlik_engine.py`) maintains WebSocket connections and implements:
- Connection retry logic with multiple endpoint attempts
- Request ID management for JSON-RPC
- Response parsing and error handling

### Repository API (HTTPS - Port 4242)

Used for metadata retrieval:
- Lists available applications
- Gets app details, published status, streams
- Manages app permissions

### App and Table Mappings

The system supports mapping between human-friendly names and Qlik GUIDs:
- `APP_MAPPINGS_JSON` - Maps app names to app IDs
- `TABLE_OBJECT_MAPPINGS_JSON` - Maps table names to Qlik object IDs
- Configured in environment variables as JSON strings

## Configuration

All configuration is in `.env` file (see `.env.example` for template):

**Critical Settings:**
- `QLIK_SENSE_HOST` - Qlik Sense server hostname
- `QLIK_USER_DIRECTORY` - Must match certificate user directory
- `QLIK_USER_ID` - Must match certificate user ID
- `API_KEY` - Required for API authentication
- Certificate paths must point to valid PEM files

**Common Pitfalls:**
- Certificate user directory/ID mismatch causes authentication failures
- Missing or expired certificates
- Incorrect ports (4747 for Engine, 4242 for Repository)
- SSL verification issues with self-signed certificates

## Testing Strategy

Tests are organized into:
- **Unit tests** (`tests/unit/`) - Test services with mocked dependencies
- **Integration tests** (`tests/integration/`) - Test endpoints end-to-end

When adding new features:
1. Add Pydantic schema in `src/api/schemas/`
2. Implement repository method in `src/api/repositories/`
3. Add service logic in `src/api/services/`
4. Create endpoint in `src/api/api/v1/endpoints/`
5. Wire up in router (`src/api/api/v1/router.py`)
6. Write unit tests for service layer
7. Write integration tests for endpoints

## Common Development Patterns

### Adding a New Endpoint

1. Define request/response schemas in `src/api/schemas/`
2. Add repository method in appropriate repository class
3. Implement business logic in service class
4. Create endpoint in `src/api/api/v1/endpoints/`
5. Register in `src/api/api/v1/router.py`

### Error Handling

Use custom exceptions from `src/api/core/exceptions.py`:
- `QlikSenseAPIException` - Base exception
- Caught by global exception handler in `src/api/middleware/error_handler.py`
- Returns consistent JSON error responses

### Dependency Injection

FastAPI dependencies are defined in:
- `src/api/core/dependencies.py` - Global dependencies
- `src/api/api/deps.py` - API-specific dependencies (pagination, sorting)

### Logging

Structured logging is configured in `src/api/middleware/logging.py`:
- Request/response logging via middleware
- Use module-level loggers: `logger = logging.getLogger(__name__)`
- Log levels controlled by `LOG_LEVEL` environment variable

## Important Notes

- The API is stateless - each request is independent
- WebSocket connections to Qlik Engine are created per request (not persistent)
- Certificate authentication is required for all Qlik Sense communication
- API responses use Pydantic models for automatic validation and documentation
- Interactive API docs available at `/docs` (Swagger UI) and `/redoc`
