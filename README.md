# Crawler

Production site crawler runtime.

## Local setup

Create a virtual environment and install the project:

```bash
python3.12 -m venv venv
venv/bin/pip install -e .
```

Copy `.env.example` to `.env`, then start local infrastructure:

```bash
docker compose up -d
```

Run the API with:

```bash
venv/bin/uvicorn crawler.main:create_app --factory --reload
```

The health endpoint is available at `http://localhost:8000/health`.
