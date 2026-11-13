FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[mssql,postgres]"

COPY monitor.example.toml ./
RUN useradd --create-home monitor && mkdir /state && chown monitor /state
USER monitor

EXPOSE 8000
CMD ["uvicorn", "depot_monitor.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
