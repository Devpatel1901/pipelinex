FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install -e .

EXPOSE 8000

CMD ["uvicorn", "pipelinex.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
