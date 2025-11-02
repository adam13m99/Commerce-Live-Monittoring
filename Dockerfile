FROM docker.ofood.cloud/library/python:3.12.7-slim

ENV PIP_INDEX_URL=http://box.ofood.cloud/repository/pypi/simple/ \
    PIP_TRUSTED_HOST=box.ofood.cloud \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --only-binary :all: -r requirements.txt

COPY app.py .
COPY mini.py .
COPY config.py .
COPY run_production.py .
COPY templates/ templates/
COPY static/ static/

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')"

CMD ["python", "run_production.py"]
