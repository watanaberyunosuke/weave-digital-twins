# Docker CLI only (no daemon): operations are the documented `docker exec`
# commands and scripts, run against the host's daemon through its socket.
FROM docker:27.3.1-cli AS dockercli

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl \
      && rm -rf /var/lib/apt/lists/*
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
# Leaflet is vendored at build time so the page makes no third-party requests
# at runtime — the map shows which network is loaded.
RUN mkdir -p /app/static/vendor && \
    curl -fsSL https://registry.npmjs.org/leaflet/-/leaflet-1.9.4.tgz \
      | tar -xz -C /app/static/vendor package/dist && \
    mv /app/static/vendor/package/dist /app/static/vendor/leaflet && \
    rm -rf /app/static/vendor/package
COPY app.py .
COPY static/ /app/static/
EXPOSE 8090
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8090"]
