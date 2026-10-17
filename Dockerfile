# Ganglion control plane (API server only; it never runs agents, builds or tests).
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY ganglion ./ganglion
RUN pip install --no-cache-dir ".[server]"
USER nobody
EXPOSE 8700
ENTRYPOINT ["ganglion"]
CMD ["--config", "/etc/ganglion/config.toml", "server", "start", "--host", "0.0.0.0"]
