FROM python:3.11-slim

# Show print() output immediately instead of buffering it (so logs stream
# live under `docker compose up`).
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so this layer is cached between code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default to running a worker; the producer service overrides this command.
CMD ["python", "worker.py"]
