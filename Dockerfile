FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# config.toml is expected to be mounted at /app/config.toml
CMD ["python", "main.py", "/app/config.toml"]
