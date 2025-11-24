FROM python:3.13.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Use context from docker-compose.yml
COPY . .

ENTRYPOINT []
# Execution command will be defined in root's docker-compose.yml
