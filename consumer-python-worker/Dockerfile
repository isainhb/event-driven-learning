FROM python:3.13-alpine

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# TODO: Use context from docker-compose.yml
COPY . .

# TODO: Execution command will be defined in root's docker-compose.yml