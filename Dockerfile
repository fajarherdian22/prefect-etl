FROM python:3.10-slim

WORKDIR /opt/prefect/flows

# Install system dependencies if needed (for mysqlclient, etc.)
RUN apt-get update && apt-get install -y gcc libmysqlclient-dev

COPY . .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Required for Prefect flow discovery and execution
RUN prefect cloud login --key "${PREFECT_API_KEY}" --workspace "${PREFECT_WORKSPACE}" || true

CMD ["prefect", "worker", "start", "-p", "docker-pool"]
