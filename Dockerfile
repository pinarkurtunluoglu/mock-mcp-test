# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and source code
COPY pyproject.toml .
COPY src/ ./src/
COPY README.md .

# Install the package
RUN pip install --no-cache-dir .

# Expose the port (SSE transport default is 8080)
EXPOSE 8080

# Run the server
CMD ["python", "-m", "dataverse_mcp"]
