FROM python:3.11-slim

WORKDIR /app

# Copy the Python script
COPY fiplogger.py .

# Install any dependencies
RUN pip install --no-cache-dir requests

# Define default paths via Environment Variables
# These can be overridden at runtime using -e or in docker-compose
ENV DEFAULT_DB_PATH=/data/fiplogger.db
ENV DEFAULT_LOG_PATH=/data/fiplogger.log

# Create base directories (optional, as the script creates dirs, but good practice)
RUN mkdir -p /data

# Run the script using the environment variables as arguments
# This ensures the script receives the paths as --db and --logfile
CMD ["sh", "-c", "python fiplogger.py --db $DEFAULT_DB_PATH --logfile $DEFAULT_LOG_PATH"]
