FROM python:3.14-slim

WORKDIR /app

# Copy dependency files
COPY Pipfile Pipfile.lock ./

# Install pipenv and dependencies
RUN pip install pipenv && pipenv install --deploy --system

# Copy application code
COPY app.py generate_report.py ./
COPY fetch_btc_locked.py fetch_flyover.py fetch_powpeg.py ./
COPY fetch_loop.sh ./
RUN chmod +x fetch_loop.sh

# Create data directory
RUN mkdir -p data

# Expose the application port
EXPOSE 5000

# Run the application
CMD ["python", "app.py"]
