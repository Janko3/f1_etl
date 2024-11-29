FROM apache/airflow:2.10.3

# Switch to the airflow user
USER airflow

# Copy requirements.txt into the container
COPY requirements.txt /requirements.txt

# Install Python dependencies from the file
RUN pip install --no-cache-dir -r /requirements.txt







