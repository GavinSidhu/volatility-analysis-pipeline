# Use the official stable Airflow image as our base
FROM apache/airflow:3.0.2

# Switch back to airflow user
USER airflow

# Copy our requirements file into the container
COPY requirements.txt /
# Install the Python dependencies from our requirements file
RUN pip install --no-cache-dir -r /requirements.txt

# Set working directory to Airflow's default
WORKDIR /opt/airflow