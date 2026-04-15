FROM apache/airflow:2.9.0

COPY airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files to /opt/nba-intel instead
COPY ingestion /opt/nba-intel/ingestion
COPY dbt /opt/nba-intel/dbt