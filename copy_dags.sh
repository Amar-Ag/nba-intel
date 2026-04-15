#!/bin/bash
echo "Copying DAGs to Airflow container..."
docker cp /workspaces/nba-intel/airflow/dags/nba_pipeline.py nba_airflow:/opt/airflow/dags/
echo "Done!"
