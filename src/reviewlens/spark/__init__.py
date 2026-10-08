"""Spark jobs for the Docker stack. Each module is a stage the Airflow DAG runs:

    kafka_to_bronze -> bronze_to_silver -> enrich -> gold (write-audit-publish)
"""
