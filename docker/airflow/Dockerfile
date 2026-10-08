# Airflow 3 + Spark 4 (local mode) + Delta Lake in one image.
FROM apache/airflow:3.3.2-python3.12

# Java 17 for Spark, copied from the official Temurin image (works on any base distro).
USER root
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"
RUN mkdir -p /opt/lake /opt/reviewlens && chown -R airflow: /opt/lake /opt/reviewlens

USER airflow
COPY --chown=airflow: docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# Project code (also bind-mounted in docker-compose for live edits).
COPY --chown=airflow: pyproject.toml /opt/reviewlens/
COPY --chown=airflow: contracts /opt/reviewlens/contracts
COPY --chown=airflow: src /opt/reviewlens/src
RUN pip install --no-cache-dir --no-deps -e /opt/reviewlens

# Download the Delta + Kafka connector jars at build time, not on the first DAG run.
RUN python -c "from reviewlens.spark.session import get_spark; get_spark('warmup', kafka=True).stop()"
