FROM apache/airflow:2.10.5 

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk ant procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary \
    requests \
    apache-airflow-providers-openlineage \
    openlineage-python \
    pymongo