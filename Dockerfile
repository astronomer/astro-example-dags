FROM quay.io/astronomer/astro-runtime:11.7.0

RUN pip install apache-airflow-providers-mongo==4.2.0
RUN pip install apache-airflow-providers-snowflake==5.7.0

