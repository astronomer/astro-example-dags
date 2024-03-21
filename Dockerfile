FROM quay.io/astronomer/astro-runtime:10.5.0

#ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true
#ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
ENV JAVA_HOME /usr/lib/jvm/default-java
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

USER astro

RUN echo 'alias ll="ls -al"' >> ~/.bashrc
