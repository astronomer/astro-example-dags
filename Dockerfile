FROM quay.io/astronomer/astro-runtime:10.4.0
COPY scripts/s3_countries_transform_script.sh /usr/local/airflow/transform_script.sh

#ARG HARPER__MONGO__USER=donniebachan
#ENV HARPER__MONGO__USER=$HARPER__MONGO__USER
#ARG HARPER__MONGO__PASS=;Wn(T)F9wRMy4KY4
#ENV HARPER__MONGO__PASS=$HARPER__MONGO__PASS
#ARG HARPER__MONGO__DB=production
#ENV HARPER__MONGO__DB=$HARPER__MONGO__DB
#ARG HARPER__MONGO__URI=mongodb://federateddatabaseinstance0-fiqjo.a.query.mongodb.net/production?ssl=true&authSource=admin
#ENV HARPER__MONGO__URI=$HARPER__MONGO__URI

#ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true
#ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
ENV JAVA_HOME /usr/lib/jvm/default-java
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

USER astro

RUN echo 'alias ll="ls -al"' >> ~/.bashrc
