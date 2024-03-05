CREATE DATABASE datalake;
CREATE USER datalake WITH PASSWORD '<in onepassword DATALAKE_PASS>';
GRANT CREATE ON DATABASE datalake TO datalake;
