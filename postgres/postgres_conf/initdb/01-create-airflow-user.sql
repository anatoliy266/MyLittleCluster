CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- PostgreSQL 15 requires additional privileges:
-- Note: Connect to the airflow_db database before running the following GRANT statement
-- You can do this in psql with: \c airflow_db
\c airflow_db
GRANT ALL ON SCHEMA public TO airflow_user;