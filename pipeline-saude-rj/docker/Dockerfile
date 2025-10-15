FROM apache/airflow:2.6.3-python3.9

USER root

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copia requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Instala dependências Python
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copia código do projeto
COPY --chown=airflow:root ./airflow /opt/airflow/dags/airflow
COPY --chown=airflow:root ./etl /opt/airflow/dags/etl  
COPY --chown=airflow:root ./dbt /opt/airflow/dags/dbt
COPY --chown=airflow:root ./tests /opt/airflow/dags/tests

# Configura dbt
RUN cd /opt/airflow/dags/dbt && dbt deps --profiles-dir .

# Configura timezone
ENV TZ=America/Sao_Paulo

# Variáveis de ambiente para Airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=False