"""
DAG Pipeline Saúde RJ
Orquestra o pipeline completo de dados de saúde do Rio de Janeiro.

Fluxo:
1. Extração de dados das APIs
2. Upload para BigQuery
3. Transformação com dbt
4. Validação de qualidade
5. Notificação de sucesso
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import logging

# Configurações
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'your-project-id')
GCS_BUCKET = os.getenv('GCS_BUCKET_RAW', 'data-saude-brutos')
BQ_DATASET = os.getenv('BQ_DATASET_RAW', 'brutos_saude')

# Configuração padrão da DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'email': ['your-email@example.com']  # Substitua pelo seu email
}

# Definição da DAG
dag = DAG(
    'pipeline_saude_rj',
    default_args=default_args,
    description='Pipeline completo de dados de saúde do Rio de Janeiro',
    schedule_interval='0 6 * * *',  # Executa diariamente às 6h
    max_active_runs=1,
    catchup=False,
    tags=['saude', 'rio-de-janeiro', 'data-engineering', 'production'],
)

def check_prerequisites(**context):
    """Verifica pré-requisitos antes de iniciar o pipeline."""
    required_env_vars = [
        'GCP_PROJECT_ID',
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GCS_BUCKET_RAW',
        'BQ_DATASET_RAW'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Variáveis de ambiente obrigatórias não definidas: {missing_vars}")
    
    logging.info("Todos os pré-requisitos foram verificados com sucesso")
    return True

def extract_health_data(**context):
    """Executa a extração de dados de saúde."""
    import sys
    sys.path.append('/opt/airflow/dags/etl')
    
    from etl_extract import HealthDataExtractor
    
    extractor = HealthDataExtractor()
    results = extractor.run_extraction()
    
    # Armazena resultados no XCom para próximas tasks
    context['task_instance'].xcom_push(key='extraction_results', value=results)
    
    if not all(results.values()):
        raise Exception(f"Falha na extração de alguns datasets: {results}")
    
    logging.info(f"Extração concluída com sucesso: {results}")
    return results

def load_to_bigquery(**context):
    """Carrega dados para BigQuery."""
    import sys
    sys.path.append('/opt/airflow/dags/etl')
    
    from etl_load_bq import BigQueryLoader
    
    # Recupera resultados da extração
    extraction_results = context['task_instance'].xcom_pull(key='extraction_results')
    logging.info(f"Resultados da extração: {extraction_results}")
    
    loader = BigQueryLoader()
    load_results = loader.run_load_pipeline()
    
    context['task_instance'].xcom_push(key='load_results', value=load_results)
    
    if not load_results['success']:
        raise Exception(f"Falha no carregamento para BigQuery: {load_results}")
    
    logging.info(f"Carregamento para BigQuery concluído: {load_results}")
    return load_results

def run_data_quality_tests(**context):
    """Executa testes de qualidade dos dados."""
    # Esta função pode integrar com Great Expectations
    # Por ora, implementamos validações básicas
    
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Testes básicos de qualidade
    quality_tests = {
        'test_atendimentos_not_empty': f"""
            SELECT COUNT(*) as count 
            FROM `{PROJECT_ID}.{BQ_DATASET}.atendimentos` 
            WHERE DATE(data_extracao) = CURRENT_DATE()
        """,
        'test_unidades_not_empty': f"""
            SELECT COUNT(*) as count 
            FROM `{PROJECT_ID}.{BQ_DATASET}.unidades_saude`
            WHERE DATE(data_extracao) = CURRENT_DATE()
        """,
        'test_leitos_not_empty': f"""
            SELECT COUNT(*) as count 
            FROM `{PROJECT_ID}.{BQ_DATASET}.leitos_disponibilidade`
            WHERE DATE(data_extracao) = CURRENT_DATE()
        """
    }
    
    test_results = {}
    
    for test_name, query in quality_tests.items():
        try:
            result = client.query(query).to_dataframe()
            count = result.iloc[0]['count']
            test_results[test_name] = {
                'status': 'PASS' if count > 0 else 'FAIL',
                'count': count
            }
            logging.info(f"{test_name}: {count} registros encontrados")
        except Exception as e:
            test_results[test_name] = {
                'status': 'ERROR',
                'error': str(e)
            }
            logging.error(f"Erro no teste {test_name}: {e}")
    
    # Verifica se algum teste falhou
    failed_tests = [test for test, result in test_results.items() 
                   if result['status'] in ['FAIL', 'ERROR']]
    
    if failed_tests:
        raise Exception(f"Testes de qualidade falharam: {failed_tests}")
    
    context['task_instance'].xcom_push(key='quality_results', value=test_results)
    logging.info(f"Todos os testes de qualidade passaram: {test_results}")
    return test_results

def send_success_notification(**context):
    """Envia notificação de sucesso."""
    execution_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    
    # Recupera resultados das tasks anteriores
    extraction_results = context['task_instance'].xcom_pull(key='extraction_results')
    load_results = context['task_instance'].xcom_pull(key='load_results')
    quality_results = context['task_instance'].xcom_pull(key='quality_results')
    
    message = f"""
    ✅ Pipeline Saúde RJ executado com sucesso!
    
    📅 Data de execução: {execution_date}
    
    📊 Resultados da extração:
    {extraction_results}
    
    🔄 Resultados do carregamento:
    - Tabelas carregadas: {load_results.get('successful_loads', 'N/A')}/{load_results.get('total_tables', 'N/A')}
    
    ✔️ Testes de qualidade:
    {quality_results}
    
    🎯 Próximos passos:
    - Dashboard atualizado no Looker Studio
    - Dados disponíveis para análise
    """
    
    logging.info(message)
    
    # Aqui você pode adicionar integração com Slack, email, etc.
    # Por exemplo, usando SlackWebhookOperator ou EmailOperator
    
    return message

# Definição das Tasks

# Task 1: Verificação de pré-requisitos
task_check_prerequisites = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag
)

# Task 2: Extração de dados
task_extract_data = PythonOperator(
    task_id='extract_health_data',
    python_callable=extract_health_data,
    dag=dag
)

# Task 3: Sensores para verificar se arquivos foram criados
sensor_check_files = GCSObjectExistenceSensor(
    task_id='sensor_check_extracted_files',
    bucket=GCS_BUCKET,
    object=f"raw/{datetime.now().strftime('%Y/%m/%d')}/atendimentos.parquet",
    timeout=300,  # 5 minutos
    poke_interval=30,  # Verifica a cada 30 segundos
    dag=dag
)

# Task 4: Carregamento para BigQuery
task_load_bigquery = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag
)

# Task 5: Execução do dbt
task_run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    cd /opt/airflow/dags/dbt && \
    dbt deps && \
    dbt run --profiles-dir . && \
    dbt test --profiles-dir .
    """,
    dag=dag
)

# Task 6: Testes de qualidade
task_quality_tests = PythonOperator(
    task_id='run_data_quality_tests',
    python_callable=run_data_quality_tests,
    dag=dag
)

# Task 7: Atualização de estatísticas no BigQuery
task_update_stats = BigQueryInsertJobOperator(
    task_id='update_table_statistics',
    configuration={
        'query': {
            'query': f"""
                -- Atualiza estatísticas das tabelas para otimizar performance
                ANALYZE TABLE `{PROJECT_ID}.model_saude.marts_dashboard_saude`;
            """,
            'useLegacySql': False,
        }
    },
    dag=dag
)

# Task 8: Notificação de sucesso
task_notify_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# Task 9: Notificação de falha (executa apenas em caso de falha)
task_notify_failure = PythonOperator(
    task_id='send_failure_notification',
    python_callable=lambda **context: logging.error(
        f"Pipeline falhou na execução de {context['execution_date']}"
    ),
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)

# Definição das dependências
task_check_prerequisites >> task_extract_data
task_extract_data >> sensor_check_files
sensor_check_files >> task_load_bigquery
task_load_bigquery >> task_run_dbt
task_run_dbt >> task_quality_tests
task_quality_tests >> task_update_stats
task_update_stats >> task_notify_success

# Configuração para notificação de falha
[task_extract_data, task_load_bigquery, task_run_dbt, task_quality_tests] >> task_notify_failure