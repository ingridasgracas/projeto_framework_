#!/usr/bin/env python3
"""
ETL Load Script - Pipeline Saúde RJ
Carrega dados do Google Cloud Storage para BigQuery.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BigQueryLoader:
    """Carregador de dados para BigQuery."""
    
    def __init__(self):
        """Inicializa o carregador com configurações."""
        load_dotenv()
        
        self.gcp_project_id = os.getenv('GCP_PROJECT_ID')
        self.gcs_bucket_raw = os.getenv('GCS_BUCKET_RAW', 'data-saude-brutos')
        self.bq_dataset_raw = os.getenv('BQ_DATASET_RAW', 'brutos_saude')
        self.bq_location = os.getenv('BQ_LOCATION', 'US')
        
        # Inicializa clientes
        try:
            self.bq_client = bigquery.Client(project=self.gcp_project_id)
            self.gcs_client = storage.Client(project=self.gcp_project_id)
        except Exception as e:
            logger.error(f"Erro ao conectar com GCP: {e}")
            raise
    
    def create_dataset_if_not_exists(self, dataset_id: str) -> bool:
        """Cria dataset no BigQuery se não existir."""
        try:
            full_dataset_id = f"{self.gcp_project_id}.{dataset_id}"
            
            try:
                self.bq_client.get_dataset(full_dataset_id)
                logger.info(f"Dataset {dataset_id} já existe")
                return True
            except NotFound:
                # Dataset não existe, criar
                dataset = bigquery.Dataset(full_dataset_id)
                dataset.location = self.bq_location
                dataset.description = f"Dataset para dados brutos de saúde - Rio de Janeiro"
                
                dataset = self.bq_client.create_dataset(dataset)
                logger.info(f"Dataset {dataset_id} criado com sucesso")
                return True
                
        except Exception as e:
            logger.error(f"Erro ao criar dataset {dataset_id}: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Define o schema das tabelas baseado no nome."""
        
        schemas = {
            'unidades_saude': [
                bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("nome", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("endereco", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bairro", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tipo_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("telefone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("data_extracao", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("fonte", "STRING", mode="NULLABLE"),
            ],
            'atendimentos': [
                bigquery.SchemaField("id_atendimento", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("data_atendimento", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("tipo_atendimento", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tempo_espera_minutos", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("id_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bairro", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tipo_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("sus_privado", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("idade_paciente", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("sexo_paciente", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("data_extracao", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("fonte", "STRING", mode="NULLABLE"),
            ],
            'leitos_disponibilidade': [
                bigquery.SchemaField("id_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("nome_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bairro", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("leitos_totais", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("leitos_ocupados", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("leitos_uti_totais", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("leitos_uti_ocupados", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("data_atualizacao", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("tipo_hospital", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("percentual_ocupacao", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("percentual_ocupacao_uti", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("data_extracao", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("fonte", "STRING", mode="NULLABLE"),
            ]
        }
        
        return schemas.get(table_name, [])
    
    def load_gcs_to_bq(self, gcs_uri: str, table_name: str, 
                      write_disposition: str = "WRITE_TRUNCATE") -> bool:
        """Carrega dados do GCS para BigQuery."""
        try:
            # Referência da tabela
            table_id = f"{self.gcp_project_id}.{self.bq_dataset_raw}.{table_name}"
            
            # Configuração do job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                schema=self.get_table_schema(table_name),
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extracao"
                ) if table_name in ['atendimentos'] else None
            )
            
            # Inicia o job de carregamento
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )
            
            # Aguarda conclusão
            load_job.result()
            
            # Verifica resultado
            destination_table = self.bq_client.get_table(table_id)
            logger.info(
                f"Carregados {destination_table.num_rows} registros "
                f"na tabela {table_name}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar {table_name} no BigQuery: {e}")
            return False
    
    def find_latest_files(self, date_str: Optional[str] = None) -> Dict[str, str]:
        """Encontra os arquivos mais recentes no GCS."""
        try:
            if date_str is None:
                date_str = datetime.now().strftime('%Y/%m/%d')
            
            bucket = self.gcs_client.bucket(self.gcs_bucket_raw)
            prefix = f"raw/{date_str}/"
            
            files = {}
            
            # Lista arquivos do dia
            blobs = bucket.list_blobs(prefix=prefix)
            
            for blob in blobs:
                if blob.name.endswith('.parquet'):
                    # Extrai nome da tabela do arquivo
                    filename = blob.name.split('/')[-1]
                    table_name = filename.replace('.parquet', '')
                    
                    gcs_uri = f"gs://{self.gcs_bucket_raw}/{blob.name}"
                    files[table_name] = gcs_uri
            
            logger.info(f"Encontrados {len(files)} arquivos para {date_str}")
            return files
            
        except Exception as e:
            logger.error(f"Erro ao listar arquivos do GCS: {e}")
            return {}
    
    def validate_data_quality(self, table_name: str) -> Dict[str, any]:
        """Executa validações básicas de qualidade dos dados."""
        try:
            table_id = f"{self.gcp_project_id}.{self.bq_dataset_raw}.{table_name}"
            
            # Query para validações básicas
            validation_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT data_extracao) as distinct_extraction_dates,
                COUNTIF(data_extracao IS NULL) as null_extraction_dates,
                MIN(data_extracao) as min_extraction_date,
                MAX(data_extracao) as max_extraction_date
            FROM `{table_id}`
            """
            
            result = self.bq_client.query(validation_query).to_dataframe()
            
            validation_results = {
                'table_name': table_name,
                'total_records': int(result.iloc[0]['total_records']),
                'distinct_extraction_dates': int(result.iloc[0]['distinct_extraction_dates']),
                'null_extraction_dates': int(result.iloc[0]['null_extraction_dates']),
                'min_extraction_date': result.iloc[0]['min_extraction_date'],
                'max_extraction_date': result.iloc[0]['max_extraction_date'],
            }
            
            logger.info(f"Validação {table_name}: {validation_results['total_records']} registros")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Erro na validação de {table_name}: {e}")
            return {'table_name': table_name, 'error': str(e)}
    
    def run_load_pipeline(self, date_str: Optional[str] = None) -> Dict[str, any]:
        """Executa o pipeline completo de carregamento."""
        logger.info("Iniciando carregamento de dados no BigQuery")
        
        # Cria dataset se necessário
        if not self.create_dataset_if_not_exists(self.bq_dataset_raw):
            return {'success': False, 'error': 'Falha ao criar dataset'}
        
        # Encontra arquivos para carregar
        files = self.find_latest_files(date_str)
        
        if not files:
            logger.warning("Nenhum arquivo encontrado para carregar")
            return {'success': False, 'error': 'Nenhum arquivo encontrado'}
        
        # Carrega cada arquivo
        load_results = {}
        validation_results = {}
        
        for table_name, gcs_uri in files.items():
            logger.info(f"Carregando tabela {table_name} de {gcs_uri}")
            
            load_success = self.load_gcs_to_bq(gcs_uri, table_name)
            load_results[table_name] = load_success
            
            # Executa validação se carregamento foi bem-sucedido
            if load_success:
                validation_results[table_name] = self.validate_data_quality(table_name)
        
        # Resultado final
        successful_loads = sum(load_results.values())
        total_loads = len(load_results)
        
        results = {
            'success': successful_loads == total_loads,
            'total_tables': total_loads,
            'successful_loads': successful_loads,
            'load_results': load_results,
            'validation_results': validation_results,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(
            f"Carregamento concluído: {successful_loads}/{total_loads} "
            f"tabelas carregadas com sucesso"
        )
        
        return results

def main():
    """Função principal."""
    try:
        loader = BigQueryLoader()
        
        # Permite especificar data via argumento de linha de comando
        date_str = sys.argv[1] if len(sys.argv) > 1 else None
        
        results = loader.run_load_pipeline(date_str)
        
        if results['success']:
            logger.info("Pipeline de carregamento executado com sucesso!")
            sys.exit(0)
        else:
            logger.error(f"Falhas no pipeline: {results}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()