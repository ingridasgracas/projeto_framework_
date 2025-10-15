"""
ETL com Integração Google Cloud Platform
Versão do pipeline que carrega dados diretamente no GCS e BigQuery
"""

import pandas as pd
import os
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.etl_extract_local import LocalHealthDataExtractor
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GCPHealthDataPipeline:
    """Pipeline de dados de saúde integrado com Google Cloud Platform"""
    
    def __init__(self):
        """Inicializa conexões GCP"""
        self.project_id = os.getenv('GCP_PROJECT_ID', 'pipeline-saude-rj-dev')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME', 'saude-rj-data-lake-dev')
        self.credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # Configurar credenciais
        if self.credentials_path and os.path.exists(self.credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
        else:
            logger.warning("Credenciais GCP não encontradas. Usando dados locais.")
            credentials = None
            
        # Inicializar clientes GCP
        if credentials:
            self.storage_client = storage.Client(
                project=self.project_id, 
                credentials=credentials
            )
            self.bigquery_client = bigquery.Client(
                project=self.project_id,
                credentials=credentials  
            )
        else:
            self.storage_client = None
            self.bigquery_client = None
            
        # Datasets BigQuery
        self.bq_dataset_raw = os.getenv('BQ_DATASET_RAW', 'saude_rj_raw')
        self.bq_dataset_staging = os.getenv('BQ_DATASET_STAGING', 'saude_rj_staging')
        self.bq_dataset_marts = os.getenv('BQ_DATASET_MARTS', 'saude_rj_marts')
        
    def setup_gcp_infrastructure(self):
        """Cria buckets e datasets necessários no GCP"""
        
        if not self.storage_client or not self.bigquery_client:
            logger.error("Clientes GCP não inicializados")
            return False
            
        try:
            # Criar bucket GCS se não existir
            try:
                bucket = self.storage_client.get_bucket(self.bucket_name)
                logger.info(f"Bucket {self.bucket_name} já existe")
            except:
                bucket = self.storage_client.create_bucket(
                    self.bucket_name,
                    location='us-central1'
                )
                logger.info(f"Bucket {self.bucket_name} criado")
                
            # Criar datasets BigQuery
            datasets_to_create = [
                self.bq_dataset_raw,
                self.bq_dataset_staging, 
                self.bq_dataset_marts
            ]
            
            for dataset_id in datasets_to_create:
                try:
                    dataset = self.bigquery_client.get_dataset(dataset_id)
                    logger.info(f"Dataset {dataset_id} já existe")
                except:
                    dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
                    dataset.location = "US"
                    dataset = self.bigquery_client.create_dataset(dataset)
                    logger.info(f"Dataset {dataset_id} criado")
                    
            return True
            
        except Exception as e:
            logger.error(f"Erro ao configurar infraestrutura GCP: {e}")
            return False
    
    def upload_to_gcs(self, local_file_path, gcs_path):
        """Upload de arquivo para Google Cloud Storage"""
        
        if not self.storage_client:
            logger.warning("Storage client não disponível")
            return False
            
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            
            blob.upload_from_filename(local_file_path)
            logger.info(f"Arquivo {local_file_path} enviado para gs://{self.bucket_name}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro no upload para GCS: {e}")
            return False
    
    def load_csv_to_bigquery(self, gcs_uri, table_id, schema=None):
        """Carrega CSV do GCS para BigQuery"""
        
        if not self.bigquery_client:
            logger.warning("BigQuery client não disponível")
            return False
            
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Pular header
                autodetect=True,  # Auto-detectar schema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            if schema:
                job_config.schema = schema
                job_config.autodetect = False
            
            load_job = self.bigquery_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )
            
            load_job.result()  # Aguardar conclusão
            
            table = self.bigquery_client.get_table(table_id)
            logger.info(f"Carregados {table.num_rows} registros na tabela {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar no BigQuery: {e}")
            return False
    
    def run_full_pipeline(self):
        """Executa pipeline completo: extração -> GCS -> BigQuery"""
        
        logger.info("🚀 Iniciando pipeline completo...")
        
        # 1. Extrair dados localmente
        extractor = LocalHealthDataExtractor()
        datasets = extractor.extract_all_datasets()
        
        if not datasets:
            logger.error("Falha na extração de dados")
            return False
        
        # 2. Configurar infraestrutura GCP
        if self.storage_client and self.bigquery_client:
            logger.info("🏗️ Configurando infraestrutura GCP...")
            if not self.setup_gcp_infrastructure():
                logger.error("Falha na configuração GCP")
                return False
        
        # 3. Upload para GCS e BigQuery
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        pipeline_results = {
            'extraction_time': datetime.now(),
            'datasets_processed': [],
            'gcs_uploads': [],
            'bigquery_loads': [],
            'alerts_triggered': []
        }
        
        for dataset_name, df in datasets.items():
            logger.info(f"📊 Processando {dataset_name}...")
            
            # Salvar CSV local
            local_path = f"data_output/{dataset_name}.csv"
            df.to_csv(local_path, index=False)
            
            pipeline_results['datasets_processed'].append({
                'name': dataset_name,
                'records': len(df),
                'local_path': local_path
            })
            
            # Upload para GCS (se disponível)
            if self.storage_client:
                gcs_path = f"raw/health-data/{timestamp}/{dataset_name}.csv"
                if self.upload_to_gcs(local_path, gcs_path):
                    pipeline_results['gcs_uploads'].append(gcs_path)
                
                    # Carregar no BigQuery
                    gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
                    table_id = f"{self.project_id}.{self.bq_dataset_raw}.{dataset_name}"
                    
                    if self.load_csv_to_bigquery(gcs_uri, table_id):
                        pipeline_results['bigquery_loads'].append(table_id)
        
        # 4. Verificar alertas críticos
        alerts = self.check_critical_alerts(datasets)
        pipeline_results['alerts_triggered'] = alerts
        
        # 5. Salvar log da execução
        self.save_pipeline_log(pipeline_results)
        
        logger.info("✅ Pipeline concluído com sucesso!")
        return pipeline_results
    
    def check_critical_alerts(self, datasets):
        """Verifica condições críticas e gera alertas"""
        
        alerts = []
        
        # Verificar ocupação de leitos
        if 'leitos_disponibilidade' in datasets:
            df_leitos = datasets['leitos_disponibilidade']
            
            # UTI crítica (> 85%)
            uti_critica = df_leitos[df_leitos['percentual_ocupacao_uti'] > 85]
            if not uti_critica.empty:
                alerts.append({
                    'type': 'OCUPACAO_UTI_CRITICA',
                    'severity': 'HIGH',
                    'message': f"🚨 {len(uti_critica)} hospitais com UTI >85% ocupada",
                    'affected_hospitals': uti_critica['nome_unidade'].tolist()
                })
            
            # Leitos gerais críticos (> 90%)
            leitos_criticos = df_leitos[df_leitos['percentual_ocupacao'] > 90]
            if not leitos_criticos.empty:
                alerts.append({
                    'type': 'OCUPACAO_GERAL_CRITICA',
                    'severity': 'MEDIUM', 
                    'message': f"⚠️ {len(leitos_criticos)} hospitais com ocupação >90%",
                    'affected_hospitals': leitos_criticos['nome_unidade'].tolist()
                })
        
        # Verificar tempo de espera excessivo
        if 'atendimentos' in datasets:
            df_atendimentos = datasets['atendimentos']
            
            # Tempo de espera > 2 horas
            espera_longa = df_atendimentos[df_atendimentos['tempo_espera_minutos'] > 120]
            if not espera_longa.empty:
                alerts.append({
                    'type': 'TEMPO_ESPERA_EXCESSIVO',
                    'severity': 'MEDIUM',
                    'message': f"⏰ {len(espera_longa)} atendimentos com espera >2h",
                    'avg_wait_time': espera_longa['tempo_espera_minutos'].mean()
                })
        
        # Log dos alertas
        for alert in alerts:
            logger.warning(f"ALERTA: {alert['message']}")
            
        return alerts
    
    def save_pipeline_log(self, results):
        """Salva log detalhado da execução do pipeline"""
        
        log_data = {
            'timestamp': results['extraction_time'].isoformat(),
            'status': 'SUCCESS',
            'datasets_count': len(results['datasets_processed']),
            'total_records': sum(d['records'] for d in results['datasets_processed']),
            'gcs_uploads_count': len(results['gcs_uploads']),
            'bigquery_loads_count': len(results['bigquery_loads']),
            'alerts_count': len(results['alerts_triggered']),
            'details': results
        }
        
        # Salvar JSON
        os.makedirs('logs', exist_ok=True)
        log_file = f"logs/pipeline_run_{results['extraction_time'].strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
            
        logger.info(f"📝 Log salvo em {log_file}")
        
        return log_file


def main():
    """Executa o pipeline principal"""
    
    # Carregar variáveis de ambiente
    from dotenv import load_dotenv
    load_dotenv('.env', override=True)
    
    # Executar pipeline
    pipeline = GCPHealthDataPipeline()
    results = pipeline.run_full_pipeline()
    
    if results:
        print("\n🎉 PIPELINE EXECUTADO COM SUCESSO!")
        print(f"📊 Datasets processados: {len(results['datasets_processed'])}")
        print(f"📁 Uploads GCS: {len(results['gcs_uploads'])}")
        print(f"🗄️ Cargas BigQuery: {len(results['bigquery_loads'])}")
        print(f"🚨 Alertas gerados: {len(results['alerts_triggered'])}")
        
        # Mostrar alertas
        for alert in results['alerts_triggered']:
            print(f"   {alert['message']}")
    else:
        print("❌ Falha na execução do pipeline")


if __name__ == "__main__":
    main()