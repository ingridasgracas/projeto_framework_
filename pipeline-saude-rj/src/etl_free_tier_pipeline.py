# ✅ GUIA COMPLETO: Google Cloud FREE TIER para Pipeline Saúde RJ

## 🆓 **RESUMO: 100% GRATUITO SIM!**

### O que você ganha de graça para sempre:
- **BigQuery**: 1TB consultas/mês + 10GB storage
- **Cloud Storage**: 5GB storage
- **Compute Engine**: 1 instância f1-micro (sempre gratuito)
- **Cloud Functions**: 2 milhões invocações/mês
- **Cloud Run**: 2 milhões requests/mês

### 💰 **Créditos Adicionais**:
- **$300 USD** em créditos por 90 dias (novos usuários)
- **Sem cobrança** até esgotar os créditos

---

## 🎯 **NOSSO PIPELINE - CUSTO REAL: $0.00/mês**

### Estimativa de uso para dados de saúde RJ:
```
📊 DADOS PROCESSADOS:
- Atendimentos: ~3.000 registros/dia = ~5MB/dia
- Leitos: ~50 hospitais = ~10KB/dia  
- Unidades: ~200 unidades = ~50KB/dia
- TOTAL: ~5MB/dia = ~150MB/mês

💾 STORAGE (Cloud Storage):
- Dados mensais: ~150MB
- Histórico 1 ano: ~1.8GB
- LIMITE FREE: 5GB ✅ DENTRO DO LIMITE

🔍 BIGQUERY:
- Consultas diárias: ~50MB processados
- Consultas mensais: ~1.5GB processados  
- LIMITE FREE: 1TB/mês ✅ MUITO DENTRO DO LIMITE

💸 CUSTO ESTIMADO: $0.00/mês
```

---

## 🚀 **SETUP OTIMIZADO PARA FREE TIER**

### 1. Criar Conta Google Cloud (GRATUITO)
```bash
# Acesse: https://cloud.google.com/free
# Clique "Get started for free"
# Use Gmail pessoal ou corporativo
# Adicione cartão (não será cobrado até você ativar billing)
```

### 2. Configurar Projeto (GRATUITO)
```bash
# Criar projeto
gcloud projects create pipeline-saude-rj-free --name="Pipeline Saude RJ Free"
gcloud config set project pipeline-saude-rj-free

# Habilitar APIs (gratuitas)
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### 3. Configurar Storage (GRATUITO - 5GB)
```bash
# Bucket otimizado para free tier
gsutil mb -p pipeline-saude-rj-free -c STANDARD -l us-central1 gs://saude-rj-free

# Configurar lifecycle para economizar (move dados antigos)
echo '{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 365}
      }
    ]
  }
}' > lifecycle-free.json

gsutil lifecycle set lifecycle-free.json gs://saude-rj-free
```

### 4. BigQuery Datasets (GRATUITO - 10GB storage)
```bash
# Apenas os datasets essenciais
bq mk --dataset --location=US pipeline-saude-rj-free:saude_rj_raw
bq mk --dataset --location=US pipeline-saude-rj-free:saude_rj_marts

# Configurar expiração de tabelas (economizar storage)
bq mk --dataset --default_table_expiration=31536000 pipeline-saude-rj-free:saude_rj_temp
```

---

## 💡 **OTIMIZAÇÕES PARA PERMANECER GRATUITO**

### Storage Optimization
```bash
# Comprimir dados antes do upload
gzip data_output/*.csv

# Upload comprimido (economiza ~70% storage)
gsutil -m cp data_output/*.csv.gz gs://saude-rj-free/raw/
```

### BigQuery Optimization
```sql
-- ✅ QUERY OTIMIZADA (processa menos dados)
SELECT 
  tipo_atendimento,
  AVG(tempo_espera_minutos) as tempo_medio,
  COUNT(*) as total_atendimentos
FROM `pipeline-saude-rj-free.saude_rj_raw.atendimentos`
WHERE data_atendimento >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- Apenas última semana
GROUP BY tipo_atendimento;

-- ❌ EVITAR (processa todos os dados)
SELECT * FROM `pipeline-saude-rj-free.saude_rj_raw.atendimentos`;
```

### Automação FREE com Cloud Scheduler
```bash
# Criar job que roda 1x por dia (gratuito)
gcloud scheduler jobs create http saude-pipeline-daily \
  --schedule="0 6 * * *" \
  --uri="https://your-cloud-run-url/pipeline" \
  --http-method=POST
```

---

## 📊 **LOOKER STUDIO - SEMPRE GRATUITO**

### Conexão BigQuery FREE
1. **Looker Studio**: https://lookerstudio.google.com (100% gratuito)
2. **Conectar BigQuery**: Projeto `pipeline-saude-rj-free`
3. **Dashboards ilimitados** sem custo

### Dashboards Otimizados FREE
```sql
-- Dashboard queries que gastam <1MB cada
CREATE OR REPLACE VIEW `pipeline-saude-rj-free.saude_rj_marts.dashboard_ocupacao` AS
SELECT 
  nome_unidade,
  percentual_ocupacao,
  percentual_ocupacao_uti,
  data_atualizacao
FROM `pipeline-saude-rj-free.saude_rj_raw.leitos_disponibilidade`
WHERE data_atualizacao >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
```

---

## 🎯 **ARQUIVO .env PARA FREE TIER**

```bash
# Google Cloud FREE Configuration
GCP_PROJECT_ID=pipeline-saude-rj-free
GCP_REGION=us-central1
GCS_BUCKET_NAME=saude-rj-free
GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account-key.json

# BigQuery FREE (apenas essencial)
BQ_DATASET_RAW=saude_rj_raw
BQ_DATASET_MARTS=saude_rj_marts

# Otimizações FREE
COMPRESS_DATA=true
RETENTION_DAYS=365
BATCH_SIZE=100
ENABLE_LIFECYCLE=true
```

---

## 🔥 **PIPELINE OTIMIZADO PARA FREE TIER**

<function_calls>
<invoke name="create_file">
<parameter name="content">"""
Pipeline Otimizado para Google Cloud FREE TIER
Mantém custos em $0.00 usando limites gratuitos
"""

import pandas as pd
import os
import gzip
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

class FreeTierGCPPipeline:
    """Pipeline otimizado para Free Tier do Google Cloud"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID', 'pipeline-saude-rj-free')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME', 'saude-rj-free')
        self.compress_data = os.getenv('COMPRESS_DATA', 'true').lower() == 'true'
        self.retention_days = int(os.getenv('RETENTION_DAYS', '365'))
        
        # Configurar clientes
        self.storage_client = storage.Client(project=self.project_id)
        self.bigquery_client = bigquery.Client(project=self.project_id)
        
    def optimize_for_free_tier(self, df: pd.DataFrame) -> pd.DataFrame:
        """Otimiza dados para economizar storage e processing"""
        
        # Remover colunas desnecessárias para economizar storage
        unnecessary_cols = [col for col in df.columns if col.startswith('debug_') or col.startswith('temp_')]
        df = df.drop(columns=unnecessary_cols, errors='ignore')
        
        # Converter tipos para economizar espaço
        for col in df.select_dtypes(include=['float64']).columns:
            if df[col].max() < 1000:  # Se valores pequenos, usar float32
                df[col] = df[col].astype('float32')
        
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].max() < 32767:  # Se valores pequenos, usar int16
                df[col] = df[col].astype('int16')
        
        return df
    
    def upload_compressed_to_gcs(self, local_file: str, gcs_path: str) -> bool:
        """Upload comprimido para economizar storage (70% menos espaço)"""
        
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            
            if self.compress_data:
                # Comprimir arquivo
                compressed_path = f"{local_file}.gz"
                with open(local_file, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        f_out.writelines(f_in)
                
                # Upload do arquivo comprimido
                blob = bucket.blob(f"{gcs_path}.gz")
                blob.upload_from_filename(compressed_path)
                
                # Limpar arquivo temporário
                os.remove(compressed_path)
                
                logger.info(f"✅ Upload comprimido: {gcs_path}.gz (economizou ~70% storage)")
                return True
            else:
                # Upload normal
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file)
                logger.info(f"✅ Upload: {gcs_path}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erro no upload: {e}")
            return False
    
    def create_optimized_bigquery_tables(self):
        """Cria tabelas otimizadas para Free Tier"""
        
        # Schemas otimizados (tipos menores = menos storage)
        schemas = {
            'atendimentos': [
                bigquery.SchemaField("id_atendimento", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("data_atendimento", "DATE", mode="REQUIRED"), 
                bigquery.SchemaField("tipo_atendimento", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("tempo_espera_minutos", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("id_unidade", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bairro", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("data_extracao", "TIMESTAMP", mode="REQUIRED")
            ],
            'leitos_disponibilidade': [
                bigquery.SchemaField("id_unidade", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("nome_unidade", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("leitos_totais", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("leitos_ocupados", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("percentual_ocupacao", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("percentual_ocupacao_uti", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("data_atualizacao", "TIMESTAMP", mode="REQUIRED")
            ]
        }
        
        for table_name, schema in schemas.items():
            table_id = f"{self.project_id}.saude_rj_raw.{table_name}"
            
            table = bigquery.Table(table_id, schema=schema)
            
            # Configurações para economizar (Free Tier)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_extracao",
                expiration_ms=self.retention_days * 24 * 60 * 60 * 1000  # Auto-delete dados antigos
            )
            
            try:
                table = self.bigquery_client.create_table(table, exists_ok=True)
                logger.info(f"✅ Tabela otimizada criada: {table_name}")
            except Exception as e:
                logger.error(f"❌ Erro ao criar tabela {table_name}: {e}")
    
    def load_to_bigquery_free(self, gcs_uri: str, table_id: str) -> bool:
        """Carrega dados otimizado para Free Tier"""
        
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append para evitar reprocessar tudo
                max_bad_records=10,  # Tolerância a erros
                compression=bigquery.Compression.GZIP if self.compress_data else None
            )
            
            # Configurar para economizar slots (Free Tier)
            job_config.job_timeout_ms = 300000  # 5 minutos max
            
            load_job = self.bigquery_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )
            
            load_job.result()  # Aguardar
            
            table = self.bigquery_client.get_table(table_id)
            logger.info(f"✅ BigQuery: {table.num_rows} registros em {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro BigQuery: {e}")
            return False
    
    def cleanup_old_data(self):
        """Remove dados antigos para manter dentro do Free Tier"""
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        # Limpar Cloud Storage
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            for blob in bucket.list_blobs(prefix="raw/"):
                if blob.time_created.replace(tzinfo=None) < cutoff_date:
                    blob.delete()
                    logger.info(f"🧹 Removido arquivo antigo: {blob.name}")
        except Exception as e:
            logger.error(f"Erro na limpeza GCS: {e}")
        
        # Limpar BigQuery (partições antigas são removidas automaticamente)
        logger.info(f"🧹 Partições BigQuery >={self.retention_days} dias são removidas automaticamente")
    
    def run_free_tier_pipeline(self):
        """Executa pipeline otimizado para Free Tier"""
        
        logger.info("🆓 Iniciando pipeline FREE TIER...")
        
        # 1. Criar infraestrutura otimizada
        self.create_optimized_bigquery_tables()
        
        # 2. Extrair dados (usar dados locais por enquanto)
        from src.etl_extract_local import LocalHealthDataExtractor
        extractor = LocalHealthDataExtractor()
        datasets = extractor.extract_all_datasets()
        
        results = {'uploaded': [], 'failed': []}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for dataset_name, df in datasets.items():
            logger.info(f"📊 Processando {dataset_name} para Free Tier...")
            
            # Otimizar dados
            df_optimized = self.optimize_for_free_tier(df)
            
            # Salvar local otimizado
            local_path = f"data_output/{dataset_name}_optimized.csv"
            df_optimized.to_csv(local_path, index=False)
            
            # Upload para GCS (comprimido)
            gcs_path = f"raw/health-data/{timestamp}/{dataset_name}"
            if self.upload_compressed_to_gcs(local_path, gcs_path):
                
                # Carregar no BigQuery
                gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
                if self.compress_data:
                    gcs_uri += ".gz"
                    
                table_id = f"{self.project_id}.saude_rj_raw.{dataset_name}"
                
                if self.load_to_bigquery_free(gcs_uri, table_id):
                    results['uploaded'].append(dataset_name)
                else:
                    results['failed'].append(dataset_name)
            else:
                results['failed'].append(dataset_name)
        
        # 3. Limpeza automática
        self.cleanup_old_data()
        
        # 4. Relatório final
        logger.info(f"✅ Pipeline Free Tier concluído!")
        logger.info(f"📤 Uploads: {len(results['uploaded'])}")
        logger.info(f"❌ Falhas: {len(results['failed'])}")
        
        return results


def main():
    """Executa pipeline Free Tier"""
    
    from dotenv import load_dotenv
    load_dotenv('.env', override=True)
    
    logging.basicConfig(level=logging.INFO)
    
    # Executar pipeline Free Tier
    pipeline = FreeTierGCPPipeline()
    results = pipeline.run_free_tier_pipeline()
    
    print("\n🆓 PIPELINE FREE TIER EXECUTADO!")
    print(f"✅ Datasets processados: {len(results['uploaded'])}")
    print(f"💰 Custo estimado: $0.00")
    print(f"📊 Storage usado: ~{sum(os.path.getsize(f'data_output/{f}') for f in os.listdir('data_output') if f.endswith('.csv')) / 1024:.1f} KB")
    

if __name__ == "__main__":
    main()