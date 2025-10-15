# GUIA PRÁTICO: Configuração Google Cloud Platform

## 🚀 **PASSO 1: Configurar Projeto GCP (5 minutos)**

### 1.1 Criar Projeto no Console GCP
```bash
# No terminal ou Cloud Shell
gcloud projects create pipeline-saude-rj-dev --name="Pipeline Saúde RJ"
gcloud config set project pipeline-saude-rj-dev

# Habilitar APIs necessárias
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com
```

### 1.2 Configurar Billing (Obrigatório)
1. Acesse: https://console.cloud.google.com/billing
2. Vincule uma conta de faturamento ao projeto
3. **Dica**: Use os créditos gratuitos do Google Cloud

---

## 🔐 **PASSO 2: Criar Service Account (3 minutos)**

### 2.1 Criar Service Account
```bash
# Criar service account
gcloud iam service-accounts create pipeline-saude-sa \
    --display-name="Pipeline Saude Service Account" \
    --description="SA para pipeline de dados de saúde do Rio"

# Conceder permissões
gcloud projects add-iam-policy-binding pipeline-saude-rj-dev \
    --member="serviceAccount:pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding pipeline-saude-rj-dev \
    --member="serviceAccount:pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
```

### 2.2 Gerar Chave JSON
```bash
# Criar diretório para credenciais
mkdir -p credentials

# Gerar chave JSON
gcloud iam service-accounts keys create credentials/service-account-key.json \
    --iam-account=pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com
```

---

## 🗄️ **PASSO 3: Criar Infraestrutura (2 minutos)**

### 3.1 Criar Bucket no Cloud Storage
```bash
# Criar bucket para data lake
gsutil mb -p pipeline-saude-rj-dev -c STANDARD -l us-central1 gs://saude-rj-data-lake-dev

# Definir lifecycle (opcional - dados antigos são movidos para storage mais barato)
echo '{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 30}
      }
    ]
  }
}' > bucket-lifecycle.json

gsutil lifecycle set bucket-lifecycle.json gs://saude-rj-data-lake-dev
```

### 3.2 Criar Datasets no BigQuery
```bash
# Datasets para diferentes camadas
bq mk --dataset --location=US pipeline-saude-rj-dev:saude_rj_raw
bq mk --dataset --location=US pipeline-saude-rj-dev:saude_rj_staging  
bq mk --dataset --location=US pipeline-saude-rj-dev:saude_rj_core
bq mk --dataset --location=US pipeline-saude-rj-dev:saude_rj_marts
```

---

## ⚙️ **PASSO 4: Configurar Ambiente Local**

### 4.1 Instalar Google Cloud SDK
```bash
# Windows (PowerShell como Admin)
Invoke-WebRequest -Uri https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe -OutFile GoogleCloudSDKInstaller.exe
.\GoogleCloudSDKInstaller.exe

# Após instalação, autenticar
gcloud auth login
gcloud config set project pipeline-saude-rj-dev
```

### 4.2 Configurar Arquivo .env
Copie o arquivo `.env.template` para `.env` e preencha:

```bash
# Copiar template
cp .env.template .env

# Editar com seus valores reais
# GCP_PROJECT_ID=pipeline-saude-rj-dev
# GCS_BUCKET_NAME=saude-rj-data-lake-dev
# GOOGLE_APPLICATION_CREDENTIALS=credentials/service-account-key.json
```

---

## 🧪 **PASSO 5: Testar Conexão**

### 5.1 Executar Pipeline GCP
```bash
# Instalar dependências GCP
pip install google-cloud-storage google-cloud-bigquery

# Executar pipeline
python src/etl_gcp_pipeline.py
```

### 5.2 Verificar Resultados
```bash
# Listar arquivos no bucket
gsutil ls gs://saude-rj-data-lake-dev/raw/

# Verificar tabelas no BigQuery
bq ls saude_rj_raw

# Contar registros
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`pipeline-saude-rj-dev.saude_rj_raw.atendimentos\`"
```

---

## 📊 **PASSO 6: Conectar ao Looker Studio**

### 6.1 Criar Conexão BigQuery
1. Acesse: https://lookerstudio.google.com
2. Criar → Fonte de dados → BigQuery
3. Selecionar projeto: `pipeline-saude-rj-dev`
4. Dataset: `saude_rj_marts`

### 6.2 Dashboards Sugeridos
- **Ocupação Hospitalar**: Gráficos de ocupação UTI e leitos gerais
- **Tempos de Espera**: Médias por tipo de atendimento e unidade
- **Fluxo de Pacientes**: Atendimentos por hora/dia
- **Alertas**: Mapa de calor com hospitais críticos

---

## 🚨 **PASSO 7: Configurar Monitoramento**

### 7.1 Alertas GCP
```bash
# Criar política de alerta para falhas no pipeline
gcloud alpha monitoring policies create --policy-from-file=monitoring-policy.yaml
```

### 7.2 Slack Integration (Opcional)
```bash
# Configurar webhook no Slack
# Adicionar URL no .env: SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

---

## ✅ **Checklist Final**

- [ ] ✅ Projeto GCP criado e billing ativado
- [ ] ✅ Service Account criado com permissões
- [ ] ✅ Chave JSON baixada e configurada
- [ ] ✅ Bucket GCS criado
- [ ] ✅ Datasets BigQuery criados  
- [ ] ✅ Arquivo .env configurado
- [ ] ✅ Pipeline GCP executado com sucesso
- [ ] ✅ Dados visíveis no BigQuery
- [ ] ✅ Looker Studio conectado
- [ ] ✅ Dashboard funcionando

---

## 🎯 **Próximo Passo Imediato**

**Execute agora**: Criar seu projeto GCP e seguir os passos 1-3. O setup completo leva cerca de 15 minutos.

Após configurar, execute:
```bash
python src/etl_gcp_pipeline.py
```

E você terá seu pipeline rodando na nuvem! 🚀