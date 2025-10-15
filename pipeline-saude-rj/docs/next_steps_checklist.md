# CHECKLIST: Próximos Passos - Pipeline em Produção

## 🏗️ **PASSO 1: Configuração do Ambiente de Desenvolvimento na Nuvem**

### 1.1 Configurar Google Cloud Project
```bash
# Criar projeto no Google Cloud Console
gcloud projects create pipeline-saude-rj-dev
gcloud config set project pipeline-saude-rj-dev

# Habilitar APIs necessárias
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com
```

### 1.2 Criar Service Account e Credenciais
```bash
# Criar service account
gcloud iam service-accounts create pipeline-saude-sa \
    --display-name="Pipeline Saude Service Account"

# Conceder permissões
gcloud projects add-iam-policy-binding pipeline-saude-rj-dev \
    --member="serviceAccount:pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding pipeline-saude-rj-dev \
    --member="serviceAccount:pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Gerar chave JSON
gcloud iam service-accounts keys create credentials/service-account-key.json \
    --iam-account=pipeline-saude-sa@pipeline-saude-rj-dev.iam.gserviceaccount.com
```

### 1.3 Criar Infrastructure na GCP
- [ ] **Bucket GCS**: `saude-rj-data-lake-dev`
- [ ] **Datasets BigQuery**: 
  - `saude_rj_raw`
  - `saude_rj_staging` 
  - `saude_rj_core`
  - `saude_rj_marts`

---

## 🧪 **PASSO 2: Implementar Qualidade de Dados (Great Expectations)**

### 2.1 Testes de Schema
- [ ] Validar colunas obrigatórias
- [ ] Verificar tipos de dados
- [ ] Validar ranges de valores

### 2.2 Testes de Negócio  
- [ ] Tempo de espera não pode ser negativo
- [ ] Ocupação de leitos entre 0-100%
- [ ] Coordenadas dentro dos limites do RJ

---

## 🚀 **PASSO 3: Deploy do Pipeline para GCP**

### 3.1 Modificar ETL para GCP
- [ ] Enviar CSVs para Google Cloud Storage
- [ ] Carregar dados do GCS para BigQuery
- [ ] Implementar logging e monitoramento

### 3.2 Executar dbt no BigQuery
- [ ] Configurar profile do BigQuery
- [ ] Rodar `dbt run` para criar modelos
- [ ] Executar `dbt test` para validações

---

## ⚙️ **PASSO 4: Automação com Airflow**

### 4.1 Configurar Airflow Local
- [ ] Inicializar banco do Airflow
- [ ] Configurar conexões (GCP, BigQuery)
- [ ] Ativar DAG do pipeline

### 4.2 Monitoramento e Alertas
- [ ] Configurar alertas de falha
- [ ] Métricas de performance
- [ ] Logs centralizados

---

## 📊 **PASSO 5: Dashboard e Visualização**

### 5.1 Looker Studio
- [ ] Conectar ao BigQuery marts
- [ ] Criar dashboards interativos
- [ ] Configurar refresh automático

---

## 🎯 **Prioridade Atual**

**COMEÇAR AGORA**: Configurar credenciais GCP e testar upload para Cloud Storage

Qual dessas etapas você gostaria que eu implemente primeiro?