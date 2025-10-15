# 🏥 Pipeline de Dados Públicos — Saúde no Rio

> **Pipeline automatizado de dados sobre atendimentos de saúde na cidade do Rio de Janeiro**  
> Arquitetura moderna usando Data Lake + Data Warehouse para monitorar indicadores de saúde pública

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-1.5+-orange.svg)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.6+-red.svg)](https://airflow.apache.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue.svg)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🎯 Objetivo

Construir um pipeline de dados automatizado que coleta, transforma e publica indicadores sobre atendimentos de saúde na cidade do Rio, permitindo:

- 📊 Monitorar tempo médio de espera em unidades públicas e privadas
- 🏥 Acompanhar disponibilidade de leitos e ocupação hospitalar  
- 🗺️ Comparar indicadores por região da cidade
- ⚡ Detectar gargalos e alertas em tempo real
- 📈 Gerar insights para melhoria do sistema de saúde

## 🏗️ Arquitetura

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   APIs Públicas │    │  Google Cloud    │    │   Airflow       │
│                 │    │   Storage        │    │  Orquestração   │
│ • DataRio       │───▶│                  │───▶│                 │
│ • DataSUS       │    │  (Dados Brutos)  │    │  • ETL Daily    │
│ • IBGE          │    │                  │    │  • dbt Run      │
└─────────────────┘    └──────────────────┘    │  • Data Quality │
                                              └─────────────────┘
                                                       │
┌─────────────────┐    ┌──────────────────┐           │
│  Looker Studio  │    │    BigQuery      │           │
│                 │    │                  │◀──────────┘
│ • Dashboards    │◀───│ • Data Warehouse │
│ • Alertas       │    │ • dbt Models     │
│ • Relatórios    │    │ • Marts          │
└─────────────────┘    └──────────────────┘
```

## 🛠️ Stack Tecnológica

| Camada | Ferramenta | Função |
|--------|------------|---------|
| **Extração** | Python + Requests | Coleta dados das APIs públicas |
| **Armazenamento** | Google Cloud Storage | Data Lake para dados brutos |
| **Transformação** | dbt + BigQuery | Limpeza, modelagem e métricas |
| **Orquestração** | Apache Airflow | Automação e monitoramento |
| **Visualização** | Looker Studio | Dashboards e relatórios |
| **Qualidade** | Great Expectations | Validação e testes de dados |

## 📁 Estrutura do Projeto

```
pipeline-saude-rj/
├── airflow/                    # Orquestração
│   └── dag_saude_rj.py        # DAG principal
├── dbt/                        # Transformações
│   ├── models/
│   │   ├── stg/               # Staging (limpeza)
│   │   ├── core/              # Core (métricas)
│   │   └── marts/             # Marts (dashboard)
│   ├── dbt_project.yml
│   └── profiles.yml
├── etl/                        # Scripts ETL
│   ├── etl_extract.py         # Extração de dados
│   └── etl_load_bq.py         # Carregamento BigQuery
├── tests/                      # Testes de qualidade
├── docker/                     # Containerização
├── docs/                       # Documentação
├── config/                     # Configurações
├── requirements.txt            # Dependências Python
├── setup.py                   # Script de instalação
└── README.md                  # Este arquivo
```

## 🚀 Quick Start

### 1. Pré-requisitos

- Python 3.9+
- Google Cloud Platform (conta ativa)
- Docker & Docker Compose
- Git

### 2. Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/pipeline-saude-rj.git
cd pipeline-saude-rj

# Execute o setup automático
python setup.py

# Configure as credenciais
cp .env.template .env
# Edite o arquivo .env com suas credenciais GCP
```

### 3. Configuração GCP

```bash
# Instale e configure Google Cloud CLI
gcloud auth login
gcloud config set project SEU_PROJECT_ID

# Crie service account
gcloud iam service-accounts create pipeline-saude-sa

# Download das credenciais
gcloud iam service-accounts keys create credentials.json \
  --iam-account=pipeline-saude-sa@SEU_PROJECT_ID.iam.gserviceaccount.com
```

### 4. Executar Pipeline

```bash
# Teste extração local
cd etl
python etl_extract.py

# Teste dbt
cd ../dbt
dbt run --profiles-dir .

# Executar com Airflow (Docker)
docker-compose up -d
```

## 📊 Fontes de Dados

### DataRio - Portal de Dados Abertos
- **URL**: https://www.dados.rio/dataset
- **Dados**: Unidades de saúde, filas, atendimentos
- **Frequência**: Diária
- **Formato**: JSON via API REST

### DataSUS - Sistema Único de Saúde
- **URL**: https://datasus.saude.gov.br/
- **Dados**: Estatísticas nacionais e regionais
- **Frequência**: Mensal/Anual
- **Formato**: CSV e APIs públicas

### IBGE - Demografia e Contexto
- **URL**: https://servicodados.ibge.gov.br/api/
- **Dados**: População, renda, territórios
- **Frequência**: Anual
- **Formato**: JSON via API REST

## 🔄 Fluxo de Dados

### 1. **Extração (06:00 diário)**
```python
# Coleta dados das APIs
python etl/etl_extract.py
# → Salva em GCS: gs://data-saude-brutos/raw/YYYY/MM/DD/
```

### 2. **Carregamento (06:15)**
```python
# Upload para BigQuery
python etl/etl_load_bq.py
# → Tabelas: brutos_saude.*
```

### 3. **Transformação (06:30)**
```bash
# Executa modelos dbt
dbt run --profiles-dir dbt/
# → Layers: staging → core → marts
```

### 4. **Qualidade (07:00)**
```python
# Validações automáticas
great_expectations checkpoint run pipeline_checkpoint
```

### 5. **Dashboard (07:15)**
- Looker Studio atualiza automaticamente
- Alertas enviados via email/Slack

## 📈 Métricas e KPIs

### Indicadores Principais
- **Tempo Médio de Espera**: Por região e tipo de unidade
- **Taxa de Ocupação**: Leitos gerais e UTI
- **Volume de Atendimentos**: SUS vs. Privado
- **Disponibilidade**: Vagas por especialidade

### Alertas Automatizados
- 🔴 **Crítico**: Ocupação > 95% ou espera > 2h
- 🟡 **Atenção**: Ocupação > 80% ou espera > 1h  
- 🟢 **Normal**: Indicadores dentro da meta

## 🔍 Modelos de Dados

### Camada Staging (stg_*)
- `stg_atendimentos`: Limpeza e tipagem de atendimentos
- `stg_unidades_saude`: Cadastro de unidades normalizado
- `stg_leitos`: Disponibilidade e ocupação de leitos

### Camada Core (core_*)
- `core_metricas_atendimento`: Agregações por unidade/período
- `core_ocupacao_leitos`: Métricas de ocupação hospitalar
- `core_indicadores_saude`: KPIs consolidados por região

### Camada Marts (marts_*)
- `marts_dashboard_saude`: Tabela final para visualização

## 🧪 Qualidade de Dados

### Validações Implementadas
- ✅ Completude: Campos obrigatórios não-nulos
- ✅ Consistência: Validação de domínios e ranges
- ✅ Precisão: Verificação de coordenadas geográficas
- ✅ Atualidade: Dados atualizados nas últimas 24h

### Testes dbt
```sql
-- Exemplo de teste
{{ config(severity='error') }}
SELECT * FROM {{ ref('stg_atendimentos') }}
WHERE tempo_espera_minutos < 0 OR tempo_espera_minutos > 600
```

## 🐳 Deploy com Docker

```yaml
# docker-compose.yml
version: '3.8'
services:
  airflow-webserver:
    image: apache/airflow:2.6.3
    environment:
      - LOAD_EX=n
      - EXECUTOR=Local
    volumes:
      - ./airflow:/opt/airflow/dags
      - ./etl:/opt/airflow/dags/etl
      - ./dbt:/opt/airflow/dags/dbt
```

```bash
# Executar em produção
docker-compose up -d

# Acessar interface
open http://localhost:8080
```

## 📊 Dashboard Looker Studio

### Visualizações Principais
1. **Mapa de Calor**: Tempo de espera por região
2. **Série Temporal**: Evolução dos indicadores
3. **Comparativo**: SUS vs. Privado
4. **Alertas**: Status crítico por unidade
5. **Filtros**: Período, região, tipo de atendimento

### URL do Dashboard
🔗 [Dashboard Saúde RJ](https://lookerstudio.google.com/seu-dashboard)

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

## 📋 Roadmap

- [x] ✅ Pipeline básico ETL
- [x] ✅ Modelos dbt estruturados  
- [x] ✅ Orquestração Airflow
- [ ] 🔄 Integração Great Expectations
- [ ] 📱 Alertas Slack/Teams
- [ ] 🔐 Autenticação OAuth
- [ ] 📊 ML para previsão de demanda
- [ ] 🚀 Deploy GCP Cloud Composer

## 📝 Licença

Este projeto está sob a licença MIT. Veja [LICENSE](LICENSE) para mais detalhes.

## 👥 Autores

- **Seu Nome** - *Engenheira de Dados* - [GitHub](https://github.com/seu-usuario)

## 🙏 Agradecimentos

- Prefeitura do Rio de Janeiro pelos dados abertos
- Comunidade dbt pelos templates e boas práticas
- Apache Airflow pela orquestração robusta

---

**💡 Dica**: Este projeto pode ser adaptado para outras cidades ou domínios de saúde pública. Fork e customize conforme sua necessidade!

## 📞 Suporte

- 📧 Email: seu-email@exemplo.com
- 💬 Issues: [GitHub Issues](https://github.com/seu-usuario/pipeline-saude-rj/issues)
- 📖 Wiki: [Documentação Completa](https://github.com/seu-usuario/pipeline-saude-rj/wiki)