# 🚀 Data Engineering Learning Setup Guide

**Environment**: Python 3.9+ com Anaconda  
**OS**: Windows com PowerShell  
**IDE**: VS Code com extensions  

---

## 🔧 **SETUP INICIAL (Primeira Semana)**

### **1. Instalar Anaconda**
```bash
# Download Anaconda Python 3.9+
# https://www.anaconda.com/products/distribution
# Instalar com opções padrão
```

### **2. Criar Ambiente Virtual**
```bash
# Criar ambiente específico para data engineering
conda create -n data-eng python=3.9
conda activate data-eng

# Instalar dependências básicas
pip install -r requirements.txt
```

### **3. VS Code Setup**
**Extensions Essenciais:**
- Python (Microsoft)
- Jupyter (Microsoft)  
- Python Docstring Generator
- GitLens
- Docker (para futuro)
- Thunder Client (para APIs)

### **4. Git Configuration**
```bash
git config --global user.name "Ingrid Lima"
git config --global user.email "il4196212@gmail.com"
git config --global init.defaultBranch main
```

### **5. BigQuery Authentication**
```bash
# Instalar gcloud CLI
# https://cloud.google.com/sdk/docs/install

# Autenticar
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project rj-superapp
```

---

## 📁 **ESTRUTURA DE PROJETOS**

### **Organização Recomendada**
```
data-engineering-roadmap/
├── projects/                    # Projetos práticos
│   ├── nivel-1-python-dbt/     
│   │   ├── week-01-python-setup/
│   │   ├── week-02-pandas-basics/
│   │   ├── week-03-dbt-intro/
│   │   └── week-04-dbt-advanced/
│   ├── nivel-2-streaming/
│   └── nivel-3-mlops/
├── notebooks/                   # Jupyter notebooks
├── scripts/                     # Scripts utilitários
├── data/                       # Dados locais (gitignore)
└── config/                     # Configurações
```

---

## ⚙️ **CONFIGURAÇÕES POR TECNOLOGIA**

### **Python Development**
```python
# .env file template
GOOGLE_CLOUD_PROJECT=rj-superapp
BIGQUERY_DATASET_RMI=brutos_rmi
BIGQUERY_DATASET_GO=brutos_go

# VS Code settings.json recommended
{
    "python.defaultInterpreterPath": "~/anaconda3/envs/data-eng/bin/python",
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.formatting.provider": "black"
}
```

### **dbt Configuration**
```bash
# dbt profiles.yml location: ~/.dbt/profiles.yml
# Template será criado na semana 3
```

### **Docker Setup** (Futuro)
```dockerfile
# Dockerfile template for later use
FROM python:3.9-slim
# Will be created when needed
```

---

## 🔍 **TESTING SETUP**

### **Validar Ambiente Python**
```python
# test_environment.py
import pandas as pd
import numpy as np
import sqlalchemy
from google.cloud import bigquery

def test_imports():
    print("✅ All imports successful!")
    print(f"Pandas version: {pd.__version__}")
    print(f"NumPy version: {np.__version__}")

if __name__ == "__main__":
    test_imports()
```

### **Testar Conexão BigQuery**
```python
# test_bigquery.py
from google.cloud import bigquery

def test_bigquery_connection():
    client = bigquery.Client()
    query = "SELECT 1 as test"
    result = client.query(query).result()
    print("✅ BigQuery connection successful!")

if __name__ == "__main__":
    test_bigquery_connection()
```

---

## 📊 **DESENVOLVIMENTO WORKFLOW**

### **Daily Workflow**
```bash
# 1. Ativar ambiente
conda activate data-eng

# 2. Abrir VS Code no projeto
code data-engineering-roadmap/

# 3. Git workflow
git pull origin main
git checkout -b feature/week-X-topic
# ... fazer alterações ...
git add .
git commit -m "feat: completed week X exercise Y"
git push origin feature/week-X-topic

# 4. Documentar progresso
# Atualizar weekly report
# Commit no final do dia
```

### **Weekly Workflow**
```bash
# Domingo - Setup semana
git checkout main
git pull origin main
mkdir projects/nivel-1-python-dbt/week-XX-topic
cp tracking/template-weekly-report.md tracking/weekly-reports/week-XX-topic.md

# Sexta - Review semana  
# Finalizar weekly report
# Atualizar progress tracker
# Planejar próxima semana
```

---

## 🚨 **TROUBLESHOOTING COMUM**

### **Environment Issues**
```bash
# Recriar ambiente se necessário
conda remove -n data-eng --all
conda create -n data-eng python=3.9
conda activate data-eng
pip install -r requirements.txt
```

### **BigQuery Authentication**
```bash
# Reset auth se necessário  
gcloud auth revoke
gcloud auth login
gcloud auth application-default login
```

### **VS Code Python Path**
- `Ctrl+Shift+P` → "Python: Select Interpreter"
- Escolher: `~/anaconda3/envs/data-eng/bin/python`

---

## 📚 **RECURSOS DE SETUP**

### **Documentation Links**
- [Anaconda Documentation](https://docs.anaconda.com/)
- [VS Code Python Setup](https://code.visualstudio.com/docs/python/python-tutorial)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs)
- [dbt Installation](https://docs.getdbt.com/dbt-cli/installation)

### **Video Tutorials**
- [Python Environment Setup - Corey Schafer](https://www.youtube.com/watch?v=YYXdXT2l-Gg)
- [VS Code Python Setup](https://www.youtube.com/watch?v=06I63_p-2A4)

---

**💡 Dica**: Salve este arquivo como referência e teste cada seção antes de prosseguir!