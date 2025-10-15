#!/bin/bash
"""
Setup Script para Pipeline Saúde RJ
Configura o ambiente de desenvolvimento e produção
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def print_header(message):
    print(f"\n{'='*60}")
    print(f"  {message}")
    print(f"{'='*60}")

def run_command(command, description=""):
    """Executa comando e trata erros."""
    try:
        print(f"Executando: {description or command}")
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar: {command}")
        print(f"Saída do erro: {e.stderr}")
        return False

def setup_python_environment():
    """Configura ambiente Python."""
    print_header("Configurando Ambiente Python")
    
    # Verifica se Python está instalado
    try:
        python_version = subprocess.check_output([sys.executable, "--version"]).decode().strip()
        print(f"Python encontrado: {python_version}")
    except Exception as e:
        print(f"Erro ao verificar Python: {e}")
        return False
    
    # Cria ambiente virtual se não existir
    venv_path = Path("venv")
    if not venv_path.exists():
        print("Criando ambiente virtual...")
        if not run_command(f"{sys.executable} -m venv venv"):
            return False
    
    # Ativa ambiente virtual e instala dependências
    if os.name == 'nt':  # Windows
        pip_cmd = "venv\\Scripts\\pip"
        python_cmd = "venv\\Scripts\\python"
    else:  # Linux/Mac
        pip_cmd = "venv/bin/pip"
        python_cmd = "venv/bin/python"
    
    print("Instalando dependências...")
    return run_command(f"{pip_cmd} install -r requirements.txt", "Instalando requirements.txt")

def setup_gcp_credentials():
    """Configura credenciais do Google Cloud Platform."""
    print_header("Configurando Credenciais GCP")
    
    # Verifica se arquivo .env existe
    env_file = Path(".env")
    if not env_file.exists():
        print("Copiando template de configuração...")
        subprocess.run(["cp", ".env.template", ".env"], check=False)
        print("❗ ATENÇÃO: Configure as variáveis no arquivo .env antes de continuar")
    
    # Verifica se gcloud está instalado
    try:
        gcloud_version = subprocess.check_output(["gcloud", "version"]).decode()
        print("Google Cloud CLI encontrado")
    except FileNotFoundError:
        print("❗ Google Cloud CLI não encontrado. Instale em: https://cloud.google.com/sdk/docs/install")
        return False
    
    # Verifica autenticação
    try:
        auth_info = subprocess.check_output(["gcloud", "auth", "list"]).decode()
        if "ACTIVE" in auth_info:
            print("✅ Autenticação GCP ativa encontrada")
        else:
            print("❗ Execute: gcloud auth login")
            return False
    except Exception as e:
        print(f"Erro ao verificar autenticação: {e}")
        return False
    
    return True

def setup_dbt():
    """Configura dbt."""
    print_header("Configurando dbt")
    
    dbt_dir = Path("dbt")
    if not dbt_dir.exists():
        print("Diretório dbt não encontrado!")
        return False
    
    os.chdir("dbt")
    
    # Instala dependências do dbt
    if run_command("dbt deps", "Instalando dependências dbt"):
        print("✅ dbt configurado com sucesso")
        os.chdir("..")
        return True
    else:
        os.chdir("..")
        return False

def create_gcs_buckets():
    """Cria buckets no Google Cloud Storage."""
    print_header("Criando Buckets GCS")
    
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        print("❗ GCP_PROJECT_ID não definido no .env")
        return False
    
    buckets = [
        os.getenv('GCS_BUCKET_RAW', 'data-saude-brutos'),
        os.getenv('GCS_BUCKET_PROCESSED', 'data-saude-processados')
    ]
    
    for bucket in buckets:
        bucket_name = f"{project_id}-{bucket}"
        print(f"Criando bucket: {bucket_name}")
        
        # Verifica se bucket existe
        check_cmd = f"gsutil ls gs://{bucket_name}"
        if run_command(check_cmd):
            print(f"✅ Bucket {bucket_name} já existe")
        else:
            # Cria bucket
            create_cmd = f"gsutil mb -p {project_id} gs://{bucket_name}"
            if run_command(create_cmd):
                print(f"✅ Bucket {bucket_name} criado com sucesso")
            else:
                print(f"❗ Falha ao criar bucket {bucket_name}")
                return False
    
    return True

def create_bigquery_datasets():
    """Cria datasets no BigQuery."""
    print_header("Criando Datasets BigQuery")
    
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        print("❗ GCP_PROJECT_ID não definido")
        return False
    
    datasets = [
        os.getenv('BQ_DATASET_RAW', 'brutos_saude'),
        os.getenv('BQ_DATASET_PROCESSED', 'model_saude')
    ]
    
    for dataset in datasets:
        print(f"Criando dataset: {dataset}")
        
        # Comando para criar dataset
        create_cmd = f"bq mk --dataset --location=US {project_id}:{dataset}"
        if run_command(create_cmd):
            print(f"✅ Dataset {dataset} criado/verificado com sucesso")
        else:
            print(f"⚠️ Dataset {dataset} pode já existir ou houve erro")
    
    return True

def setup_docker():
    """Configura Docker para desenvolvimento local."""
    print_header("Configurando Docker")
    
    # Verifica se Docker está instalado
    try:
        docker_version = subprocess.check_output(["docker", "--version"]).decode()
        print(f"Docker encontrado: {docker_version.strip()}")
        
        # Verifica se docker-compose está disponível
        compose_version = subprocess.check_output(["docker", "compose", "version"]).decode()
        print(f"Docker Compose encontrado: {compose_version.strip()}")
        
        return True
    except FileNotFoundError:
        print("❗ Docker não encontrado. Instale em: https://docs.docker.com/get-docker/")
        return False
    except Exception as e:
        print(f"Erro ao verificar Docker: {e}")
        return False

def validate_setup():
    """Valida se o setup está correto."""
    print_header("Validando Setup")
    
    validations = []
    
    # Verifica arquivos essenciais
    essential_files = [
        ".env",
        "requirements.txt",
        "dbt/dbt_project.yml",
        "airflow/dag_saude_rj.py"
    ]
    
    for file_path in essential_files:
        if Path(file_path).exists():
            validations.append(f"✅ {file_path}")
        else:
            validations.append(f"❌ {file_path}")
    
    # Verifica variáveis de ambiente
    env_vars = [
        'GCP_PROJECT_ID',
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GCS_BUCKET_RAW',
        'BQ_DATASET_RAW'
    ]
    
    for var in env_vars:
        if os.getenv(var):
            validations.append(f"✅ {var}")
        else:
            validations.append(f"❌ {var}")
    
    print("\nResultado da Validação:")
    for validation in validations:
        print(validation)
    
    failed = len([v for v in validations if "❌" in v])
    if failed == 0:
        print("\n🎉 Setup concluído com sucesso!")
        return True
    else:
        print(f"\n⚠️ {failed} item(s) precisam de atenção")
        return False

def main():
    """Função principal do setup."""
    print_header("SETUP PIPELINE SAÚDE RJ")
    print("Este script configurará seu ambiente de desenvolvimento")
    
    # Carrega variáveis de ambiente se .env existir
    env_file = Path(".env")
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv()
    
    steps = [
        ("Ambiente Python", setup_python_environment),
        ("Credenciais GCP", setup_gcp_credentials),
        ("dbt", setup_dbt),
        ("Buckets GCS", create_gcs_buckets),
        ("Datasets BigQuery", create_bigquery_datasets),
        ("Docker", setup_docker),
    ]
    
    success_count = 0
    
    for step_name, step_func in steps:
        try:
            if step_func():
                success_count += 1
                print(f"✅ {step_name} - Concluído")
            else:
                print(f"❌ {step_name} - Falhou")
        except Exception as e:
            print(f"❌ {step_name} - Erro: {e}")
    
    print(f"\nSetup concluído: {success_count}/{len(steps)} etapas realizadas com sucesso")
    
    # Validação final
    validate_setup()
    
    print("\n" + "="*60)
    print("PRÓXIMOS PASSOS:")
    print("1. Configure as credenciais GCP no arquivo .env")
    print("2. Execute: python -m dbt run --profiles-dir dbt")
    print("3. Teste a DAG: python airflow/dag_saude_rj.py")
    print("4. Para produção: docker-compose up -d")
    print("="*60)

if __name__ == "__main__":
    main()