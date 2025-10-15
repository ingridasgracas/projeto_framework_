#!/bin/bash
"""
Script de inicialização do ambiente Docker para Pipeline Saúde RJ
"""

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'  
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Função para print colorido
print_message() {
    echo -e "${2}${1}${NC}"
}

print_header() {
    echo
    echo "=================================="
    echo "  $1"
    echo "=================================="
}

# Verifica se Docker está rodando
check_docker() {
    print_header "Verificando Docker"
    
    if ! docker info > /dev/null 2>&1; then
        print_message "❌ Docker não está rodando. Inicie o Docker e tente novamente." $RED
        exit 1
    fi
    
    print_message "✅ Docker está rodando" $GREEN
}

# Verifica se arquivo .env existe
check_env_file() {
    print_header "Verificando Configurações"
    
    if [[ ! -f ".env" ]]; then
        print_message "⚠️ Arquivo .env não encontrado. Copiando template..." $YELLOW
        cp .env.template .env
        print_message "❗ Configure as variáveis no arquivo .env antes de continuar" $RED
        print_message "Especialmente: GCP_PROJECT_ID e GOOGLE_APPLICATION_CREDENTIALS" $YELLOW
        exit 1
    fi
    
    print_message "✅ Arquivo .env encontrado" $GREEN
    
    # Carrega variáveis do .env
    source .env
    
    # Verifica variáveis críticas
    if [[ -z "$GCP_PROJECT_ID" ]]; then
        print_message "❌ GCP_PROJECT_ID não definido no .env" $RED
        exit 1
    fi
    
    if [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
        print_message "❌ GOOGLE_APPLICATION_CREDENTIALS não definido no .env" $RED
        exit 1
    fi
    
    if [[ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
        print_message "❌ Arquivo de credenciais não encontrado: $GOOGLE_APPLICATION_CREDENTIALS" $RED
        exit 1
    fi
    
    print_message "✅ Configurações válidas" $GREEN
}

# Configura permissões
setup_permissions() {
    print_header "Configurando Permissões"
    
    # Cria diretórios necessários
    mkdir -p airflow/logs airflow/dags airflow/plugins
    
    # Define AIRFLOW_UID se não estiver definido (Linux/Mac)
    if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "darwin"* ]]; then
        export AIRFLOW_UID=$(id -u)
        echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
        print_message "✅ AIRFLOW_UID configurado: $AIRFLOW_UID" $GREEN
    fi
}

# Constrói imagens Docker
build_images() {
    print_header "Construindo Imagens Docker"
    
    print_message "📦 Construindo imagem personalizada do Airflow..." $YELLOW
    docker-compose -f docker/docker-compose.yml build --no-cache
    
    print_message "✅ Imagens construídas com sucesso" $GREEN
}

# Inicializa banco de dados do Airflow
init_airflow() {
    print_header "Inicializando Airflow"
    
    print_message "🔄 Inicializando banco de dados..." $YELLOW
    docker-compose -f docker/docker-compose.yml up airflow-init
    
    print_message "✅ Airflow inicializado" $GREEN
}

# Inicia serviços
start_services() {
    print_header "Iniciando Serviços"
    
    print_message "🚀 Iniciando todos os serviços..." $YELLOW
    docker-compose -f docker/docker-compose.yml up -d
    
    print_message "✅ Serviços iniciados com sucesso" $GREEN
    
    # Aguarda serviços ficarem prontos
    print_message "⏳ Aguardando serviços ficarem prontos..." $YELLOW
    sleep 30
    
    # Verifica status dos serviços
    print_message "📊 Status dos serviços:" $YELLOW
    docker-compose -f docker/docker-compose.yml ps
}

# Executa testes básicos
run_tests() {
    print_header "Executando Testes Básicos"
    
    print_message "🧪 Testando conectividade com Airflow..." $YELLOW
    
    # Testa se webserver está respondendo
    max_attempts=10
    attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        if curl -s -f http://localhost:8080/health > /dev/null; then
            print_message "✅ Airflow Webserver respondendo" $GREEN
            break
        else
            print_message "⏳ Tentativa $attempt/$max_attempts - Aguardando Airflow..." $YELLOW
            sleep 10
            ((attempt++))
        fi
    done
    
    if [[ $attempt -gt $max_attempts ]]; then
        print_message "❌ Airflow não ficou pronto após $max_attempts tentativas" $RED
        exit 1
    fi
    
    # Lista DAGs disponíveis
    print_message "📋 DAGs disponíveis:" $YELLOW
    docker-compose -f docker/docker-compose.yml exec -T airflow-scheduler airflow dags list
}

# Exibe informações finais
show_final_info() {
    print_header "🎉 Ambiente Pronto!"
    
    echo
    print_message "🌐 Airflow UI: http://localhost:8080" $GREEN
    print_message "👤 Usuário: airflow" $GREEN  
    print_message "🔑 Senha: airflow" $GREEN
    echo
    print_message "📊 Jupyter Lab: http://localhost:8888 (se profile development ativo)" $YELLOW
    print_message "🌸 Flower: http://localhost:5555 (se profile flower ativo)" $YELLOW
    echo
    print_message "📝 Comandos úteis:" $YELLOW
    echo "   # Ver logs do scheduler:"
    echo "   docker-compose -f docker/docker-compose.yml logs -f airflow-scheduler"
    echo
    echo "   # Executar comando no Airflow:"  
    echo "   docker-compose -f docker/docker-compose.yml exec airflow-scheduler airflow dags list"
    echo
    echo "   # Parar todos os serviços:"
    echo "   docker-compose -f docker/docker-compose.yml down"
    echo
    echo "   # Ver status dos serviços:"
    echo "   docker-compose -f docker/docker-compose.yml ps"
    echo
    print_message "📖 Para mais informações, consulte o README.md" $YELLOW
}

# Função principal
main() {
    print_header "🏥 Pipeline Saúde RJ - Setup Docker"
    
    # Verifica se estamos na raiz do projeto
    if [[ ! -f "requirements.txt" ]] || [[ ! -d "airflow" ]]; then
        print_message "❌ Execute este script na raiz do projeto pipeline-saude-rj" $RED
        exit 1
    fi
    
    # Executa setup
    check_docker
    check_env_file
    setup_permissions
    build_images
    init_airflow
    start_services
    run_tests
    show_final_info
}

# Tratamento de argumentos
case "${1:-start}" in
    "start")
        main
        ;;
    "stop")
        print_message "🛑 Parando todos os serviços..." $YELLOW
        docker-compose -f docker/docker-compose.yml down
        print_message "✅ Serviços parados" $GREEN
        ;;
    "restart")
        print_message "🔄 Reiniciando serviços..." $YELLOW
        docker-compose -f docker/docker-compose.yml down
        docker-compose -f docker/docker-compose.yml up -d
        print_message "✅ Serviços reiniciados" $GREEN
        ;;
    "status")
        docker-compose -f docker/docker-compose.yml ps
        ;;
    "logs")
        docker-compose -f docker/docker-compose.yml logs -f ${2:-airflow-scheduler}
        ;;
    "help")
        echo "Uso: $0 [comando]"
        echo
        echo "Comandos disponíveis:"
        echo "  start    - Inicia o ambiente (padrão)"
        echo "  stop     - Para todos os serviços"
        echo "  restart  - Reinicia todos os serviços"
        echo "  status   - Mostra status dos serviços"
        echo "  logs     - Mostra logs (opcional: especificar serviço)"
        echo "  help     - Mostra esta ajuda"
        ;;
    *)
        print_message "❌ Comando inválido: $1" $RED
        print_message "Use '$0 help' para ver comandos disponíveis" $YELLOW
        exit 1
        ;;
esac