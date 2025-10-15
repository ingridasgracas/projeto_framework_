"""
Great Expectations Setup para Pipeline Saúde RJ
Configura e executa validações de qualidade dos dados
"""

import os
import sys
from pathlib import Path
import yaml
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HealthDataValidator:
    """Validador de dados de saúde usando Great Expectations."""
    
    def __init__(self, project_root: str = None):
        """Inicializa o validador."""
        if project_root is None:
            project_root = Path(__file__).parent.parent
        
        self.project_root = Path(project_root)
        self.gx_root_dir = self.project_root / "great_expectations"
        
        # Inicializa contexto Great Expectations
        self._setup_great_expectations()
    
    def _setup_great_expectations(self):
        """Configura Great Expectations."""
        try:
            if not self.gx_root_dir.exists():
                logger.info("Inicializando Great Expectations...")
                os.chdir(self.project_root)
                self.context = gx.get_context()
            else:
                logger.info("Carregando contexto Great Expectations existente...")
                self.context = gx.get_context(context_root_dir=str(self.gx_root_dir))
                
        except Exception as e:
            logger.error(f"Erro ao configurar Great Expectations: {e}")
            raise
    
    def create_datasource(self):
        """Cria datasource para BigQuery."""
        try:
            datasource_config = {
                "name": "bigquery_datasource",
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": f"bigquery://{os.getenv('GCP_PROJECT_ID')}/{os.getenv('BQ_DATASET_RAW')}"
                },
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
            
            self.context.test_yaml_config(yaml.dump(datasource_config))
            self.context.add_datasource(**datasource_config)
            
            logger.info("Datasource BigQuery configurado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao criar datasource: {e}")
            raise
    
    def create_expectation_suites(self):
        """Cria suites de expectativas para cada tabela."""
        
        # Suite para tabela de atendimentos
        suite_atendimentos = self.context.create_expectation_suite(
            expectation_suite_name="suite_atendimentos_raw",
            overwrite_existing=True
        )
        
        # Expectativas para atendimentos
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=10,  # Pelo menos 10 atendimentos por dia
                max_value=100000  # Máximo 100k atendimentos por dia
            )
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnToExist(column="id_atendimento")
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id_atendimento")
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(column="id_atendimento")
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="data_atendimento")
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="tempo_espera_minutos",
                min_value=0,
                max_value=600  # Máximo 10 horas de espera
            )
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="sus_privado",
                value_set=["SUS", "Privado"]
            )
        )
        
        suite_atendimentos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="idade_paciente",
                min_value=0,
                max_value=120
            )
        )
        
        # Suite para unidades de saúde
        suite_unidades = self.context.create_expectation_suite(
            expectation_suite_name="suite_unidades_saude",
            overwrite_existing=True
        )
        
        suite_unidades.add_expectation(
            gx.expectations.ExpectColumnToExist(column="id")
        )
        
        suite_unidades.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
        )
        
        suite_unidades.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(column="id")
        )
        
        suite_unidades.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="nome")
        )
        
        suite_unidades.add_expectation(
            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                column="nome",
                min_value=3,
                max_value=200
            )
        )
        
        # Suite para leitos
        suite_leitos = self.context.create_expectation_suite(
            expectation_suite_name="suite_leitos_disponibilidade",
            overwrite_existing=True
        )
        
        suite_leitos.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id_unidade")
        )
        
        suite_leitos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="leitos_totais",
                min_value=0,
                max_value=1000  # Máximo 1000 leitos por hospital
            )
        )
        
        suite_leitos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="leitos_ocupados",
                min_value=0,
                max_value=1000
            )
        )
        
        # Validação customizada: leitos ocupados <= leitos totais
        suite_leitos.add_expectation(
            gx.expectations.ExpectColumnPairValuesToBeEqual(
                column_A="leitos_ocupados",
                column_B="leitos_totais",
                ignore_row_if="either_value_is_missing"
            )
        )
        
        suite_leitos.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="percentual_ocupacao",
                min_value=0,
                max_value=100
            )
        )
        
        logger.info("Suites de expectativas criadas com sucesso")
    
    def create_checkpoint(self):
        """Cria checkpoint para validação automatizada."""
        
        checkpoint_config = {
            "name": "pipeline_saude_checkpoint",
            "config_version": 1.0,
            "template_name": None,
            "module_name": "great_expectations.checkpoint",
            "class_name": "Checkpoint",
            "run_name_template": "%Y%m%d-%H%M%S-pipeline-saude",
            "expectation_suite_name": None,
            "batch_request": None,
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction"
                    }
                },
                {
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                        "site_names": []
                    }
                }
            ],
            "evaluation_parameters": {},
            "runtime_configuration": {},
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "bigquery_datasource",
                        "data_connector_name": "default_runtime_data_connector",
                        "data_asset_name": "atendimentos",
                        "runtime_parameters": {
                            "query": f"""
                                SELECT * FROM `{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET_RAW')}.atendimentos`
                                WHERE DATE(data_extracao) = CURRENT_DATE()
                            """
                        },
                        "batch_identifiers": {
                            "default_identifier_name": "atendimentos_hoje"
                        }
                    },
                    "expectation_suite_name": "suite_atendimentos_raw"
                },
                {
                    "batch_request": {
                        "datasource_name": "bigquery_datasource", 
                        "data_connector_name": "default_runtime_data_connector",
                        "data_asset_name": "unidades_saude",
                        "runtime_parameters": {
                            "query": f"""
                                SELECT * FROM `{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET_RAW')}.unidades_saude`
                            """
                        },
                        "batch_identifiers": {
                            "default_identifier_name": "unidades_atual"
                        }
                    },
                    "expectation_suite_name": "suite_unidades_saude"
                },
                {
                    "batch_request": {
                        "datasource_name": "bigquery_datasource",
                        "data_connector_name": "default_runtime_data_connector", 
                        "data_asset_name": "leitos_disponibilidade",
                        "runtime_parameters": {
                            "query": f"""
                                SELECT * FROM `{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BQ_DATASET_RAW')}.leitos_disponibilidade`
                                WHERE DATE(data_extracao) = CURRENT_DATE()
                            """
                        },
                        "batch_identifiers": {
                            "default_identifier_name": "leitos_hoje"
                        }
                    },
                    "expectation_suite_name": "suite_leitos_disponibilidade"
                }
            ]
        }
        
        self.context.add_checkpoint(**checkpoint_config)
        logger.info("Checkpoint configurado com sucesso")
    
    def run_validation(self):
        """Executa validações de qualidade."""
        try:
            logger.info("Iniciando validação de dados...")
            
            checkpoint_result = self.context.run_checkpoint(
                checkpoint_name="pipeline_saude_checkpoint"
            )
            
            # Analisa resultados
            if checkpoint_result.success:
                logger.info("✅ Todas as validações passaram com sucesso!")
            else:
                logger.error("❌ Algumas validações falharam!")
                
                # Detalha falhas
                for validation_result in checkpoint_result.run_results.values():
                    if not validation_result.success:
                        logger.error(f"Falha na validação: {validation_result}")
            
            return checkpoint_result
            
        except Exception as e:
            logger.error(f"Erro durante validação: {e}")
            raise
    
    def generate_data_docs(self):
        """Gera documentação dos dados."""
        try:
            self.context.build_data_docs()
            logger.info("Documentação de dados gerada com sucesso")
            
            # Encontra URL da documentação
            docs_sites = self.context.get_docs_sites_urls()
            if docs_sites:
                for site_name, site_url in docs_sites.items():
                    logger.info(f"Documentação disponível em: {site_url}")
            
        except Exception as e:
            logger.error(f"Erro ao gerar documentação: {e}")

def main():
    """Função principal para setup e execução."""
    from dotenv import load_dotenv
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Verifica variáveis obrigatórias
    required_vars = ['GCP_PROJECT_ID', 'BQ_DATASET_RAW']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Variáveis de ambiente não definidas: {missing_vars}")
        sys.exit(1)
    
    try:
        # Inicializa validador
        validator = HealthDataValidator()
        
        # Configura componentes
        validator.create_datasource()
        validator.create_expectation_suites()
        validator.create_checkpoint()
        
        # Executa validação
        result = validator.run_validation()
        
        # Gera documentação
        validator.generate_data_docs()
        
        # Retorna código de saída baseado no sucesso
        sys.exit(0 if result.success else 1)
        
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()