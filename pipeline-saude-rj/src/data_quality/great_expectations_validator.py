"""
Great Expectations Data Quality Suite para Pipeline de Saúde RJ
Implementa validações críticas de qualidade dos dados extraídos
"""

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import pandas as pd
import os
from datetime import datetime

class HealthDataQualityValidator:
    """
    Classe para validar qualidade dos dados de saúde do Rio de Janeiro
    """
    
    def __init__(self, data_context_root_dir="./great_expectations"):
        """Inicializa o contexto do Great Expectations"""
        self.context = gx.get_context(context_root_dir=data_context_root_dir)
        
    def setup_data_quality_suite(self):
        """Configura suite de expectativas para dados de saúde"""
        
        # Suite para Atendimentos
        atendimentos_suite = self.context.suites.add(
            gx.ExpectationSuite(name="atendimentos_quality_suite")
        )
        
        # Validações de Schema - Atendimentos
        atendimentos_expectations = [
            # Colunas obrigatórias
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={
                    "column_list": [
                        "id_atendimento", "paciente_id", "unidade_saude", 
                        "tipo_atendimento", "data_entrada", "hora_entrada",
                        "tempo_espera_minutos", "status_atendimento", "prioridade",
                        "profissional_responsavel", "data_extracao", "fonte"
                    ]
                }
            ),
            # Não pode ter valores nulos em colunas críticas
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id_atendimento"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null", 
                kwargs={"column": "tipo_atendimento"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "data_entrada"}
            ),
            # Tempo de espera deve ser não-negativo
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "tempo_espera_minutos",
                    "min_value": 0,
                    "max_value": 1440  # máximo 24h
                }
            ),
            # Tipos de atendimento válidos
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "tipo_atendimento",
                    "value_set": ["Consulta", "Exame", "Cirurgia", "Emergencia", "Urgencia"]
                }
            ),
            # Status válidos
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "status_atendimento", 
                    "value_set": ["Em Andamento", "Concluido", "Aguardando", "Cancelado"]
                }
            ),
            # Prioridade válida
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "prioridade",
                    "value_set": ["Baixa", "Normal", "Alta", "Critica"]
                }
            ),
            # IDs únicos
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "id_atendimento"}
            )
        ]
        
        for expectation in atendimentos_expectations:
            atendimentos_suite.add_expectation(expectation)
            
        # Suite para Leitos
        leitos_suite = self.context.suites.add(
            gx.ExpectationSuite(name="leitos_quality_suite")
        )
        
        leitos_expectations = [
            # Ocupação entre 0 e 100%
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "percentual_ocupacao",
                    "min_value": 0,
                    "max_value": 100
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between", 
                kwargs={
                    "column": "percentual_ocupacao_uti",
                    "min_value": 0,
                    "max_value": 100
                }
            ),
            # Leitos ocupados <= leitos totais
            ExpectationConfiguration(
                expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
                kwargs={
                    "column_A": "leitos_totais",
                    "column_B": "leitos_ocupados"
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
                kwargs={
                    "column_A": "leitos_uti_totais", 
                    "column_B": "leitos_uti_ocupados"
                }
            ),
            # Tipos de hospital válidos
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "tipo_hospital",
                    "value_set": ["Público", "Privado", "Filantrópico"]
                }
            )
        ]
        
        for expectation in leitos_expectations:
            leitos_suite.add_expectation(expectation)
            
        # Suite para Unidades de Saúde
        unidades_suite = self.context.suites.add(
            gx.ExpectationSuite(name="unidades_quality_suite")  
        )
        
        unidades_expectations = [
            # Coordenadas válidas para Rio de Janeiro
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "latitude",
                    "min_value": -23.1,  # Limite sul do RJ
                    "max_value": -22.7   # Limite norte do RJ
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "longitude", 
                    "min_value": -43.8,  # Limite oeste do RJ
                    "max_value": -43.0   # Limite leste do RJ
                }
            ),
            # Tipos de unidade válidos
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "tipo_unidade",
                    "value_set": ["UPA", "Hospital", "Clínica", "Posto de Saúde", "CAPS"]
                }
            ),
            # Telefone no formato brasileiro
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "telefone",
                    "regex": r"^\(\d{2}\)\s\d{4,5}-\d{4}$"
                }
            )
        ]
        
        for expectation in unidades_expectations:
            unidades_suite.add_expectation(expectation)
            
        print("✅ Suites de qualidade configuradas:")
        print("   - atendimentos_quality_suite")  
        print("   - leitos_quality_suite")
        print("   - unidades_quality_suite")
        
    def validate_data_quality(self, data_dir="./data_output"):
        """Executa validação de qualidade nos dados extraídos"""
        
        results = {}
        
        # Validar atendimentos
        if os.path.exists(f"{data_dir}/atendimentos.csv"):
            df_atendimentos = pd.read_csv(f"{data_dir}/atendimentos.csv")
            
            validator = self.context.sources.add_pandas(name="atendimentos_source") \
                .add_asset(name="atendimentos") \
                .add_batch_request_from_dataframe(df_atendimentos)
                
            results["atendimentos"] = validator.validate(
                expectation_suite_name="atendimentos_quality_suite"
            )
            
        # Validar leitos  
        if os.path.exists(f"{data_dir}/leitos_disponibilidade.csv"):
            df_leitos = pd.read_csv(f"{data_dir}/leitos_disponibilidade.csv")
            
            validator = self.context.sources.add_pandas(name="leitos_source") \
                .add_asset(name="leitos") \
                .add_batch_request_from_dataframe(df_leitos)
                
            results["leitos"] = validator.validate(
                expectation_suite_name="leitos_quality_suite"
            )
            
        # Validar unidades
        if os.path.exists(f"{data_dir}/unidades_saude.csv"):
            df_unidades = pd.read_csv(f"{data_dir}/unidades_saude.csv")
            
            validator = self.context.sources.add_pandas(name="unidades_source") \
                .add_asset(name="unidades") \
                .add_batch_request_from_dataframe(df_unidades)
                
            results["unidades"] = validator.validate(
                expectation_suite_name="unidades_quality_suite"
            )
            
        return results
        
    def generate_quality_report(self, validation_results):
        """Gera relatório de qualidade dos dados"""
        
        print("\n🔍 RELATÓRIO DE QUALIDADE DOS DADOS")
        print("="*50)
        
        total_tests = 0
        passed_tests = 0
        
        for dataset_name, result in validation_results.items():
            print(f"\n📊 Dataset: {dataset_name}")
            
            dataset_passed = 0
            dataset_total = len(result.results)
            
            for test_result in result.results:
                total_tests += 1
                if test_result.success:
                    passed_tests += 1
                    dataset_passed += 1
                    
            success_rate = (dataset_passed / dataset_total) * 100 if dataset_total > 0 else 0
            print(f"   ✅ Testes aprovados: {dataset_passed}/{dataset_total} ({success_rate:.1f}%)")
            
            # Mostrar falhas
            failed_tests = [r for r in result.results if not r.success]
            if failed_tests:
                print("   ⚠️ Testes que falharam:")
                for failed in failed_tests[:3]:  # Mostrar apenas os primeiros 3
                    print(f"      - {failed.expectation_config.expectation_type}")
                    
        overall_success = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        print(f"\n🎯 QUALIDADE GERAL: {passed_tests}/{total_tests} ({overall_success:.1f}%)")
        
        if overall_success >= 95:
            print("✅ EXCELENTE - Dados prontos para produção!")
        elif overall_success >= 85:
            print("⚠️ BOM - Alguns ajustes recomendados")  
        else:
            print("❌ ATENÇÃO - Problemas de qualidade detectados")
            
        return {
            "total_tests": total_tests,
            "passed_tests": passed_tests, 
            "success_rate": overall_success
        }


if __name__ == "__main__":
    # Executar validação de qualidade
    validator = HealthDataQualityValidator()
    
    print("🔧 Configurando suites de qualidade...")
    validator.setup_data_quality_suite()
    
    print("\n🧪 Executando validações...")  
    results = validator.validate_data_quality()
    
    print("\n📋 Gerando relatório...")
    summary = validator.generate_quality_report(results)