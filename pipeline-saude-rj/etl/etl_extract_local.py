#!/usr/bin/env python3
"""
ETL Extract Script Simplificado - Pipeline Saúde RJ
Versão de teste local sem Google Cloud Storage
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict
import pandas as pd
import requests
from dotenv import load_dotenv

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LocalHealthDataExtractor:
    """Extrator de dados de saúde para teste local."""
    
    def __init__(self):
        """Inicializa o extrator."""
        load_dotenv()
        
        # Cria pasta local para salvar dados
        self.output_dir = "data_output"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract_atendimentos_emergencia(self) -> pd.DataFrame:
        """Extrai dados de atendimentos de emergência (simulados)."""
        try:
            logger.info("Gerando dados simulados de atendimentos...")
            
            # Dados simulados para demonstração
            dados_simulados = [
                {
                    'id_atendimento': f'ATD{i:06d}',
                    'data_atendimento': (datetime.now() - pd.Timedelta(days=i%30)).strftime('%Y-%m-%d'),
                    'tipo_atendimento': ['Emergencia', 'Consulta', 'Exame', 'Cirurgia'][i%4],
                    'tempo_espera_minutos': (i * 15) % 120,
                    'id_unidade': f'UPA{(i%10)+1:02d}',
                    'bairro': ['Copacabana', 'Ipanema', 'Tijuca', 'Centro', 'Barra'][i%5],
                    'tipo_unidade': ['UPA', 'Hospital', 'Clínica'][i%3],
                    'sus_privado': ['SUS', 'Privado'][i%2],
                    'idade_paciente': 18 + (i % 60),
                    'sexo_paciente': ['M', 'F'][i%2]
                }
                for i in range(1, 101)  # 100 registros simulados
            ]
            
            df = pd.DataFrame(dados_simulados)
            df['data_extracao'] = datetime.now()
            df['fonte'] = 'DataSUS_Simulado'
            
            logger.info(f"✅ Gerados {len(df)} registros de atendimentos")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao gerar atendimentos: {e}")
            return pd.DataFrame()
    
    def extract_leitos_disponibilidade(self) -> pd.DataFrame:
        """Extrai dados de disponibilidade de leitos."""
        try:
            logger.info("Gerando dados simulados de leitos...")
            
            # Dados simulados de leitos
            dados_leitos = [
                {
                    'id_unidade': f'HOSP{i:03d}',
                    'nome_unidade': f'Hospital {chr(65+i)}',
                    'bairro': ['Centro', 'Zona Sul', 'Zona Norte', 'Barra', 'Tijuca'][i%5],
                    'leitos_totais': 50 + (i * 10) % 200,
                    'leitos_ocupados': 20 + (i * 7) % 150,
                    'leitos_uti_totais': 10 + (i * 2) % 30,
                    'leitos_uti_ocupados': 5 + (i * 1) % 25,
                    'data_atualizacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'tipo_hospital': ['Público', 'Privado', 'Filantrópico'][i%3]
                }
                for i in range(20)  # 20 hospitais
            ]
            
            df = pd.DataFrame(dados_leitos)
            
            # Calcula percentuais de ocupação
            df['percentual_ocupacao'] = (df['leitos_ocupados'] / df['leitos_totais'] * 100).round(2)
            df['percentual_ocupacao_uti'] = (df['leitos_uti_ocupados'] / df['leitos_uti_totais'] * 100).round(2)
            
            df['data_extracao'] = datetime.now()
            df['fonte'] = 'SMS_RJ_Simulado'
            
            logger.info(f"✅ Gerados {len(df)} registros de leitos")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao gerar dados de leitos: {e}")
            return pd.DataFrame()
    
    def extract_unidades_saude(self) -> pd.DataFrame:
        """Extrai dados de unidades de saúde."""
        try:
            logger.info("Gerando dados simulados de unidades...")
            
            unidades = [
                {
                    'id': f'UNI{i:03d}',
                    'nome': f'Unidade de Saúde {chr(65+i)}',
                    'endereco': f'Rua das Flores, {100+i*10}',
                    'bairro': ['Copacabana', 'Ipanema', 'Tijuca', 'Centro', 'Barra', 'Botafogo'][i%6],
                    'tipo_unidade': ['UPA', 'Hospital', 'Clínica', 'Posto de Saúde'][i%4],
                    'telefone': f'(21) 9999-{1000+i}',
                    'latitude': -22.9 + (i * 0.01) % 0.3,
                    'longitude': -43.2 - (i * 0.01) % 0.3
                }
                for i in range(15)  # 15 unidades
            ]
            
            df = pd.DataFrame(unidades)
            df['data_extracao'] = datetime.now()
            df['fonte'] = 'DataRio_Simulado'
            
            logger.info(f"✅ Gerados {len(df)} registros de unidades")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao gerar unidades: {e}")
            return pd.DataFrame()
    
    def save_to_local(self, df: pd.DataFrame, filename: str) -> bool:
        """Salva DataFrame em CSV localmente."""
        try:
            if df.empty:
                logger.warning(f"DataFrame vazio, pulando salvamento de {filename}")
                return False
            
            # Converte parquet para CSV
            csv_filename = filename.replace('.parquet', '.csv')
            filepath = os.path.join(self.output_dir, csv_filename)
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"✅ Arquivo salvo: {filepath} ({len(df)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar {filename}: {e}")
            return False
    
    def run_extraction(self) -> Dict[str, bool]:
        """Executa o processo completo de extração."""
        logger.info("🚀 Iniciando extração local de dados de saúde")
        
        results = {}
        
        # Extrai atendimentos
        logger.info("📊 Extraindo dados de atendimentos...")
        df_atendimentos = self.extract_atendimentos_emergencia()
        results['atendimentos'] = self.save_to_local(df_atendimentos, "atendimentos.parquet")
        
        # Extrai dados de leitos
        logger.info("🏥 Extraindo dados de leitos...")
        df_leitos = self.extract_leitos_disponibilidade()
        results['leitos'] = self.save_to_local(df_leitos, "leitos_disponibilidade.parquet")
        
        # Extrai unidades de saúde
        logger.info("🏢 Extraindo dados de unidades...")
        df_unidades = self.extract_unidades_saude()
        results['unidades'] = self.save_to_local(df_unidades, "unidades_saude.parquet")
        
        # Log do resultado final
        sucessos = sum(results.values())
        total = len(results)
        
        if sucessos == total:
            logger.info(f"🎉 Extração concluída com SUCESSO: {sucessos}/{total} datasets")
        else:
            logger.error(f"⚠️ Extração parcial: {sucessos}/{total} datasets")
        
        return results

def main():
    """Função principal."""
    try:
        print("="*60)
        print("  🏥 PIPELINE SAÚDE RJ - EXTRAÇÃO LOCAL")
        print("="*60)
        
        extractor = LocalHealthDataExtractor()
        results = extractor.run_extraction()
        
        # Mostra resumo
        print("\n" + "="*60)
        print("  📋 RESUMO DA EXTRAÇÃO")
        print("="*60)
        
        for dataset, success in results.items():
            status = "✅ SUCESSO" if success else "❌ FALHA"
            print(f"  {dataset.ljust(15)}: {status}")
        
        print("\n" + "="*60)
        print(f"  📁 Arquivos salvos em: {extractor.output_dir}/")
        print("="*60)
        
        # Retorna código de saída
        if all(results.values()):
            print("\n🎉 Extração realizada com sucesso!")
            sys.exit(0)
        else:
            print("\n⚠️ Algumas extrações falharam!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()