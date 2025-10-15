#!/usr/bin/env python3
"""
ETL Extract Script - Pipeline Saúde RJ
Extrai dados de APIs públicas sobre saúde no Rio de Janeiro
e salva em formato parquet no Google Cloud Storage.
"""

import os
import sys
import logging
import json
from datetime import datetime, date
from typing import Dict, List, Optional
import pandas as pd
import requests
from google.cloud import storage
from dotenv import load_dotenv

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HealthDataExtractor:
    """Extrator de dados de saúde do Rio de Janeiro."""
    
    def __init__(self):
        """Inicializa o extrator com configurações."""
        load_dotenv()
        
        self.gcp_project_id = os.getenv('GCP_PROJECT_ID')
        self.gcs_bucket_raw = os.getenv('GCS_BUCKET_RAW', 'data-saude-brutos')
        self.datario_base_url = os.getenv('DATARIO_BASE_URL')
        
        # Inicializa cliente do GCS
        try:
            self.gcs_client = storage.Client(project=self.gcp_project_id)
            self.bucket = self.gcs_client.bucket(self.gcs_bucket_raw)
        except Exception as e:
            logger.error(f"Erro ao conectar com GCS: {e}")
            raise
    
    def extract_unidades_saude(self) -> pd.DataFrame:
        """Extrai dados de unidades de saúde do DataRio."""
        try:
            # ID do dataset de unidades de saúde no DataRio
            resource_id = "b8396dd5-2761-40d4-a4e2-924706c7b20e"
            
            params = {
                'resource_id': resource_id,
                'limit': 1000,  # Ajustar conforme necessário
                'offset': 0
            }
            
            response = requests.get(self.datario_base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success'):
                records = data['result']['records']
                df = pd.DataFrame(records)
                
                # Adiciona metadados
                df['data_extracao'] = datetime.now()
                df['fonte'] = 'DataRio'
                
                logger.info(f"Extraídos {len(df)} registros de unidades de saúde")
                return df
            else:
                raise Exception(f"Erro na API DataRio: {data}")
                
        except Exception as e:
            logger.error(f"Erro ao extrair unidades de saúde: {e}")
            return pd.DataFrame()
    
    def extract_atendimentos_emergencia(self) -> pd.DataFrame:
        """Extrai dados de atendimentos de emergência."""
        try:
            # Simulação de dados de atendimento (substituir por API real)
            # Em produção, conectar com API do DataSUS ou similar
            
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
                for i in range(1, 1001)  # 1000 registros simulados
            ]
            
            df = pd.DataFrame(dados_simulados)
            df['data_extracao'] = datetime.now()
            df['fonte'] = 'DataSUS_Simulado'
            
            logger.info(f"Extraídos {len(df)} registros de atendimentos")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao extrair atendimentos: {e}")
            return pd.DataFrame()
    
    def extract_leitos_disponibilidade(self) -> pd.DataFrame:
        """Extrai dados de disponibilidade de leitos."""
        try:
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
                for i in range(50)  # 50 hospitais
            ]
            
            df = pd.DataFrame(dados_leitos)
            
            # Calcula percentuais de ocupação
            df['percentual_ocupacao'] = (df['leitos_ocupados'] / df['leitos_totais'] * 100).round(2)
            df['percentual_ocupacao_uti'] = (df['leitos_uti_ocupados'] / df['leitos_uti_totais'] * 100).round(2)
            
            df['data_extracao'] = datetime.now()
            df['fonte'] = 'SMS_RJ_Simulado'
            
            logger.info(f"Extraídos {len(df)} registros de leitos")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados de leitos: {e}")
            return pd.DataFrame()
    
    def save_to_gcs(self, df: pd.DataFrame, filename: str) -> bool:
        """Salva DataFrame em parquet no Google Cloud Storage."""
        try:
            if df.empty:
                logger.warning(f"DataFrame vazio, pulando upload de {filename}")
                return False
            
            # Converte DataFrame para parquet em memória
            parquet_buffer = df.to_parquet(index=False, engine='pyarrow')
            
            # Upload para GCS
            blob_name = f"raw/{datetime.now().strftime('%Y/%m/%d')}/{filename}"
            blob = self.bucket.blob(blob_name)
            
            blob.upload_from_string(
                parquet_buffer,
                content_type='application/octet-stream'
            )
            
            logger.info(f"Arquivo salvo com sucesso: gs://{self.gcs_bucket_raw}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar {filename} no GCS: {e}")
            return False
    
    def run_extraction(self) -> Dict[str, bool]:
        """Executa o processo completo de extração."""
        logger.info("Iniciando processo de extração de dados de saúde")
        
        results = {}
        
        # Extrai unidades de saúde
        logger.info("Extraindo dados de unidades de saúde...")
        df_unidades = self.extract_unidades_saude()
        results['unidades'] = self.save_to_gcs(df_unidades, "unidades_saude.parquet")
        
        # Extrai atendimentos
        logger.info("Extraindo dados de atendimentos...")
        df_atendimentos = self.extract_atendimentos_emergencia()
        results['atendimentos'] = self.save_to_gcs(df_atendimentos, "atendimentos.parquet")
        
        # Extrai dados de leitos
        logger.info("Extraindo dados de leitos...")
        df_leitos = self.extract_leitos_disponibilidade()
        results['leitos'] = self.save_to_gcs(df_leitos, "leitos_disponibilidade.parquet")
        
        # Log do resultado final
        sucessos = sum(results.values())
        total = len(results)
        logger.info(f"Extração concluída: {sucessos}/{total} datasets extraídos com sucesso")
        
        return results

def main():
    """Função principal."""
    try:
        extractor = HealthDataExtractor()
        results = extractor.run_extraction()
        
        # Retorna código de saída baseado no sucesso
        if all(results.values()):
            logger.info("Todos os datasets foram extraídos com sucesso!")
            sys.exit(0)
        else:
            logger.error("Alguns datasets falharam na extração")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()