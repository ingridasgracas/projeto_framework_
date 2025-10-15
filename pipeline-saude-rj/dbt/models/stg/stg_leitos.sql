{{
  config(
    materialized='view',
    description='Dados de disponibilidade de leitos limpos e tipados'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'leitos_disponibilidade') }}
),

cleaned_data AS (
    SELECT
        -- Identificadores
        id_unidade,
        UPPER(TRIM(nome_unidade)) AS nome_unidade,
        
        -- Localização
        UPPER(TRIM(bairro)) AS bairro,
        
        -- Leitos gerais
        COALESCE(leitos_totais, 0) AS leitos_totais,
        COALESCE(leitos_ocupados, 0) AS leitos_ocupados,
        
        -- Leitos UTI
        COALESCE(leitos_uti_totais, 0) AS leitos_uti_totais,
        COALESCE(leitos_uti_ocupados, 0) AS leitos_uti_ocupados,
        
        -- Percentuais (recalculados para garantir consistência)
        SAFE_DIVIDE(leitos_ocupados, leitos_totais) * 100 AS percentual_ocupacao,
        SAFE_DIVIDE(leitos_uti_ocupados, leitos_uti_totais) * 100 AS percentual_ocupacao_uti,
        
        -- Classificação
        CASE 
            WHEN UPPER(TRIM(tipo_hospital)) = 'PÚBLICO' THEN 'PÚBLICO'
            WHEN UPPER(TRIM(tipo_hospital)) = 'PRIVADO' THEN 'PRIVADO'
            WHEN UPPER(TRIM(tipo_hospital)) = 'FILANTRÓPICO' OR UPPER(TRIM(tipo_hospital)) = 'FILANTROPICO' THEN 'FILANTRÓPICO'
            ELSE 'NÃO INFORMADO'
        END AS tipo_hospital,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_atualizacao) AS data_atualizacao,
        data_extracao,
        fonte
        
    FROM source_data
    WHERE id_unidade IS NOT NULL
      AND leitos_totais > 0
)

SELECT 
    *,
    -- Leitos disponíveis
    (leitos_totais - leitos_ocupados) AS leitos_disponiveis,
    (leitos_uti_totais - leitos_uti_ocupados) AS leitos_uti_disponiveis,
    
    -- Classificações de ocupação
    CASE 
        WHEN percentual_ocupacao <= 50 THEN 'BAIXA OCUPAÇÃO'
        WHEN percentual_ocupacao <= 80 THEN 'OCUPAÇÃO MODERADA'
        WHEN percentual_ocupacao <= 95 THEN 'ALTA OCUPAÇÃO'
        ELSE 'OCUPAÇÃO CRÍTICA'
    END AS status_ocupacao,
    
    CASE 
        WHEN percentual_ocupacao_uti <= 60 THEN 'UTI DISPONÍVEL'
        WHEN percentual_ocupacao_uti <= 85 THEN 'UTI LIMITADA'
        ELSE 'UTI CRÍTICA'
    END AS status_uti,
    
    -- Região
    CASE 
        WHEN bairro IN ('COPACABANA', 'IPANEMA', 'LEBLON', 'BOTAFOGO', 'FLAMENGO', 'LARANJEIRAS') THEN 'ZONA SUL'
        WHEN bairro IN ('TIJUCA', 'VILA ISABEL', 'GRAJAÚ', 'MARACANÃ', 'ANDARAÍ') THEN 'ZONA NORTE'
        WHEN bairro IN ('BARRA DA TIJUCA', 'RECREIO', 'JACAREPAGUÁ', 'FREGUESIA') THEN 'ZONA OESTE'
        WHEN bairro IN ('CENTRO', 'LAPA', 'SANTA TERESA', 'CATETE', 'GLÓRIA') THEN 'CENTRO'
        ELSE 'OUTRAS'
    END AS regiao

FROM cleaned_data