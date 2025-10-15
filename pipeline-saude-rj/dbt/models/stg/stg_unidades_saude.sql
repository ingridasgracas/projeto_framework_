{{
  config(
    materialized='view',
    description='Dados de unidades de saúde limpos e tipados'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'unidades_saude') }}
),

cleaned_data AS (
    SELECT
        -- Identificadores
        id AS id_unidade,
        
        -- Informações básicas
        UPPER(TRIM(nome)) AS nome_unidade,
        TRIM(endereco) AS endereco,
        UPPER(TRIM(bairro)) AS bairro,
        
        -- Classificação
        CASE 
            WHEN UPPER(TRIM(tipo_unidade)) LIKE '%UPA%' THEN 'UPA'
            WHEN UPPER(TRIM(tipo_unidade)) LIKE '%HOSPITAL%' THEN 'HOSPITAL'
            WHEN UPPER(TRIM(tipo_unidade)) LIKE '%CLÍNICA%' OR UPPER(TRIM(tipo_unidade)) LIKE '%CLINICA%' THEN 'CLÍNICA'
            WHEN UPPER(TRIM(tipo_unidade)) LIKE '%POSTO%' THEN 'POSTO DE SAÚDE'
            WHEN UPPER(TRIM(tipo_unidade)) LIKE '%CENTRO%' THEN 'CENTRO DE SAÚDE'
            ELSE UPPER(TRIM(tipo_unidade))
        END AS tipo_unidade,
        
        -- Contato
        TRIM(telefone) AS telefone,
        
        -- Localização geográfica
        SAFE_CAST(latitude AS FLOAT64) AS latitude,
        SAFE_CAST(longitude AS FLOAT64) AS longitude,
        
        -- Metadados
        data_extracao,
        fonte
        
    FROM source_data
    WHERE id IS NOT NULL
      AND nome IS NOT NULL
)

SELECT 
    *,
    -- Região baseada no bairro (simplificado)
    CASE 
        WHEN bairro IN ('COPACABANA', 'IPANEMA', 'LEBLON', 'BOTAFOGO', 'FLAMENGO', 'LARANJEIRAS') THEN 'ZONA SUL'
        WHEN bairro IN ('TIJUCA', 'VILA ISABEL', 'GRAJAÚ', 'MARACANÃ', 'ANDARAÍ') THEN 'ZONA NORTE'
        WHEN bairro IN ('BARRA DA TIJUCA', 'RECREIO', 'JACAREPAGUÁ', 'FREGUESIA') THEN 'ZONA OESTE'
        WHEN bairro IN ('CENTRO', 'LAPA', 'SANTA TERESA', 'CATETE', 'GLÓRIA') THEN 'CENTRO'
        ELSE 'OUTRAS'
    END AS regiao,
    
    -- Validação de coordenadas
    CASE 
        WHEN latitude BETWEEN -23.1 AND -22.7 AND longitude BETWEEN -43.8 AND -43.1 THEN TRUE
        ELSE FALSE
    END AS coordenadas_validas

FROM cleaned_data