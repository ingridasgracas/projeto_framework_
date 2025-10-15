{{
  config(
    materialized='view',
    description='Dados de atendimentos limpos e tipados'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'atendimentos') }}
),

cleaned_data AS (
    SELECT
        -- Identificadores
        id_atendimento,
        id_unidade,
        
        -- Datas e timestamps
        PARSE_DATE('%Y-%m-%d', data_atendimento) AS data_atendimento,
        data_extracao,
        
        -- Informações do atendimento
        UPPER(TRIM(tipo_atendimento)) AS tipo_atendimento,
        COALESCE(tempo_espera_minutos, 0) AS tempo_espera_minutos,
        
        -- Localização
        UPPER(TRIM(bairro)) AS bairro,
        UPPER(TRIM(tipo_unidade)) AS tipo_unidade,
        
        -- Classificação
        CASE 
            WHEN UPPER(TRIM(sus_privado)) IN ('SUS', 'PÚBLICO', 'PUBLICO') THEN 'SUS'
            WHEN UPPER(TRIM(sus_privado)) IN ('PRIVADO', 'PARTICULAR') THEN 'PRIVADO'
            ELSE 'NÃO INFORMADO'
        END AS sus_privado,
        
        -- Informações do paciente
        COALESCE(idade_paciente, 0) AS idade_paciente,
        CASE 
            WHEN UPPER(TRIM(sexo_paciente)) = 'M' THEN 'MASCULINO'
            WHEN UPPER(TRIM(sexo_paciente)) = 'F' THEN 'FEMININO'
            ELSE 'NÃO INFORMADO'
        END AS sexo_paciente,
        
        -- Classificação de idade
        CASE 
            WHEN idade_paciente < 18 THEN 'CRIANÇA/ADOLESCENTE'
            WHEN idade_paciente BETWEEN 18 AND 64 THEN 'ADULTO'
            WHEN idade_paciente >= 65 THEN 'IDOSO'
            ELSE 'NÃO INFORMADO'
        END AS faixa_etaria,
        
        -- Metadados
        fonte
        
    FROM source_data
    WHERE data_atendimento IS NOT NULL
      AND id_atendimento IS NOT NULL
      AND data_extracao >= '{{ var("start_date") }}'
)

SELECT 
    *,
    -- Derived fields
    EXTRACT(YEAR FROM data_atendimento) AS ano,
    EXTRACT(MONTH FROM data_atendimento) AS mes,
    EXTRACT(DAYOFWEEK FROM data_atendimento) AS dia_semana,
    FORMAT_DATE('%Y-%m', data_atendimento) AS ano_mes,
    
    -- Classificação de tempo de espera
    CASE 
        WHEN tempo_espera_minutos <= 15 THEN 'RÁPIDO (≤15min)'
        WHEN tempo_espera_minutos <= 60 THEN 'MODERADO (16-60min)'
        WHEN tempo_espera_minutos <= 120 THEN 'LONGO (1-2h)'
        ELSE 'MUITO LONGO (>2h)'
    END AS classificacao_espera

FROM cleaned_data