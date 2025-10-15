{{
  config(
    materialized='table',
    description='Métricas agregadas de atendimentos por unidade e período'
  )
}}

WITH atendimentos_base AS (
    SELECT * FROM {{ ref('stg_atendimentos') }}
),

metricas_diarias AS (
    SELECT
        data_atendimento,
        id_unidade,
        bairro,
        regiao,
        tipo_unidade,
        sus_privado,
        
        -- Contadores
        COUNT(*) AS total_atendimentos,
        COUNT(DISTINCT id_atendimento) AS atendimentos_unicos,
        
        -- Métricas de tempo de espera
        AVG(tempo_espera_minutos) AS tempo_espera_medio,
        PERCENTILE_CONT(tempo_espera_minutos, 0.5) OVER (PARTITION BY data_atendimento, id_unidade) AS tempo_espera_mediano,
        MIN(tempo_espera_minutos) AS tempo_espera_minimo,
        MAX(tempo_espera_minutos) AS tempo_espera_maximo,
        
        -- Distribuição por tipo de atendimento
        COUNTIF(tipo_atendimento = 'EMERGENCIA') AS atendimentos_emergencia,
        COUNTIF(tipo_atendimento = 'CONSULTA') AS atendimentos_consulta,
        COUNTIF(tipo_atendimento = 'EXAME') AS atendimentos_exame,
        COUNTIF(tipo_atendimento = 'CIRURGIA') AS atendimentos_cirurgia,
        
        -- Distribuição por faixa etária
        COUNTIF(faixa_etaria = 'CRIANÇA/ADOLESCENTE') AS atendimentos_crianca_adolescente,
        COUNTIF(faixa_etaria = 'ADULTO') AS atendimentos_adulto,
        COUNTIF(faixa_etaria = 'IDOSO') AS atendimentos_idoso,
        
        -- Distribuição por sexo
        COUNTIF(sexo_paciente = 'MASCULINO') AS atendimentos_masculino,
        COUNTIF(sexo_paciente = 'FEMININO') AS atendimentos_feminino,
        
        -- Distribuição por tempo de espera
        COUNTIF(classificacao_espera = 'RÁPIDO (≤15min)') AS atendimentos_rapido,
        COUNTIF(classificacao_espera = 'MODERADO (16-60min)') AS atendimentos_moderado,
        COUNTIF(classificacao_espera = 'LONGO (1-2h)') AS atendimentos_longo,
        COUNTIF(classificacao_espera = 'MUITO LONGO (>2h)') AS atendimentos_muito_longo
        
    FROM atendimentos_base
    GROUP BY 1, 2, 3, 4, 5, 6
),

metricas_enriquecidas AS (
    SELECT 
        *,
        
        -- Percentuais por tipo de atendimento
        SAFE_DIVIDE(atendimentos_emergencia, total_atendimentos) * 100 AS pct_emergencia,
        SAFE_DIVIDE(atendimentos_consulta, total_atendimentos) * 100 AS pct_consulta,
        SAFE_DIVIDE(atendimentos_exame, total_atendimentos) * 100 AS pct_exame,
        SAFE_DIVIDE(atendimentos_cirurgia, total_atendimentos) * 100 AS pct_cirurgia,
        
        -- Percentuais por faixa etária
        SAFE_DIVIDE(atendimentos_crianca_adolescente, total_atendimentos) * 100 AS pct_crianca_adolescente,
        SAFE_DIVIDE(atendimentos_adulto, total_atendimentos) * 100 AS pct_adulto,
        SAFE_DIVIDE(atendimentos_idoso, total_atendimentos) * 100 AS pct_idoso,
        
        -- Percentuais por tempo de espera
        SAFE_DIVIDE(atendimentos_rapido, total_atendimentos) * 100 AS pct_atendimento_rapido,
        SAFE_DIVIDE(atendimentos_muito_longo, total_atendimentos) * 100 AS pct_atendimento_muito_longo,
        
        -- Classificação de performance
        CASE 
            WHEN tempo_espera_medio <= 30 AND SAFE_DIVIDE(atendimentos_rapido, total_atendimentos) >= 0.6 THEN 'EXCELENTE'
            WHEN tempo_espera_medio <= 60 AND SAFE_DIVIDE(atendimentos_rapido, total_atendimentos) >= 0.4 THEN 'BOM'
            WHEN tempo_espera_medio <= 90 THEN 'REGULAR'
            ELSE 'RUIM'
        END AS classificacao_performance,
        
        -- Flags de alerta
        tempo_espera_medio > 120 AS alerta_tempo_longo,
        total_atendimentos > (SELECT PERCENTILE_CONT(total_atendimentos, 0.9) FROM metricas_diarias) AS alerta_volume_alto,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM metricas_diarias
)

SELECT * FROM metricas_enriquecidas