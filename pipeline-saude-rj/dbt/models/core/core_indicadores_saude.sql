{{
  config(
    materialized='table',
    description='Indicadores consolidados da rede de saúde do Rio de Janeiro'
  )
}}

WITH atendimentos_agregados AS (
    SELECT 
        data_atendimento,
        regiao,
        sus_privado,
        COUNT(*) AS total_atendimentos_dia,
        AVG(tempo_espera_medio) AS tempo_espera_medio_regiao,
        SUM(atendimentos_emergencia) AS total_emergencias,
        SUM(total_atendimentos) AS total_atendimentos_consolidado
    FROM {{ ref('core_metricas_atendimento') }}
    GROUP BY 1, 2, 3
),

leitos_agregados AS (
    SELECT 
        data_referencia,
        regiao,
        tipo_hospital,
        COUNT(*) AS total_hospitais,
        SUM(leitos_totais) AS capacidade_leitos,
        SUM(leitos_disponiveis) AS leitos_disponiveis,
        AVG(percentual_ocupacao) AS ocupacao_media,
        COUNTIF(nivel_criticidade = 'CRÍTICO') AS hospitais_criticos
    FROM {{ ref('core_ocupacao_leitos') }}
    GROUP BY 1, 2, 3
),

indicadores_consolidados AS (
    SELECT 
        COALESCE(a.data_atendimento, l.data_referencia) AS data_referencia,
        COALESCE(a.regiao, l.regiao) AS regiao,
        
        -- Indicadores de atendimento SUS
        COALESCE(SUM(CASE WHEN a.sus_privado = 'SUS' THEN a.total_atendimentos_dia END), 0) AS atendimentos_sus,
        COALESCE(SUM(CASE WHEN a.sus_privado = 'PRIVADO' THEN a.total_atendimentos_dia END), 0) AS atendimentos_privados,
        COALESCE(SUM(a.total_emergencias), 0) AS total_emergencias,
        
        -- Tempo médio de espera por região
        AVG(CASE WHEN a.sus_privado = 'SUS' THEN a.tempo_espera_medio_regiao END) AS tempo_espera_sus,
        AVG(CASE WHEN a.sus_privado = 'PRIVADO' THEN a.tempo_espera_medio_regiao END) AS tempo_espera_privado,
        
        -- Indicadores de leitos públicos vs privados
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PÚBLICO' THEN l.capacidade_leitos END), 0) AS leitos_publicos,
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PRIVADO' THEN l.capacidade_leitos END), 0) AS leitos_privados,
        
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PÚBLICO' THEN l.leitos_disponiveis END), 0) AS leitos_disponiveis_publicos,
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PRIVADO' THEN l.leitos_disponiveis END), 0) AS leitos_disponiveis_privados,
        
        -- Ocupação média por tipo
        AVG(CASE WHEN l.tipo_hospital = 'PÚBLICO' THEN l.ocupacao_media END) AS ocupacao_media_publica,
        AVG(CASE WHEN l.tipo_hospital = 'PRIVADO' THEN l.ocupacao_media END) AS ocupacao_media_privada,
        
        -- Contadores de criticidade
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PÚBLICO' THEN l.hospitais_criticos END), 0) AS hospitais_publicos_criticos,
        COALESCE(SUM(CASE WHEN l.tipo_hospital = 'PRIVADO' THEN l.hospitais_criticos END), 0) AS hospitais_privados_criticos,
        
        -- Total de hospitais
        COALESCE(SUM(l.total_hospitais), 0) AS total_hospitais_regiao
        
    FROM atendimentos_agregados a
    FULL OUTER JOIN leitos_agregados l 
        ON a.data_atendimento = l.data_referencia 
        AND a.regiao = l.regiao
    GROUP BY 1, 2
)

SELECT 
    *,
    
    -- KPIs calculados
    atendimentos_sus + atendimentos_privados AS total_atendimentos,
    leitos_publicos + leitos_privados AS capacidade_total_leitos,
    leitos_disponiveis_publicos + leitos_disponiveis_privados AS total_leitos_disponiveis,
    
    -- Percentuais
    SAFE_DIVIDE(atendimentos_sus, atendimentos_sus + atendimentos_privados) * 100 AS pct_atendimentos_sus,
    SAFE_DIVIDE(leitos_publicos, leitos_publicos + leitos_privados) * 100 AS pct_leitos_publicos,
    
    -- Relação atendimento/capacidade
    SAFE_DIVIDE(atendimentos_sus + atendimentos_privados, leitos_publicos + leitos_privados) AS ratio_atendimento_leito,
    
    -- Diferencial de tempo de espera
    COALESCE(tempo_espera_sus, 0) - COALESCE(tempo_espera_privado, 0) AS diferencial_tempo_espera,
    
    -- Indicadores de pressão no sistema
    CASE 
        WHEN (ocupacao_media_publica > 85 OR hospitais_publicos_criticos > 0) 
             AND atendimentos_sus > 100 THEN 'PRESSÃO ALTA'
        WHEN ocupacao_media_publica > 70 AND atendimentos_sus > 50 THEN 'PRESSÃO MODERADA'
        ELSE 'PRESSÃO NORMAL'
    END AS status_pressao_sistema,
    
    -- Score de qualidade (0-100)
    CASE 
        WHEN tempo_espera_sus IS NULL THEN NULL
        ELSE GREATEST(0, 
            100 - (tempo_espera_sus / 2) - (ocupacao_media_publica - 50) - (hospitais_publicos_criticos * 10)
        )
    END AS score_qualidade_regiao,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_processamento,
    
    -- Flags de monitoramento
    ocupacao_media_publica > 85 AS flag_ocupacao_critica,
    hospitais_publicos_criticos > 0 AS flag_hospitais_criticos,
    tempo_espera_sus > 90 AS flag_tempo_espera_alto,
    leitos_disponiveis_publicos < 50 AS flag_poucos_leitos_disponiveis

FROM indicadores_consolidados
WHERE data_referencia IS NOT NULL