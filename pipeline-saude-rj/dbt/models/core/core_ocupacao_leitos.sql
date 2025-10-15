{{
  config(
    materialized='table',
    description='Métricas de ocupação de leitos e disponibilidade hospitalar'
  )
}}

WITH leitos_base AS (
    SELECT * FROM {{ ref('stg_leitos') }}
),

unidades_base AS (
    SELECT * FROM {{ ref('stg_unidades_saude') }}
),

metricas_leitos AS (
    SELECT
        l.data_atualizacao,
        DATE(l.data_atualizacao) AS data_referencia,
        l.id_unidade,
        l.nome_unidade,
        l.bairro,
        l.regiao,
        l.tipo_hospital,
        
        -- Métricas de leitos gerais
        l.leitos_totais,
        l.leitos_ocupados,
        l.leitos_disponiveis,
        l.percentual_ocupacao,
        l.status_ocupacao,
        
        -- Métricas de UTI
        l.leitos_uti_totais,
        l.leitos_uti_ocupados,
        l.leitos_uti_disponiveis,
        l.percentual_ocupacao_uti,
        l.status_uti,
        
        -- Enriquecimento com dados da unidade
        u.coordenadas_validas,
        u.latitude,
        u.longitude,
        
        -- Classificações de capacidade
        CASE 
            WHEN l.leitos_totais >= 200 THEN 'GRANDE'
            WHEN l.leitos_totais >= 100 THEN 'MÉDIO'
            WHEN l.leitos_totais >= 50 THEN 'PEQUENO'
            ELSE 'MICRO'
        END AS porte_hospital,
        
        -- Indicadores de criticidade
        CASE 
            WHEN l.percentual_ocupacao >= 95 AND l.percentual_ocupacao_uti >= 90 THEN 'CRÍTICO'
            WHEN l.percentual_ocupacao >= 85 OR l.percentual_ocupacao_uti >= 80 THEN 'ALERTA'
            WHEN l.percentual_ocupacao >= 70 OR l.percentual_ocupacao_uti >= 60 THEN 'ATENÇÃO'
            ELSE 'NORMAL'
        END AS nivel_criticidade,
        
        -- Capacidade de absorção
        CASE 
            WHEN l.leitos_disponiveis >= 20 AND l.leitos_uti_disponiveis >= 5 THEN 'ALTA CAPACIDADE'
            WHEN l.leitos_disponiveis >= 10 AND l.leitos_uti_disponiveis >= 2 THEN 'MÉDIA CAPACIDADE'
            WHEN l.leitos_disponiveis >= 5 OR l.leitos_uti_disponiveis >= 1 THEN 'BAIXA CAPACIDADE'
            ELSE 'SEM CAPACIDADE'
        END AS capacidade_absorcao,
        
        -- Flags de monitoramento
        l.percentual_ocupacao > 90 AS flag_ocupacao_critica,
        l.percentual_ocupacao_uti > 85 AS flag_uti_critica,
        l.leitos_disponiveis <= 5 AS flag_poucos_leitos,
        l.leitos_uti_disponiveis = 0 AS flag_uti_lotada,
        
        -- Metadados
        l.data_extracao,
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM leitos_base l
    LEFT JOIN unidades_base u 
        ON l.id_unidade = u.id_unidade
),

metricas_agregadas_regiao AS (
    SELECT
        data_referencia,
        regiao,
        tipo_hospital,
        
        -- Agregações por região
        COUNT(*) AS total_hospitais,
        SUM(leitos_totais) AS leitos_totais_regiao,
        SUM(leitos_ocupados) AS leitos_ocupados_regiao,
        SUM(leitos_disponiveis) AS leitos_disponiveis_regiao,
        
        SUM(leitos_uti_totais) AS leitos_uti_totais_regiao,
        SUM(leitos_uti_ocupados) AS leitos_uti_ocupados_regiao,
        SUM(leitos_uti_disponiveis) AS leitos_uti_disponiveis_regiao,
        
        -- Percentuais regionais
        SAFE_DIVIDE(SUM(leitos_ocupados), SUM(leitos_totais)) * 100 AS pct_ocupacao_regiao,
        SAFE_DIVIDE(SUM(leitos_uti_ocupados), SUM(leitos_uti_totais)) * 100 AS pct_ocupacao_uti_regiao,
        
        -- Contadores de criticidade
        COUNTIF(nivel_criticidade = 'CRÍTICO') AS hospitais_criticos,
        COUNTIF(nivel_criticidade = 'ALERTA') AS hospitais_alerta,
        COUNTIF(flag_uti_lotada) AS hospitais_uti_lotada,
        
        -- Médias regionais
        AVG(percentual_ocupacao) AS media_ocupacao_regiao,
        AVG(percentual_ocupacao_uti) AS media_ocupacao_uti_regiao
        
    FROM metricas_leitos
    GROUP BY 1, 2, 3
)

-- Resultado final combinando métricas por hospital e por região
SELECT 
    ml.*,
    
    -- Dados regionais para contexto
    mar.leitos_totais_regiao,
    mar.leitos_disponiveis_regiao,
    mar.pct_ocupacao_regiao,
    mar.pct_ocupacao_uti_regiao,
    mar.hospitais_criticos,
    mar.hospitais_alerta,
    mar.media_ocupacao_regiao,
    
    -- Comparação com a média regional
    ml.percentual_ocupacao - mar.media_ocupacao_regiao AS diferenca_media_regiao,
    ml.percentual_ocupacao_uti - mar.media_ocupacao_uti_regiao AS diferenca_media_uti_regiao

FROM metricas_leitos ml
LEFT JOIN metricas_agregadas_regiao mar 
    ON ml.data_referencia = mar.data_referencia 
    AND ml.regiao = mar.regiao 
    AND ml.tipo_hospital = mar.tipo_hospital