{{
  config(
    materialized='table',
    description='Dashboard consolidado para visualização no Looker Studio'
  )
}}

WITH base_data AS (
    SELECT 
        data_referencia,
        regiao,
        
        -- Métricas principais
        total_atendimentos,
        atendimentos_sus,
        atendimentos_privados,
        total_emergencias,
        
        -- Tempos de espera
        tempo_espera_sus,
        tempo_espera_privado,
        diferencial_tempo_espera,
        
        -- Capacidade hospitalar
        capacidade_total_leitos,
        leitos_publicos,
        leitos_privados,
        total_leitos_disponiveis,
        leitos_disponiveis_publicos,
        leitos_disponiveis_privados,
        
        -- Ocupação
        ocupacao_media_publica,
        ocupacao_media_privada,
        
        -- Indicadores de pressão
        hospitais_publicos_criticos,
        hospitais_privados_criticos,
        status_pressao_sistema,
        score_qualidade_regiao,
        
        -- Percentuais
        pct_atendimentos_sus,
        pct_leitos_publicos,
        
        -- Flags
        flag_ocupacao_critica,
        flag_hospitais_criticos,
        flag_tempo_espera_alto,
        flag_poucos_leitos_disponiveis
        
    FROM {{ ref('core_indicadores_saude') }}
),

dashboard_data AS (
    SELECT 
        *,
        
        -- Campos para série temporal
        EXTRACT(YEAR FROM data_referencia) AS ano,
        EXTRACT(MONTH FROM data_referencia) AS mes,
        EXTRACT(DAYOFWEEK FROM data_referencia) AS dia_semana,
        FORMAT_DATE('%Y-%m', data_referencia) AS ano_mes,
        FORMAT_DATE('%A', data_referencia) AS nome_dia_semana,
        
        -- KPIs calculados para cards do dashboard
        ROUND(tempo_espera_sus, 1) AS tempo_espera_sus_formatado,
        ROUND(ocupacao_media_publica, 1) AS ocupacao_publica_formatada,
        ROUND(score_qualidade_regiao, 1) AS score_qualidade_formatado,
        
        -- Classificações textuais para filtros
        CASE 
            WHEN pct_atendimentos_sus >= 80 THEN 'Predominantemente SUS'
            WHEN pct_atendimentos_sus >= 60 THEN 'Maioria SUS'
            WHEN pct_atendimentos_sus >= 40 THEN 'Misto'
            ELSE 'Predominantemente Privado'
        END AS perfil_atendimento,
        
        CASE 
            WHEN ocupacao_media_publica <= 60 THEN 'Baixa Ocupação'
            WHEN ocupacao_media_publica <= 80 THEN 'Ocupação Moderada'
            WHEN ocupacao_media_publica <= 90 THEN 'Alta Ocupação'
            ELSE 'Ocupação Crítica'
        END AS nivel_ocupacao_textual,
        
        CASE 
            WHEN score_qualidade_regiao >= 80 THEN 'Excelente'
            WHEN score_qualidade_regiao >= 60 THEN 'Bom'
            WHEN score_qualidade_regiao >= 40 THEN 'Regular'
            ELSE 'Ruim'
        END AS classificacao_qualidade,
        
        -- Métricas per capita (assumindo população estimada por região)
        CASE 
            WHEN regiao = 'ZONA SUL' THEN SAFE_DIVIDE(total_atendimentos, 1200000) * 1000
            WHEN regiao = 'ZONA NORTE' THEN SAFE_DIVIDE(total_atendimentos, 2800000) * 1000
            WHEN regiao = 'ZONA OESTE' THEN SAFE_DIVIDE(total_atendimentos, 1800000) * 1000
            WHEN regiao = 'CENTRO' THEN SAFE_DIVIDE(total_atendimentos, 400000) * 1000
            ELSE NULL
        END AS atendimentos_por_mil_habitantes,
        
        -- Indicadores de eficiência
        SAFE_DIVIDE(total_atendimentos, capacidade_total_leitos) AS ratio_atendimento_leito,
        SAFE_DIVIDE(total_leitos_disponiveis, capacidade_total_leitos) * 100 AS pct_leitos_disponiveis,
        
        -- Cores para visualização (hex codes)
        CASE 
            WHEN flag_ocupacao_critica THEN '#FF4444'  -- Vermelho
            WHEN ocupacao_media_publica > 75 THEN '#FFA500'  -- Laranja
            ELSE '#4CAF50'  -- Verde
        END AS cor_status_ocupacao,
        
        CASE 
            WHEN tempo_espera_sus > 90 THEN '#FF4444'
            WHEN tempo_espera_sus > 60 THEN '#FFA500'
            ELSE '#4CAF50'
        END AS cor_tempo_espera,
        
        -- Metas e benchmarks
        60 AS meta_tempo_espera,  -- Meta de 60 minutos
        75 AS meta_ocupacao,      -- Meta de 75% ocupação
        70 AS meta_score_qualidade, -- Meta de score 70
        
        -- Variação em relação ao dia anterior
        LAG(total_atendimentos) OVER (PARTITION BY regiao ORDER BY data_referencia) AS atendimentos_dia_anterior,
        LAG(ocupacao_media_publica) OVER (PARTITION BY regiao ORDER BY data_referencia) AS ocupacao_dia_anterior,
        
        -- Médias móveis (7 dias)
        AVG(total_atendimentos) OVER (
            PARTITION BY regiao 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7d_atendimentos,
        
        AVG(ocupacao_media_publica) OVER (
            PARTITION BY regiao 
            ORDER BY data_referencia 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS media_movel_7d_ocupacao
        
    FROM base_data
),

final_dashboard AS (
    SELECT 
        *,
        
        -- Variações percentuais
        SAFE_DIVIDE(total_atendimentos - atendimentos_dia_anterior, atendimentos_dia_anterior) * 100 AS variacao_pct_atendimentos,
        ocupacao_media_publica - ocupacao_dia_anterior AS variacao_ocupacao,
        
        -- Status em relação às metas
        CASE 
            WHEN tempo_espera_sus <= meta_tempo_espera THEN 'Meta Atingida'
            ELSE 'Abaixo da Meta'
        END AS status_meta_tempo,
        
        CASE 
            WHEN ocupacao_media_publica <= meta_ocupacao THEN 'Meta Atingida'
            ELSE 'Acima da Meta'
        END AS status_meta_ocupacao,
        
        CASE 
            WHEN score_qualidade_regiao >= meta_score_qualidade THEN 'Meta Atingida'
            ELSE 'Abaixo da Meta'
        END AS status_meta_qualidade,
        
        -- Alertas consolidados
        CASE 
            WHEN (flag_ocupacao_critica OR flag_tempo_espera_alto OR flag_hospitais_criticos) THEN 'CRÍTICO'
            WHEN (ocupacao_media_publica > 80 OR tempo_espera_sus > 75) THEN 'ATENÇÃO'
            ELSE 'NORMAL'
        END AS nivel_alerta_geral,
        
        -- Texto descritivo para tooltips
        CONCAT(
            'Região: ', regiao, '\n',
            'Atendimentos: ', FORMAT('%d', total_atendimentos), '\n',
            'Tempo Espera SUS: ', FORMAT('%.1f min', tempo_espera_sus), '\n',
            'Ocupação: ', FORMAT('%.1f%%', ocupacao_media_publica), '\n',
            'Score Qualidade: ', FORMAT('%.1f', score_qualidade_regiao)
        ) AS tooltip_resumo
        
    FROM dashboard_data
)

SELECT * FROM final_dashboard
ORDER BY data_referencia DESC, regiao