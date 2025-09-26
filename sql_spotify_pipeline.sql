-- Script SQL = espelho do modelo do Python

-- 0) testes de funcionamento do banco
select * from spotify.hist_streaming where pessoa = 'tomas'

SELECT 
    coalesce(master_metadata_track_name,'TOTAL GERAL') as track_name,
    COUNT(*) AS play_count,
    ROUND(SUM(minutes_played)::numeric, 2) AS total_minutes
FROM spotify.hist_streaming
WHERE pessoa = 'tomas'
  AND master_metadata_track_name IS NOT NULL
GROUP BY GROUPING SETS (
    (master_metadata_track_name),  -- normal per-track aggregation
    ()                             -- empty set = grand total
)
ORDER BY total_minutes DESC NULLS LAST;


    SELECT 
        master_metadata_track_name,
        master_metadata_album_artist_name,
        CONCAT(master_metadata_album_artist_name, ' - ', master_metadata_track_name) as music_name,
        COUNT(*) AS play_count,
        ROUND(SUM(minutes_played::numeric), 2) AS minutes_played
    FROM spotify.hist_streaming
    WHERE 1=1
    AND pessoa = 'tomas'
    AND EXTRACT(YEAR FROM ts) = 2025
    GROUP BY 1,2
    order by 5 desc
    limit 2000


-- 1) Metadados
WITH metadata AS (
    SELECT
        column_name AS "Nome da Coluna",
        data_type AS "Tipo de Dado"
    FROM information_schema.columns
    WHERE table_schema = 'spotify'
      AND table_name = 'hist_streaming'
)
select * from metadata


-- 2) Mínimo e máximo de ts por pessoa
SELECT
    pessoa,
    MIN(ts) AS min_value_ts,
    MAX(ts) AS max_value_ts
FROM spotify.hist_streaming
GROUP BY pessoa
ORDER BY pessoa;

-- 3) Mínimo e máximo usados na análise
with a as (
SELECT
    pessoa,
    MIN(ts) AS min_value_ts,
    MAX(ts) AS max_value_ts
FROM spotify.hist_streaming
GROUP BY pessoa
ORDER BY pessoa )
select 
	max(min_value_ts) as min_date, 
	min(max_value_ts) as max_date
from a

-- 4): Intervalo comum entre pessoas + Filtrar base para uma pessoa específica
WITH intervalo_comum AS (
    -- intervalo já calculado no Bloco 4 (ajuste se quiser usar valores fixos)
    SELECT
        date_trunc('month', MAX(min_value_ts)) AS data_inicio,
        date_trunc('day', MIN(max_value_ts)) AS data_fim
    FROM (
        SELECT
            pessoa,
            MIN(ts) AS min_value_ts,
            MAX(ts) AS max_value_ts
        FROM spotify.hist_streaming
        GROUP BY pessoa
    ) AS min_max_por_pessoa
)
SELECT s.*
FROM spotify.hist_streaming s
CROSS JOIN intervalo_comum i
WHERE s.pessoa = 'tomas'  -- equivalente ao filtro_pessoa
  AND s.ts >= i.data_inicio
  AND s.ts <= i.data_fim
ORDER BY s.ts;



-- 5): Média diária de minutos escutados por dia do mês e por ano
WITH data_filtrada AS (
    SELECT
        ts,
        minutes_played
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
),
daily_totals AS (
    -- extrair ano, mês e dia
    SELECT
        EXTRACT(YEAR FROM ts)::int AS year,
        EXTRACT(MONTH FROM ts)::int AS month,
        EXTRACT(DAY FROM ts)::int AS day,
        SUM(minutes_played) AS total_minutes
    FROM data_filtrada
    GROUP BY 1,2,3
),
avg_minutes_by_day_year AS (
    -- média dos totais diários por dia do mês e ano
    SELECT
        day,
        year,
        AVG(total_minutes) AS avg_minutes
    FROM daily_totals
    GROUP BY day, year
)
-- Resultado final pronto para plot
SELECT *
FROM avg_minutes_by_day_year
ORDER BY day, year;



-- 6) Padrões de escuta em 2025 para pessoa 'tomas'

-- 6.1) Total por dia do mês
WITH data_2025 AS (
    SELECT *
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND EXTRACT(YEAR FROM ts) = 2025
)
SELECT
    EXTRACT(DAY FROM ts)::int AS day,
    SUM(minutes_played) AS total_minutes
FROM data_2025
GROUP BY day
ORDER BY day;

-- 6.2) Total por dia da semana
WITH data_2025 AS (
    SELECT *,
           TO_CHAR(ts, 'Day') AS dia
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND EXTRACT(YEAR FROM ts) = 2025
)
SELECT
    dia,
    SUM(minutes_played) AS total_minutes
FROM data_2025
GROUP BY dia
ORDER BY CASE dia
             WHEN 'Monday   ' THEN 1
             WHEN 'Tuesday  ' THEN 2
             WHEN 'Wednesday' THEN 3
             WHEN 'Thursday ' THEN 4
             WHEN 'Friday   ' THEN 5
             WHEN 'Saturday ' THEN 6
             WHEN 'Sunday   ' THEN 7
         END;

-- 6.3) Total por hora do dia - dias de semana
WITH data_2025 AS (
    SELECT *,
           TRIM(TO_CHAR(ts, 'Day')) as dia
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND EXTRACT(YEAR FROM ts) = 2025
)
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    SUM(minutes_played) AS total_minutes
FROM data_2025
WHERE dia not IN ('Saturday', 'Sunday')
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 6.4) Total por hora do dia - final de semana
WITH data_2025 AS (
    SELECT *,
           TRIM(TO_CHAR(ts, 'Day')) as dia
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND EXTRACT(YEAR FROM ts) = 2025
)
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    SUM(minutes_played) AS total_minutes
FROM data_2025
WHERE dia IN ('Saturday', 'Sunday')
GROUP BY hour_of_day
ORDER BY hour_of_day;



-- 7) — Tendência de Escuta ao Longo do Tempo (Série temporal por minuto/hora/dia)
WITH daily_minutes AS (
    SELECT
        DATE(ts) AS ts,
        SUM(minutes_played) AS minutes_played
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY DATE(ts)
),
daily_converted AS (
    SELECT
        ts,
        minutes_played,
        (minutes_played / 60.0) AS hours_played,
        (minutes_played / 60.0 / 24.0) AS days_played
    FROM daily_minutes
)
SELECT
    ts,
    minutes_played,
    CASE 
        WHEN COUNT(minutes_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) = 90
        THEN AVG(minutes_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )
    END AS moving_avg_3_months_minutes,
    hours_played,
    CASE 
        WHEN COUNT(hours_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) = 90
        THEN AVG(hours_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )
    END AS moving_avg_3_months_hours,
    days_played,
    CASE 
        WHEN COUNT(days_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) = 90
        THEN AVG(days_played) OVER (
            ORDER BY ts
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )
    END AS moving_avg_3_months_days
FROM daily_converted
ORDER BY ts



-- 8) 4 — Tempo total 2025 x 2024 por mês e média total (qtd e variação % a.a.)
WITH daily_minutes AS (
    SELECT
        DATE(ts) AS ts,
        SUM(minutes_played) AS minutes_played
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY DATE(ts)
),
monthly_sum AS (
    SELECT
        EXTRACT(YEAR FROM ts) AS year,
        EXTRACT(MONTH FROM ts) AS month,
        SUM(minutes_played) AS minutes_played
    FROM daily_minutes
    GROUP BY EXTRACT(YEAR FROM ts), EXTRACT(MONTH FROM ts)
),
pivoted AS (
    SELECT
        m2025.month,
        m2025.minutes_played AS minutes_played_2025,
        m2024.minutes_played AS minutes_played_2024,
        CASE 
            WHEN m2024.minutes_played IS NOT NULL AND m2024.minutes_played <> 0
            THEN ((m2025.minutes_played - m2024.minutes_played) / m2024.minutes_played) * 100
        END AS variation
    FROM monthly_sum m2025
    LEFT JOIN monthly_sum m2024
        ON m2025.month = m2024.month
       AND m2024.year = 2024
    WHERE m2025.year = 2025
),
averages AS (
    SELECT
        (SELECT AVG(minutes_played) FROM monthly_sum WHERE year = 2025) AS avg_2025,
        (SELECT AVG(minutes_played) FROM monthly_sum WHERE year = 2024) AS avg_2024
)
SELECT 
    p.month,
    TO_CHAR(TO_DATE(p.month::text, 'MM'), 'Mon') AS month_label,
    p.minutes_played_2025,
    p.minutes_played_2024,
    p.variation,
    a.avg_2025,
    a.avg_2024
FROM pivoted p
CROSS JOIN averages a
ORDER BY p.month


-- 9) Top 10 Artistas, Álbuns e Músicas - com possibilidade de filtro de ano
-- Parâmetro: substitua 2025 pelo ano que quiser, ou remova o filtro para pegar todos os anos
WITH base AS (
    SELECT
        ts,
        master_metadata_album_artist_name AS artist,
        master_metadata_album_album_name  AS album,
        master_metadata_track_name        AS track,
        minutes_played
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_album_artist_name IS NOT NULL
      -- Descomente a linha abaixo se quiser filtrar por ano específico
      -- AND EXTRACT(YEAR FROM ts) = 2025
),
artist_sum AS (
    SELECT
        artist,
        SUM(minutes_played) AS total_minutes
    FROM base
    GROUP BY artist
    ORDER BY total_minutes DESC
    LIMIT 10
),
album_sum AS (
    SELECT
        album || ' (' || artist || ')' AS label,
        SUM(minutes_played) AS total_minutes
    FROM base
    GROUP BY album, artist
    ORDER BY total_minutes DESC
    LIMIT 10
),
track_sum AS (
    SELECT
        track || ' (' || artist || ')' AS label,
        SUM(minutes_played) AS total_minutes
    FROM base
    GROUP BY track, artist
    ORDER BY total_minutes DESC
    LIMIT 10
)
-- Resultado consolidado
SELECT 'Artist' AS category, artist AS label, total_minutes
FROM artist_sum
UNION ALL
SELECT 'Album', label, total_minutes
FROM album_sum
UNION ALL
SELECT 'Track', label, total_minutes
FROM track_sum;



-- Extra - TOP 10 ARTISTAS POR ANO - MATRIZ
WITH base AS (
    SELECT
        EXTRACT(YEAR FROM ts)::int AS ano,
        master_metadata_album_artist_name AS artist,
        SUM(minutes_played) AS total_minutes
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_album_artist_name IS NOT NULL
    GROUP BY ano, artist
),
ranked AS (
    SELECT
        ano,
        artist,
        total_minutes,
        ROW_NUMBER() OVER (PARTITION BY ano ORDER BY total_minutes DESC) AS rn
    FROM base
)
SELECT
    ano,
    artist,
    total_minutes
FROM ranked
WHERE rn <= 10
ORDER BY ano DESC, total_minutes desc



-- 10) GRAFICOS DE DISPERSAO Top 20 músicas mais escutadas — Geral, 2025 e último mês fechado Distribuição de todas as músicas escutadas em 2025
-- View consolidada para Top 20 músicas e distribuição 2025
WITH base AS (
    SELECT
        DATE(ts) AS data,
        UPPER(master_metadata_track_name) AS track,
        UPPER(master_metadata_album_artist_name) AS artist,
        minutes_played
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_track_name IS NOT NULL
      AND master_metadata_album_artist_name IS NOT NULL
),
total AS (
    -- Top All Time
    SELECT *
    FROM (
        SELECT
            track,
            artist,
            ROUND(SUM(minutes_played)::numeric,2) AS total_minutes_played,
            COUNT(*) AS times_played,
            'all_time' AS dataset,
            ROW_NUMBER() OVER (ORDER BY SUM(minutes_played) DESC) AS row_num
        FROM base
        GROUP BY track, artist
    ) t
    -- WHERE row_num <= 20
),
a AS (
    -- Top 20 All Time
    SELECT *
    FROM (
        SELECT
            track,
            artist,
            ROUND(SUM(minutes_played)::numeric,2) AS total_minutes_played,
            COUNT(*) AS times_played,
            'top20_all_time' AS dataset,
            ROW_NUMBER() OVER (ORDER BY SUM(minutes_played) DESC) AS row_num
        FROM base
        GROUP BY track, artist
    ) t
    WHERE row_num <= 20
),
b AS (
    -- Top 20 2025
    SELECT *
    FROM (
        SELECT
            track,
            artist,
            ROUND(SUM(minutes_played)::numeric,2) AS total_minutes_played,
            COUNT(*) AS times_played,
            'all_2025' AS dataset,
            ROW_NUMBER() OVER (ORDER BY SUM(minutes_played) DESC) AS row_num
        FROM base
        WHERE EXTRACT(YEAR FROM data) = 2025
        GROUP BY track, artist
    ) t
    WHERE row_num <= 20
),
c AS (
    -- Top 20 Last Closed Month
    SELECT *
    FROM (
        SELECT
            track,
            artist,
            ROUND(SUM(minutes_played)::numeric,2) AS total_minutes_played,
            COUNT(*) AS times_played,
            'last_month' AS dataset,
            ROW_NUMBER() OVER (ORDER BY SUM(minutes_played) DESC) AS row_num
        FROM base
        WHERE DATE_TRUNC('month', data) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        GROUP BY track, artist
    ) t
    WHERE row_num <= 20
)
SELECT * FROM total
union ALL
SELECT * FROM a
UNION ALL
SELECT * FROM b
UNION ALL
SELECT * FROM c
ORDER BY dataset, total_minutes_played DESC



--  METODOLOGIA WRAPPED TOP 100 - Ponderando qtd e tempo e criando um índice 0-1
WITH base AS (
    SELECT
        UPPER(master_metadata_track_name) AS track,
        UPPER(master_metadata_album_artist_name) AS artist,
        SUM(minutes_played) AS total_minutes_played,
        COUNT(*) AS play_count
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_track_name IS NOT NULL
      AND master_metadata_album_artist_name IS NOT NULL
    GROUP BY track, artist
),
stats AS (
    SELECT
        track,
        artist,
        total_minutes_played,
        play_count,
        -- Normalização tempo
        (total_minutes_played::numeric - MIN(total_minutes_played::numeric) OVER ()) /
        NULLIF(MAX(total_minutes_played::numeric) OVER () - MIN(total_minutes_played::numeric) OVER (),0) AS indice_tempo,
        -- Normalização quantidade (numeric para evitar zero)
        (play_count::numeric - MIN(play_count::numeric) OVER ()) /
        NULLIF(MAX(play_count::numeric) OVER () - MIN(play_count::numeric) OVER (),0) AS indice_qtd
    FROM base
),
final AS (
    SELECT
        track,
        artist,
        total_minutes_played,
        play_count,
        indice_tempo,
        indice_qtd,
        indice_tempo * indice_qtd AS indice_final,
        ROW_NUMBER() OVER (ORDER BY total_minutes_played DESC) AS ranking
    FROM stats
)
SELECT
    ranking,
    play_count,
    ROUND(total_minutes_played::numeric,2) AS time_played_minutes,
    track,
    artist,
    ROUND(indice_tempo,4) AS indice_tempo,
    ROUND(indice_qtd,4) AS indice_qtd,
    ROUND(indice_final,4) AS indice_final
FROM final
ORDER BY indice_final DESC
LIMIT 100;
WITH base AS (
    SELECT
        UPPER(master_metadata_track_name) AS track,
        UPPER(master_metadata_album_artist_name) AS artist,
        SUM(minutes_played) AS total_minutes_played,
        COUNT(*) AS play_count
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_track_name IS NOT NULL
      AND master_metadata_album_artist_name IS NOT NULL
    GROUP BY track, artist
),
stats AS (
    SELECT
        track,
        artist,
        total_minutes_played,
        play_count,
        (total_minutes_played - MIN(total_minutes_played) OVER ()) / 
        NULLIF(MAX(total_minutes_played) OVER () - MIN(total_minutes_played) OVER (),0) AS indice_tempo,
       (CAST(play_count AS numeric) - MIN(CAST(play_count AS numeric)) OVER ()) /
NULLIF(MAX(CAST(play_count AS numeric)) OVER () - MIN(CAST(play_count AS numeric)) OVER (),0) AS indice_qtd
    FROM base
),
final AS (
    SELECT
        track,
        artist,
        total_minutes_played,
        play_count,
        indice_tempo::numeric,
        indice_qtd::numeric,
        indice_tempo::numeric * indice_qtd::numeric AS indice_final,
        ROW_NUMBER() OVER (ORDER BY total_minutes_played DESC) AS ranking
    FROM stats
)
SELECT
    ranking,
    play_count,
    ROUND(total_minutes_played::numeric,2) AS time_played_minutes,
    track,
    artist,
    ROUND(indice_tempo::numeric,2) AS indice_tempo,
    ROUND(indice_qtd::numeric,2) AS indice_qtd,
    ROUND(indice_final::numeric,2) AS indice_final
FROM final
ORDER BY indice_final DESC
LIMIT 100;




-- 12 — Sessões de Escuta por horário de início e dia da semana

-- 12.1) metodologia para calcular sessão
-- uma sessão é quando o tempo tocado desde a última música é > 1800s (30min)
--- Cada faixa tocada tem um timestamp (ts)
--- Calcula-se a diferença de tempo entre faixas consecutivas (ts_diff_seconds) usando LAG(ts)
WITH ordered AS (
    SELECT
        *,
        EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (PARTITION BY pessoa ORDER BY ts))) AS ts_diff_seconds
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
),
sessions AS (
    SELECT
        *,
        CASE 
            WHEN ts_diff_seconds IS NULL OR ts_diff_seconds > 1800 THEN 1
            ELSE 0
        END AS new_session_flag
    FROM ordered
),
session_ids AS (
    SELECT
        *,
        SUM(new_session_flag) OVER (ORDER BY ts) AS session_id
    FROM sessions
),
session_metrics AS (
    SELECT
        session_id,
        MIN(ts) AS start_time,
        MAX(ts) AS end_time,
        SUM(ts_diff_seconds) AS duration_minutes,
        COUNT(*) AS num_tracks,
        SUM(minutes_played) AS total_play_time,
        EXTRACT(HOUR FROM MIN(ts)) AS start_hour,
        TO_CHAR(MIN(ts), 'Day') AS weekday
    FROM session_ids
    GROUP BY session_id
)
-- 12.2) retorno da tabela
SELECT *
FROM session_metrics
ORDER BY start_time
-- 12.3) sessões por horário do dia
SELECT
    start_hour,
    COUNT(*) AS num_sessions
FROM session_metrics
GROUP BY start_hour
ORDER BY start_hour
-- 12.4) sessões por dia da semana
SELECT
    weekday,
    AVG(duration_minutes) AS avg_duration_minutes
FROM session_metrics
GROUP BY weekday
ORDER BY 
    CASE weekday
        WHEN 'Monday   ' THEN 1
        WHEN 'Tuesday  ' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday ' THEN 4
        WHEN 'Friday   ' THEN 5
        WHEN 'Saturday ' THEN 6
        WHEN 'Sunday   ' THEN 7
    END
-- 12.5) Numero medio de faixas por sessao por dia de semana
SELECT
    weekday,
    AVG(num_tracks) AS avg_tracks_per_session
FROM session_metrics
GROUP BY weekday
ORDER BY 
    CASE weekday
        WHEN 'Monday   ' THEN 1
        WHEN 'Tuesday  ' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday ' THEN 4
        WHEN 'Friday   ' THEN 5
        WHEN 'Saturday ' THEN 6
        WHEN 'Sunday   ' THEN 7
    end 
    
    
    
    
-- 13) Taxa de Skip por Artista e Música
-- 13.1) Skip por música (top 20)
    WITH track_skips AS (
    SELECT
        master_metadata_track_name,
        MIN(master_metadata_album_artist_name) AS artist,
        COUNT(*) AS total_plays,
        SUM(skipped::int) AS total_skips,
        SUM(skipped::int)::numeric / COUNT(*) AS skip_rate
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY master_metadata_track_name
    HAVING COUNT(*) >= 5
)
SELECT *
FROM track_skips
ORDER BY skip_rate DESC
LIMIT 20

-- 13.2) Skip por música (top 20)
WITH artist_skips AS (
    SELECT
        master_metadata_album_artist_name AS artist,
        COUNT(*) AS total_plays,
        SUM(skipped::int) AS total_skips,
        SUM(skipped::int)::numeric / COUNT(*) AS skip_rate
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY master_metadata_album_artist_name
    HAVING COUNT(*) >= 10
)
SELECT *
FROM artist_skips
ORDER BY skip_rate DESC
LIMIT 20




-- 14) Uso de Shuffle e Escuta Offline
-- 14.1) Gráfico de pizza - Shuffle
SELECT
    CASE WHEN shuffle THEN 'Shuffle Ativado' ELSE 'Shuffle Desativado' END AS shuffle_status,
    COUNT(*) AS total_tracks,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM spotify.hist_streaming
WHERE pessoa = 'tomas'
GROUP BY shuffle
ORDER BY shuffle_status
-- 14.2) Gráfico de pizza - Offline
SELECT
    CASE WHEN offline THEN 'Offline' ELSE 'Online' END AS offline_status,
    COUNT(*) AS total_tracks,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM spotify.hist_streaming
WHERE pessoa = 'tomas'
GROUP BY offline
ORDER BY offline_status




-- 15) Análise por Plataforma - % utilizado e ao longo do tempo
-- 15.1) Proporção de faixas tocadas por plataforma
SELECT
    platform_category,
    COUNT(*) AS total_tracks,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM spotify.hist_streaming
WHERE pessoa = 'tomas'
GROUP BY platform_category
ORDER BY percentage desc
-- 15.2) Participação percentual das plataformas ao longo do tempo
WITH monthly_counts AS (
    SELECT
        TO_CHAR(ts, 'YYYY-MM') AS year_month,
        platform_category,
        COUNT(*) AS count_tracks
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY TO_CHAR(ts, 'YYYY-MM'), platform_category
),
monthly_totals AS (
    SELECT
        year_month,
        SUM(count_tracks) AS total_tracks
    FROM monthly_counts
    GROUP BY year_month
)
SELECT
    mc.year_month,
    mc.platform_category,
    mc.count_tracks,
    mt.total_tracks,
    ROUND(100.0 * mc.count_tracks / mt.total_tracks, 1) AS percentage
FROM monthly_counts mc
JOIN monthly_totals mt
  ON mc.year_month = mt.year_month
ORDER BY mc.year_month, percentage DESC




-- 16) Faixas Escutadas Uma Única Vez vs Repetidas
-- 16.1) Proporção de faixas escutadas mais de uma vez - Total
WITH track_counts AS (
    SELECT
        master_metadata_track_name,
        COUNT(*) AS play_count
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY master_metadata_track_name
)
SELECT
    CASE 
        WHEN play_count >= 2 THEN 'Repetida'
        ELSE 'Única'
    END AS categoria,
    COUNT(*) AS num_tracks,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percent
FROM track_counts
GROUP BY categoria
ORDER BY categoria

-- 16.2) Proporção ao longo do tempo
WITH monthly_track_counts AS (
    SELECT
        TO_CHAR(ts, 'YYYY-MM') AS year_month,
        master_metadata_track_name,
        COUNT(*) AS play_count
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY TO_CHAR(ts, 'YYYY-MM'), master_metadata_track_name
),
categorized AS (
    SELECT
        year_month,
        CASE 
            WHEN play_count >= 2 THEN 'Repetida'
            ELSE 'Única'
        END AS categoria,
        COUNT(*) AS num_tracks
    FROM monthly_track_counts
    GROUP BY year_month, CASE WHEN play_count >= 2 THEN 'Repetida' ELSE 'Única' END
)
SELECT
    year_month,
    categoria,
    num_tracks,
    ROUND(100.0 * num_tracks / SUM(num_tracks) OVER (PARTITION BY year_month), 1) AS percent
FROM categorized
ORDER BY year_month, categoria




-- 17) Identificação de Músicas e Artistas Novos
WITH base AS (
    SELECT
        DATE_TRUNC('month', ts)::date AS year_month,
        UPPER(master_metadata_track_name) AS track,
        UPPER(master_metadata_album_artist_name) AS artist
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND master_metadata_track_name IS NOT NULL
      AND master_metadata_album_artist_name IS NOT NULL
),
-- Primeiro mês que cada música apareceu
first_track_month AS (
    SELECT
        track,
        MIN(year_month) AS first_month_track
    FROM base
    GROUP BY track
),
-- Primeiro mês que cada artista apareceu
first_artist_month AS (
    SELECT
        artist,
        MIN(year_month) AS first_month_artist
    FROM base
    GROUP BY artist
),
-- Identificar músicas novas em seu mês de estreia
new_tracks AS (
    SELECT
        b.year_month,
        b.track,
        b.artist,
        ft.first_month_track,
        fa.first_month_artist,
        CASE 
            WHEN ft.first_month_track = fa.first_month_artist THEN 1
            ELSE 0
        END AS artista_novo
    FROM base b
    JOIN first_track_month ft ON b.track = ft.track
    JOIN first_artist_month fa ON b.artist = fa.artist
    WHERE b.year_month = ft.first_month_track
),
-- Contagem de músicas novas por mês
new_tracks_per_month AS (
    SELECT
        year_month,
        COUNT(DISTINCT track) AS musicas_novas
    FROM new_tracks
    GROUP BY year_month
),
-- Contagem de artistas novos por mês
new_artists_per_month AS (
    SELECT
        fa.first_month_artist AS year_month,
        COUNT(DISTINCT fa.artist) AS artistas_novos
    FROM first_artist_month fa
    GROUP BY fa.first_month_artist
),
-- Proporção de músicas novas que são de artistas novos
proporcao_novas_de_artistas_novos AS (
    SELECT
        year_month,
        AVG(artista_novo::numeric) AS proporcao
    FROM new_tracks
    GROUP BY year_month
)
-- Resultado final juntando tudo
SELECT
    t.year_month,
    t.musicas_novas,
    a.artistas_novos,
    ROUND(p.proporcao * 100, 2) AS proporcao_musicas_de_artistas_novos_percent
FROM new_tracks_per_month t
LEFT JOIN new_artists_per_month a ON t.year_month = a.year_month
LEFT JOIN proporcao_novas_de_artistas_novos p ON t.year_month = p.year_month
ORDER BY t.year_month;




-- 18) Diversidade Musical
WITH plays AS (
    SELECT
        DATE_TRUNC('month', ts)::date AS year_month,
        master_metadata_album_artist_name AS artista,
        COUNT(*) AS play_count
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
    GROUP BY DATE_TRUNC('month', ts), master_metadata_album_artist_name
),
totais AS (
    SELECT
        year_month,
        SUM(play_count) AS total_mes
    FROM plays
    GROUP BY year_month
),
proporcoes AS (
    SELECT
        p.year_month,
        p.artista,
        p.play_count,
        t.total_mes,
        (p.play_count::numeric / t.total_mes) AS proporcao
    FROM plays p
    JOIN totais t ON p.year_month = t.year_month
),
shannon AS (
    SELECT
        year_month,
        COUNT(DISTINCT artista) AS artistas_unicos,
        round(-SUM(proporcao * LOG(2, proporcao)),2) AS shannon_index
    FROM proporcoes
    GROUP BY year_month
)
SELECT *
FROM shannon
ORDER BY year_month



-- 19) Engajamento com Podcasts: Tempo Total e Proporção Mensal
WITH base AS (
    SELECT
        pessoa,
        tipo_conteudo,
        minutes_played,
        TO_CHAR(ts, 'YYYY-MM') AS year_month
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
),
-- Tempo total de escuta de podcasts por mês
podcast_por_mes AS (
    SELECT
        year_month,
        SUM(minutes_played) AS minutes_played_podcast
    FROM base
    WHERE tipo_conteudo = 'Podcast'
    GROUP BY year_month
    HAVING SUM(minutes_played) > 0
),
-- Tempo total de escuta de todos os conteúdos por mês
total_geral_por_mes AS (
    SELECT
        year_month,
        SUM(minutes_played) AS minutes_played_geral
    FROM base
    GROUP BY year_month
)
-- Merge e cálculo de proporção
SELECT
    t.year_month,
    p.minutes_played_podcast,
    t.minutes_played_geral,
    ROUND(p.minutes_played_podcast::numeric / t.minutes_played_geral::numeric, 4) AS proporcao
FROM total_geral_por_mes t
JOIN podcast_por_mes p
    ON t.year_month = p.year_month
ORDER BY t.year_month;



-- 20) Mudança de Preferências Musicais e Concentração de favoritos ao Longo do Tempo
WITH filtered AS (
    SELECT *
    FROM spotify.hist_streaming
    WHERE pessoa = 'tomas'
      AND EXTRACT(YEAR FROM ts) IN (2024, 2025)
),
-- Top 20 artistas mais escutados no período
artist_total AS (
    SELECT 
        master_metadata_album_artist_name,
        SUM(minutes_played) AS total_minutes
    FROM filtered
    GROUP BY master_metadata_album_artist_name
    ORDER BY total_minutes DESC
    LIMIT 20
),
-- Apenas plays dos top 20 artistas
top_artists AS (
    SELECT f.*
    FROM filtered f
    JOIN artist_total t
      ON f.master_metadata_album_artist_name = t.master_metadata_album_artist_name
),
-- Total mensal por artista
monthly_top AS (
    SELECT
        TO_CHAR(ts, 'YYYY-MM') AS year_month,
        master_metadata_album_artist_name,
        SUM(minutes_played) AS total_minutes
    FROM top_artists
    GROUP BY TO_CHAR(ts, 'YYYY-MM'), master_metadata_album_artist_name
),
-- Total mensal geral
total_mes AS (
    SELECT
        TO_CHAR(ts, 'YYYY-MM') AS year_month,
        SUM(minutes_played) AS minutes_played
    FROM filtered
    GROUP BY TO_CHAR(ts, 'YYYY-MM')
),
-- Total mensal dos favoritos (top 20)
total_favoritos_mes AS (
    SELECT
        year_month,
        SUM(total_minutes) AS total_minutes
    FROM monthly_top
    GROUP BY year_month
)
-- Proporção dos favoritos sobre o total
SELECT
    t.year_month,
    t.total_minutes AS total_favoritos,
    g.minutes_played AS total_geral,
    ROUND(t.total_minutes::numeric / g.minutes_played::numeric, 4) AS proporcao
FROM total_favoritos_mes t
JOIN total_mes g 
  ON t.year_month = g.year_month
ORDER BY t.year_month




-- 21) Retenção e Frequência de Uso
-- CTE 1: Obter dias únicos de escuta por pessoa (Um “dia único” nesse contexto significa um dia distinto em que a pessoa escutou pelo menos uma faixa, independentemente de quantas reproduções houve nesse dia)
-- Análise 22 - Retenção e Frequência de Uso (SQL) - corrigido
WITH dias_unicos AS (
    -- Obtemos apenas a data sem hora e removemos duplicatas por pessoa
    SELECT DISTINCT
        pessoa,
        CAST(ts AS DATE) AS dia
    FROM spotify.hist_streaming
    WHERE pessoa IS NOT NULL
),
gaps_e_streaks AS (
    SELECT
        pessoa,
        dia,
        LAG(dia) OVER (PARTITION BY pessoa ORDER BY dia) AS dia_anterior,
        (dia - LAG(dia) OVER (PARTITION BY pessoa ORDER BY dia))::INT AS diff_dias
    FROM dias_unicos
),
streak_groups AS (
    SELECT
        pessoa,
        dia,
        diff_dias,
        -- Incrementa o grupo sempre que houver gap (diff_dias > 1)
        SUM(CASE WHEN diff_dias > 1 OR diff_dias IS NULL THEN 1 ELSE 0 END) 
            OVER (PARTITION BY pessoa ORDER BY dia ROWS UNBOUNDED PRECEDING) AS streak_group
    FROM gaps_e_streaks
),
streaks AS (
    SELECT
        pessoa,
        dia,
        diff_dias,
        streak_group,
        -- Número de dias consecutivos dentro do grupo
        ROW_NUMBER() OVER (PARTITION BY pessoa, streak_group ORDER BY dia) AS streak_count
    FROM streak_groups
),
metrics AS (
    SELECT
        pessoa,
        COUNT(*) AS total_dias_unicos,                                -- Total de dias únicos
        COUNT(CASE WHEN diff_dias > 1 THEN 1 END) AS numero_gaps,     -- Número de gaps > 1 dia
        AVG(CASE WHEN diff_dias > 1 THEN diff_dias END) AS media_gaps,-- Média dos gaps
        MAX(streak_count) AS maior_streak_consecutivo                 -- Maior streak
    FROM streaks
    GROUP BY pessoa
)
SELECT
    pessoa,
    total_dias_unicos,
    numero_gaps,
    ROUND(COALESCE(media_gaps,0),2) AS media_dias_por_gap,
    COALESCE(maior_streak_consecutivo,1) AS maior_streak_consecutivo
FROM metrics
ORDER BY pessoa

