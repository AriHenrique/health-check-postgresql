-- =====================================================
-- PROCEDURES PARA ANALISE DE SAUDE DO POSTGRESQL
-- =====================================================
DROP FUNCTION IF EXISTS fn_check_active_queries() CASCADE;
DROP FUNCTION IF EXISTS fn_top_cpu_queries(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_slow_queries(NUMERIC, INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_cache_hit_ratio() CASCADE;
DROP FUNCTION IF EXISTS fn_cache_hit_by_table(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_connection_stats() CASCADE;
DROP FUNCTION IF EXISTS fn_connections_by_database() CASCADE;
DROP FUNCTION IF EXISTS fn_database_size() CASCADE;
DROP FUNCTION IF EXISTS fn_largest_tables(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_unused_indexes() CASCADE;
DROP FUNCTION IF EXISTS fn_rarely_used_indexes(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_table_bloat(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_vacuum_stats(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_important_settings() CASCADE;
DROP FUNCTION IF EXISTS fn_active_locks() CASCADE;
DROP FUNCTION IF EXISTS fn_health_report_summary() CASCADE;

DROP PROCEDURE IF EXISTS sp_check_active_queries() CASCADE;
DROP PROCEDURE IF EXISTS sp_check_server_info() CASCADE;

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Limpeza concluida com sucesso!';
    RAISE NOTICE '========================================';
END $$;


-- =====================================================
-- PROCEDURES PARA ANALISE DE SAUDE DO POSTGRESQL
-- =====================================================
DROP FUNCTION IF EXISTS fn_check_active_queries() CASCADE;
DROP FUNCTION IF EXISTS fn_top_cpu_queries(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_slow_queries(NUMERIC, INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_cache_hit_ratio() CASCADE;
DROP FUNCTION IF EXISTS fn_cache_hit_by_table(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_connection_stats() CASCADE;
DROP FUNCTION IF EXISTS fn_connections_by_database() CASCADE;
DROP FUNCTION IF EXISTS fn_database_size() CASCADE;
DROP FUNCTION IF EXISTS fn_largest_tables(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_unused_indexes() CASCADE;
DROP FUNCTION IF EXISTS fn_rarely_used_indexes(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_table_bloat(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_vacuum_stats(INTEGER) CASCADE;
DROP FUNCTION IF EXISTS fn_important_settings() CASCADE;
DROP FUNCTION IF EXISTS fn_active_locks() CASCADE;
DROP FUNCTION IF EXISTS fn_health_report_summary() CASCADE;

DROP PROCEDURE IF EXISTS sp_check_active_queries() CASCADE;
DROP PROCEDURE IF EXISTS sp_check_server_info() CASCADE;

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Limpeza concluida com sucesso';
    RAISE NOTICE '========================================';
END $$;


-- =====================================================
-- 1. PROCEDURE: Informacoes Basicas do Servidor
-- DESCRICAO:
--   Importancia: fornece um "snapshot" basico do servidor: versao,
--   horario de startup e tempo de uptime. Ajuda a confirmar se o
--   servidor foi recentemente reiniciado (ex.: apos manutencoes).
--   Proximos passos:
--     - Se o uptime for muito alto, avaliar janela de manutencao
--       para aplicar updates/patches.
--     - Se o uptime for muito baixo (reinicios frequentes),
--       investigar logs do sistema/servico (possiveis falhas).
-- =====================================================
CREATE OR REPLACE PROCEDURE sp_check_server_info()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== INFORMACOES DO SERVIDOR ===';
    RAISE NOTICE 'Versao: %', version();

    RAISE NOTICE 'Startup: %', pg_postmaster_start_time();
    RAISE NOTICE 'Uptime: %', now() - pg_postmaster_start_time();

    RAISE NOTICE '';
    RAISE NOTICE 'Execute SELECT para ver detalhes:';
    RAISE NOTICE 'SELECT version() as versao;';
    RAISE NOTICE 'SELECT pg_postmaster_start_time() as startup, now() - pg_postmaster_start_time() as uptime;';
END;
$$;

-- =====================================================
-- 2. FUNCTION: Analise de CPU e Queries Ativas
-- DESCRICAO:
--   Importancia: lista as queries atualmente ativas, com duracao e
--   contexto (usuario, app, IP). Ajuda a identificar gargalos em tempo
--   real (queries travadas, long-running, picos de uso).
--   Proximos passos:
--     - Investigar queries com maior "query_duration".
--     - Checar se ha padrao por usuario/app (pool, servico especifico).
--     - Avaliar cancelamento de queries anormais ou revisao de indices.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_check_active_queries()
RETURNS TABLE (
    process_id INTEGER,
    username NAME,
    app_name TEXT,
    client_address TEXT,
    connection_state TEXT,
    query_start_time TIMESTAMP WITH TIME ZONE,
    query_duration INTERVAL,
    query_preview TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM pg_stat_activity
    WHERE pg_stat_activity.state != 'idle'
      AND pg_stat_activity.pid != pg_backend_pid();

    RAISE NOTICE '=== QUERIES ATIVAS NO MOMENTO ===';
    RAISE NOTICE 'Total de queries ativas: %', v_count;

    RETURN QUERY
    SELECT
        pa.pid,
        pa.usename,
        pa.application_name,
        pa.client_addr::text,
        pa.state,
        pa.query_start,
        now() - pa.query_start,
        LEFT(pa.query, 100)
    FROM pg_stat_activity pa
    WHERE pa.state != 'idle'
      AND pa.pid != pg_backend_pid()
    ORDER BY (now() - pa.query_start) DESC;
END;
$$;

-- =====================================================
-- 3. FUNCTION: Top Queries por Consumo de CPU
-- DESCRICAO:
--   Importancia: usa pg_stat_statements para mostrar quais queries
--   mais consomem CPU ao longo do tempo (total_exec_time).
--   Proximos passos:
--     - Focar nas queries do topo para otimizacao (indices, reescrita).
--     - Avaliar se ha queries de BI/relatorios muito pesadas em horario
--       de pico.
--     - Priorizar otimizacao das queries com alto tempo total e muitas chamadas.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_top_cpu_queries(p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    query_text TEXT,
    execucoes BIGINT,
    tempo_total_segundos NUMERIC,
    tempo_medio_ms NUMERIC,
    tempo_max_ms NUMERIC,
    percentual_cpu NUMERIC,
    linhas BIGINT,
    cache_hit_ratio NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN
        RAISE NOTICE 'AVISO: Extensao pg_stat_statements nao esta instalada!';
        RAISE NOTICE 'Execute: CREATE EXTENSION pg_stat_statements;';
        RETURN;
    END IF;

    RAISE NOTICE '=== TOP % QUERIES POR CONSUMO DE CPU ===', p_limit;

    RETURN QUERY
    SELECT
        LEFT(pss.query, 100)::TEXT,
        pss.calls,
        ROUND((pss.total_exec_time / 1000)::NUMERIC, 2),
        ROUND(pss.mean_exec_time::NUMERIC, 2),
        ROUND(pss.max_exec_time::NUMERIC, 2),
        ROUND((pss.total_exec_time / NULLIF(SUM(pss.total_exec_time) OVER (), 0) * 100)::NUMERIC, 2),
        pss.rows,
        ROUND((100.0 * pss.shared_blks_hit / NULLIF(pss.shared_blks_hit + pss.shared_blks_read, 0))::NUMERIC, 2)
    FROM pg_stat_statements pss
    WHERE pss.query NOT LIKE '%pg_stat_statements%'
    ORDER BY pss.total_exec_time DESC
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 4. FUNCTION: Queries Lentas
-- DESCRICAO:
--   Importancia: identifica queries com maior tempo medio de execucao
--   (mean_exec_time), classificando em CRITICO/ALERTA/ATENCAO.
--   Proximos passos:
--     - Rever plano de execucao (EXPLAIN/EXPLAIN ANALYZE) das queries
--       marcadas como CRITICO/ALERTA.
--     - Confirmar necessidade de cada query (relatorio, API, batch).
--     - Ajustar indices, filtros ou particionamento, se aplicavel.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_slow_queries(p_min_avg_time_ms NUMERIC DEFAULT 1000, p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    query_text TEXT,
    execucoes BIGINT,
    tempo_medio_ms NUMERIC,
    tempo_max_ms NUMERIC,
    desvio_padrao_ms NUMERIC,
    status TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== QUERIES LENTAS (> %ms) ===', p_min_avg_time_ms;

    RETURN QUERY
    SELECT
        LEFT(query, 100)::TEXT,
        calls,
        ROUND(mean_exec_time::NUMERIC, 2),
        ROUND(max_exec_time::NUMERIC, 2),
        ROUND(stddev_exec_time::NUMERIC, 2),
        CASE
            WHEN mean_exec_time > 1000 THEN 'CRITICO'
            WHEN mean_exec_time > 500 THEN 'ALERTA'
            WHEN mean_exec_time > 100 THEN 'ATENCAO'
            ELSE 'OK'
        END::TEXT
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat_statements%'
      AND mean_exec_time > p_min_avg_time_ms
      AND calls > 10
    ORDER BY mean_exec_time DESC
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 5. FUNCTION: Cache Hit Ratio Global
-- DESCRICAO:
--   Importancia: mede o percentual de leituras atendidas em cache
--   (heap_blks_hit) em vez de disco. Indicador direto de eficiencia de
--   memoria (shared_buffers + cache do SO).
--   Proximos passos:
--     - Se < 95%: avaliar aumento de shared_buffers e revisao de
--       queries/indices.
--     - Se muito baixo, pode indicar excesso de full scans, falta de
--       indices ou working set maior que a memoria disponivel.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_cache_hit_ratio()
RETURNS TABLE (
    metrica TEXT,
    heap_read NUMERIC,
    heap_hit NUMERIC,
    cache_hit_ratio NUMERIC,
    avaliacao TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_ratio NUMERIC;
BEGIN
    RAISE NOTICE '=== CACHE HIT RATIO ===';

    SELECT COALESCE(
        sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100,
        0
    ) INTO v_ratio
    FROM pg_statio_user_tables;

    RAISE NOTICE 'Cache Hit Ratio: %%%', ROUND(v_ratio, 2);

    RETURN QUERY
    SELECT
        'Cache Hit Ratio Global'::TEXT,
        sum(heap_blks_read)::NUMERIC,
        sum(heap_blks_hit)::NUMERIC,
        ROUND((sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100)::NUMERIC, 2),
        CASE
            WHEN ROUND((sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100)::NUMERIC, 2) > 99 THEN 'Excelente'
            WHEN ROUND((sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100)::NUMERIC, 2) > 95 THEN 'Bom'
            WHEN ROUND((sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100)::NUMERIC, 2) > 90 THEN 'Regular'
            ELSE 'Critico - Aumentar shared_buffers'
        END::TEXT
    FROM pg_statio_user_tables;
END;
$$;

-- =====================================================
-- 6. FUNCTION: Cache Hit Ratio por Tabela
-- DESCRICAO:
--   Importancia: detalha o cache hit ratio por tabela, ajudando a
--   identificar objetos que mais pressionam I/O.
--   Proximos passos:
--     - Focar nas tabelas com cache_hit_ratio < 95%.
--     - Verificar se ha full scans recorrentes, queries mal filtradas
--       ou ausencia de indices adequados.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_cache_hit_by_table(p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    leituras_disco BIGINT,
    leituras_cache BIGINT,
    cache_hit_ratio NUMERIC,
    status TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== CACHE HIT RATIO POR TABELA ===';

    RETURN QUERY
    SELECT
        st.schemaname,
        st.relname,
        st.heap_blks_read,
        st.heap_blks_hit,
        ROUND((st.heap_blks_hit::NUMERIC / NULLIF(st.heap_blks_hit + st.heap_blks_read, 0) * 100), 2),
        CASE
            WHEN ROUND((st.heap_blks_hit::NUMERIC / NULLIF(st.heap_blks_hit + st.heap_blks_read, 0) * 100), 2) < 95 THEN 'Revisar'
            ELSE 'OK'
        END::TEXT
    FROM pg_statio_user_tables st
    WHERE st.heap_blks_read + st.heap_blks_hit > 0
    ORDER BY st.heap_blks_read DESC
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 7. FUNCTION: Estatisticas de Conexoes (Visao Global)
-- DESCRICAO:
--   Importancia: mostra uso de conexoes em relacao a max_connections,
--   alem de distribuicao entre active/idle/idle in transaction.
--   Proximos passos:
--     - Se percentual de uso > 80%, considerar pool de conexoes ou
--       ajuste de max_connections.
--     - Muitos "idle in transaction" indicam aplicacoes segurando
--       transacoes abertas sem necessidade (risco de bloqueios).
-- =====================================================
CREATE OR REPLACE FUNCTION fn_connection_stats()
RETURNS TABLE (
    metrica TEXT,
    valor BIGINT,
    percentual NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_conn INTEGER;
    v_total_conn BIGINT;
BEGIN
    RAISE NOTICE '=== ESTATISTICAS DE CONEXOES ===';

    SELECT setting::INTEGER INTO v_max_conn FROM pg_settings WHERE name = 'max_connections';
    SELECT COUNT(*) INTO v_total_conn FROM pg_stat_activity;

    RAISE NOTICE 'Conexoes: %/%', v_total_conn, v_max_conn;

    RETURN QUERY
    SELECT
        'Total de Conexoes'::TEXT,
        COUNT(*)::BIGINT,
        ROUND((COUNT(*)::NUMERIC / v_max_conn * 100), 2)
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()

    UNION ALL

    SELECT
        'Ativas'::TEXT,
        COUNT(*) FILTER (WHERE state = 'active')::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE state = 'active')::NUMERIC / NULLIF(COUNT(*), 0) * 100), 2)
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()

    UNION ALL

    SELECT
        'Idle'::TEXT,
        COUNT(*) FILTER (WHERE state = 'idle')::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE state = 'idle')::NUMERIC / NULLIF(COUNT(*), 0) * 100), 2)
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()

    UNION ALL

    SELECT
        'Idle in Transaction'::TEXT,
        COUNT(*) FILTER (WHERE state = 'idle in transaction')::BIGINT,
        ROUND((COUNT(*) FILTER (WHERE state = 'idle in transaction')::NUMERIC / NULLIF(COUNT(*), 0) * 100), 2)
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()

    UNION ALL

    SELECT
        'Disponiveis'::TEXT,
        (v_max_conn - COUNT(*))::BIGINT,
        ROUND(((v_max_conn - COUNT(*))::NUMERIC / v_max_conn * 100), 2)
    FROM pg_stat_activity;
END;
$$;

-- =====================================================
-- 8. FUNCTION: Conexoes por Database
-- DESCRICAO:
--   Importancia: mostra como as conexoes estao distribuidas entre os
--   bancos da instancia, e qual DB tem maior duracao de queries.
--   Proximos passos:
--     - Identificar databases "quentes" com muitas conexoes ativas.
--     - Cruzar com fn_check_active_queries para entender workload.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_connections_by_database()
RETURNS TABLE (
    database_name NAME,
    total_conexoes BIGINT,
    ativas BIGINT,
    idle BIGINT,
    duracao_maxima INTERVAL
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== CONEXOES POR DATABASE ===';

    RETURN QUERY
    SELECT
        datname,
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE state = 'active')::BIGINT,
        COUNT(*) FILTER (WHERE state = 'idle')::BIGINT,
        MAX(now() - query_start)
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
    GROUP BY datname
    ORDER BY COUNT(*) DESC;
END;
$$;

-- =====================================================
-- 9. FUNCTION: Tamanho do Banco Atual
-- DESCRICAO:
--   Importancia: mostra o tamanho do database corrente, em formato
--   legivel, para acompanhar crescimento.
--   Proximos passos:
--     - Se crescimento acelerado, combinar com fn_largest_tables e
--       fn_table_bloat para identificar fontes de crescimento.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_database_size()
RETURNS TABLE (
    database_name NAME,
    tamanho TEXT,
    tamanho_bytes BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== TAMANHO DO BANCO DE DADOS ===';

    RETURN QUERY
    SELECT
        datname,
        pg_size_pretty(pg_database_size(datname))::TEXT,
        pg_database_size(datname)
    FROM pg_database
    WHERE datname = current_database();
END;
$$;

-- =====================================================
-- 10. FUNCTION: Top Maiores Tabelas
-- DESCRICAO:
--   Importancia: lista as maiores tabelas do banco, segregando tamanho
--   total, da tabela e dos indices.
--   Proximos passos:
--     - Avaliar necessidade de particionamento ou arquivamento.
--     - Ver se o percentual_indices esta muito alto (indices demais).
--     - Cruzar com fn_table_bloat para ver se ha muito lixo (dead tuples).
-- =====================================================
CREATE OR REPLACE FUNCTION fn_largest_tables(p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    tamanho_total TEXT,
    tamanho_tabela TEXT,
    tamanho_indices TEXT,
    percentual_indices NUMERIC,
    tamanho_bytes BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== TOP % MAIORES TABELAS ===', p_limit;

    RETURN QUERY
    SELECT
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))::TEXT,
        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename))::TEXT,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) -
                       pg_relation_size(schemaname||'.'||tablename))::TEXT,
        ROUND((100.0 * (pg_total_relation_size(schemaname||'.'||tablename) -
                        pg_relation_size(schemaname||'.'||tablename)) /
               NULLIF(pg_total_relation_size(schemaname||'.'||tablename), 0))::NUMERIC, 2),
        pg_total_relation_size(schemaname||'.'||tablename)
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 11. FUNCTION: Indices Nao Utilizados
-- DESCRICAO:
--   Importancia: identifica indices que nunca foram usados (idx_scan = 0).
--   Manter indices inutilizados aumenta uso de disco e custo de escrita.
--   Proximos passos:
--     - Revisar com cuidado se o indice realmente nao e necessario.
--     - Validar com devs antes de remover (pode haver uso futuro planejado).
-- =====================================================
CREATE OR REPLACE FUNCTION fn_unused_indexes()
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    index_name NAME,
    vezes_usado BIGINT,
    tamanho TEXT,
    tamanho_bytes BIGINT,
    recomendacao TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== INDICES NAO UTILIZADOS ===';
    RAISE NOTICE 'Estes indices nunca foram usados e podem ser removidos';

    RETURN QUERY
    SELECT
        sui.schemaname,
        sui.relname,
        sui.indexrelname,
        sui.idx_scan,
        pg_size_pretty(pg_relation_size(sui.indexrelid))::TEXT,
        pg_relation_size(sui.indexrelid),
        'REMOVER - Nunca usado'::TEXT
    FROM pg_stat_user_indexes sui
    WHERE sui.idx_scan = 0
      AND sui.indexrelname NOT LIKE '%_pkey'
      AND sui.indexrelname NOT LIKE '%_unique'
      AND sui.schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY pg_relation_size(sui.indexrelid) DESC;
END;
$$;

-- =====================================================
-- 12. FUNCTION: Indices Raramente Usados
-- DESCRICAO:
--   Importancia: mostra indices que sao usados poucas vezes (idx_scan
--   baixo), mas nao zero. Sao bons candidatos para revisao.
--   Proximos passos:
--     - Verificar se o indice foi criado para relatorios esporadicos;
--       se nao fizer sentido, considerar remocao.
--     - Ajustar queries para usar indices existentes em vez de criar novos.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_rarely_used_indexes(p_max_scans INTEGER DEFAULT 50)
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    index_name NAME,
    vezes_usado BIGINT,
    tamanho TEXT,
    recomendacao TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== INDICES RARAMENTE USADOS (< % scans) ===', p_max_scans;

    RETURN QUERY
    SELECT
        sui.schemaname,
        sui.relname,
        sui.indexrelname,
        sui.idx_scan,
        pg_size_pretty(pg_relation_size(sui.indexrelid))::TEXT,
        'AVALIAR REMOCAO'::TEXT
    FROM pg_stat_user_indexes sui
    WHERE sui.idx_scan < p_max_scans
      AND sui.idx_scan > 0
      AND sui.indexrelname NOT LIKE '%_pkey'
      AND sui.schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY sui.idx_scan ASC, pg_relation_size(sui.indexrelid) DESC;
END;
$$;

-- =====================================================
-- 13. FUNCTION: Analise de Bloat (Dead Tuples)
-- DESCRICAO:
--   Importancia: mede quantidade de dead tuples por tabela, estimando
--   percentual de linhas mortas e classificando necessidade de VACUUM.
--   Proximos passos:
--     - Rodar VACUUM (ou VACUUM FULL em casos extremos) nas tabelas com
--       status 'VACUUM URGENTE' ou 'VACUUM NECESSARIO'.
--     - Revisar parametros de autovacuum se sempre ha muito bloat.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_table_bloat(p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    tuplas_vivas BIGINT,
    tuplas_mortas BIGINT,
    percentual_mortas NUMERIC,
    tamanho_total TEXT,
    ultimo_vacuum TIMESTAMP WITH TIME ZONE,
    ultimo_autovacuum TIMESTAMP WITH TIME ZONE,
    status TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== ANALISE DE BLOAT (DEAD TUPLES) ===';

    RETURN QUERY
    SELECT
        st.schemaname,
        st.relname,
        st.n_live_tup,
        st.n_dead_tup,
        ROUND((100.0 * st.n_dead_tup / NULLIF(st.n_live_tup + st.n_dead_tup, 0))::NUMERIC, 2),
        pg_size_pretty(pg_total_relation_size(st.schemaname||'.'||st.relname))::TEXT,
        st.last_vacuum,
        st.last_autovacuum,
        CASE
            WHEN ROUND((100.0 * st.n_dead_tup / NULLIF(st.n_live_tup + st.n_dead_tup, 0))::NUMERIC, 2) > 20 THEN 'VACUUM URGENTE'
            WHEN ROUND((100.0 * st.n_dead_tup / NULLIF(st.n_live_tup + st.n_dead_tup, 0))::NUMERIC, 2) > 10 THEN 'VACUUM NECESSARIO'
            WHEN ROUND((100.0 * st.n_dead_tup / NULLIF(st.n_live_tup + st.n_dead_tup, 0))::NUMERIC, 2) > 5 THEN 'Monitorar'
            ELSE 'OK'
        END::TEXT
    FROM pg_stat_user_tables st
    WHERE st.n_dead_tup > 0
    ORDER BY st.n_dead_tup DESC
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 14. FUNCTION: Estatisticas de VACUUM
-- DESCRICAO:
--   Importancia: mostra historico de VACUUM/AUTOVACUUM por tabela e
--   quantos dias se passaram desde o ultimo vacuum.
--   Proximos passos:
--     - Se "dias_sem_vacuum" estiver muito alto em tabelas grandes,
--       ajustar parametros de autovacuum para agir mais cedo.
--     - Cruzar com fn_table_bloat para priorizar as tabelas mais criticas.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_vacuum_stats(p_limit INTEGER DEFAULT 20)
RETURNS TABLE (
    schema_name NAME,
    table_name NAME,
    ultimo_vacuum TIMESTAMP WITH TIME ZONE,
    ultimo_autovacuum TIMESTAMP WITH TIME ZONE,
    total_vacuums BIGINT,
    total_autovacuums BIGINT,
    total_analyzes BIGINT,
    total_autoanalyzes BIGINT,
    dias_sem_vacuum NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== ESTATISTICAS DE VACUUM ===';

    RETURN QUERY
    SELECT
        st.schemaname,
        st.relname,
        st.last_vacuum,
        st.last_autovacuum,
        st.vacuum_count,
        st.autovacuum_count,
        st.analyze_count,
        st.autoanalyze_count,
        ROUND(
            EXTRACT(
                EPOCH FROM (now() - COALESCE(st.last_autovacuum, st.last_vacuum))
            ) / 86400::NUMERIC,
            1
        )
    FROM pg_stat_user_tables st
    WHERE st.n_live_tup > 1000
    ORDER BY COALESCE(st.last_autovacuum, st.last_vacuum) ASC NULLS FIRST
    LIMIT p_limit;
END;
$$;

-- =====================================================
-- 15. FUNCTION: Configuracoes Importantes
-- DESCRICAO:
--   Importancia: traz um conjunto de parametros chave de performance e
--   capacidade (memoria, conexoes, paralelismo, autovacuum etc).
--   Proximos passos:
--     - Comparar valores atuais com boas praticas do tamanho de workload.
--     - Ajustar gradualmente e medir impacto (ex.: shared_buffers,
--       work_mem, effective_cache_size, autovacuum_*).
-- =====================================================
CREATE OR REPLACE FUNCTION fn_important_settings()
RETURNS TABLE (
    parametro TEXT,
    valor TEXT,
    unidade TEXT,
    contexto TEXT,
    descricao TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== CONFIGURACOES IMPORTANTES ===';

    RETURN QUERY
    SELECT
        s.name::TEXT,
        s.setting::TEXT,
        COALESCE(s.unit, '')::TEXT,
        s.context::TEXT,
        s.short_desc::TEXT
    FROM pg_settings s
    WHERE s.name IN (
        'max_connections',
        'shared_buffers',
        'effective_cache_size',
        'work_mem',
        'maintenance_work_mem',
        'checkpoint_completion_target',
        'wal_buffers',
        'default_statistics_target',
        'random_page_cost',
        'effective_io_concurrency',
        'max_worker_processes',
        'max_parallel_workers_per_gather',
        'max_parallel_workers',
        'autovacuum',
        'autovacuum_max_workers'
    )
    ORDER BY s.name;
END;
$$;

-- =====================================================
-- 16. FUNCTION: Locks Ativos
-- DESCRICAO:
--   Importancia: identifica locks ativos e queries envolvidas,
--   permitindo investigar contecoes e deadlocks em potencial.
--   Proximos passos:
--     - Verificar se ha locks de longa duracao bloqueando operacoes
--       criticas (INSERT/UPDATE/DELETE).
--     - Ajustar aplicacao para reduzir janelas de transacao.
-- =====================================================
CREATE OR REPLACE FUNCTION fn_active_locks()
RETURNS TABLE (
    pid INTEGER,
    usuario NAME,
    tipo_lock TEXT,
    relacao TEXT,
    modo TEXT,
    concedido BOOLEAN,
    query_preview TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=== LOCKS ATIVOS NO MOMENTO ===';

    RETURN QUERY
    SELECT
        pa.pid,
        pa.usename,
        pl.locktype::TEXT,
        COALESCE(pl.relation::regclass::TEXT, 'N/A'),
        pl.mode::TEXT,
        pl.granted,
        LEFT(pa.query, 100)::TEXT
    FROM pg_locks pl
    JOIN pg_stat_activity pa ON pl.pid = pa.pid
    WHERE NOT pl.granted
    ORDER BY pa.query_start;
END;
$$;

-- =====================================================
-- 17. FUNCTION: Relatorio Completo de Saude (Resumo)
-- DESCRICAO:
--   Importancia: consolida os principais indicadores (extensoes,
--   cache, conexoes, bloat) em um painel resumido.
--   Proximos passos:
--     - Usar como primeiro check em incidentes de performance.
--     - Se algum item vier como CRITICO/Atencao, detalhar com as
--       funcoes especificas (top queries, cache por tabela, bloat, etc.).
-- =====================================================
CREATE OR REPLACE FUNCTION fn_health_report_summary()
RETURNS TABLE (
    categoria TEXT,
    metrica TEXT,
    valor TEXT,
    status TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cpu_queries INTEGER;
    v_cache_ratio NUMERIC;
    v_conn_usage NUMERIC;
    v_max_conn INTEGER;
    v_current_conn INTEGER;
    v_bloat_tables INTEGER;
BEGIN
    RAISE NOTICE '=== RELATORIO RESUMIDO DE SAUDE ===';
    RAISE NOTICE '';

    SELECT COUNT(*) INTO v_cpu_queries
    FROM pg_extension WHERE extname = 'pg_stat_statements';

    SELECT COALESCE(
        ROUND((sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100)::NUMERIC, 2),
        0
    ) INTO v_cache_ratio
    FROM pg_statio_user_tables;

    SELECT setting::INTEGER INTO v_max_conn FROM pg_settings WHERE name = 'max_connections';
    SELECT COUNT(*) INTO v_current_conn FROM pg_stat_activity;
    v_conn_usage := ROUND((v_current_conn::NUMERIC / v_max_conn * 100), 2);

    SELECT COUNT(*) INTO v_bloat_tables
    FROM pg_stat_user_tables
    WHERE ROUND((100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0))::NUMERIC, 2) > 10;

    RETURN QUERY
    SELECT
        '1. Extensoes'::TEXT,
        'pg_stat_statements'::TEXT,
        CASE WHEN v_cpu_queries > 0 THEN 'Instalada' ELSE 'NAO instalada' END::TEXT,
        CASE WHEN v_cpu_queries > 0 THEN 'OK' ELSE 'CRITICO' END::TEXT

    UNION ALL

    SELECT
        '2. Cache'::TEXT,
        'Cache Hit Ratio'::TEXT,
        v_cache_ratio || '%'::TEXT,
        CASE
            WHEN v_cache_ratio > 95 THEN 'Excelente'
            WHEN v_cache_ratio > 90 THEN 'Bom'
            ELSE 'Critico'
        END::TEXT

    UNION ALL

    SELECT
        '3. Conexoes'::TEXT,
        'Uso de Conexoes'::TEXT,
        v_current_conn || '/' || v_max_conn || ' (' || v_conn_usage || '%)'::TEXT,
        CASE
            WHEN v_conn_usage > 80 THEN 'Alto'
            WHEN v_conn_usage > 60 THEN 'Medio'
            ELSE 'OK'
        END::TEXT

    UNION ALL

    SELECT
        '4. Bloat'::TEXT,
        'Tabelas com Bloat > 10%'::TEXT,
        v_bloat_tables::TEXT,
        CASE
            WHEN v_bloat_tables > 10 THEN 'Atencao'
            WHEN v_bloat_tables > 5 THEN 'Monitorar'
            ELSE 'OK'
        END::TEXT;
END;
$$;


-- =====================================================
-- GUIA DE USO RAPIDO
-- =====================================================
/*

EXECUCAO DAS PROCEDURES E FUNCTIONS:

-- 1. Informacoes basicas
CALL sp_check_server_info();

-- 2. Queries ativas agora
SELECT * FROM fn_check_active_queries();

-- 3. Top queries consumindo CPU (requer pg_stat_statements)
SELECT * FROM fn_top_cpu_queries(20);

-- 4. Queries lentas (requer pg_stat_statements)
SELECT * FROM fn_slow_queries(1000, 20);

-- 5. Cache hit ratio
SELECT * FROM fn_cache_hit_ratio();
SELECT * FROM fn_cache_hit_by_table(20);

-- 6. Conexoes
SELECT * FROM fn_connection_stats();
SELECT * FROM fn_connections_by_database();

-- 7. Tamanho de banco e tabelas
SELECT * FROM fn_database_size();
SELECT * FROM fn_largest_tables(20);

-- 8. Indices
SELECT * FROM fn_unused_indexes();
SELECT * FROM fn_rarely_used_indexes(50);

-- 9. Bloat e VACUUM
SELECT * FROM fn_table_bloat(20);
SELECT * FROM fn_vacuum_stats(20);

-- 10. Configuracoes
SELECT * FROM fn_important_settings();

-- 11. Locks
SELECT * FROM fn_active_locks();

-- 12. Relatorio resumido (COMECE POR AQUI)
SELECT * FROM fn_health_report_summary();


IMPORTANTE:
- Para analise de CPU, instale primeiro: CREATE EXTENSION pg_stat_statements;
- Execute o relatorio resumido para visao geral: SELECT * FROM fn_health_report_summary();
- Agende execucoes regulares para monitoramento continuo

TODAS AS FUNCTIONS FORAM TESTADAS E ESTAO FUNCIONAIS
*/

    -- executar caso nao tenha a extensao
-- CREATE EXTENSION pg_stat_statements;

-- -- 1. Informacoes basicas
-- CALL sp_check_server_info();
--
-- -- 2. Queries ativas agora
-- SELECT * FROM fn_check_active_queries();
--
-- -- 3. Top queries consumindo CPU (requer pg_stat_statements)
-- SELECT * FROM fn_top_cpu_queries(20);
--
-- -- 4. Queries lentas (requer pg_stat_statements)
-- SELECT * FROM fn_slow_queries(1000, 20);
--
-- -- 5. Cache hit ratio
-- SELECT * FROM fn_cache_hit_ratio();
-- SELECT * FROM fn_cache_hit_by_table(20);
--
-- -- 6. Conexoes
-- SELECT * FROM fn_connection_stats();
-- SELECT * FROM fn_connections_by_database();
--
-- -- 7. Tamanho de banco e tabelas
-- SELECT * FROM fn_database_size();
-- SELECT * FROM fn_largest_tables(20);
--
-- -- 8. Indices
-- SELECT * FROM fn_unused_indexes();
-- SELECT * FROM fn_rarely_used_indexes(50);
--
-- -- 9. Bloat e VACUUM
-- SELECT * FROM fn_table_bloat(20);
-- SELECT * FROM fn_vacuum_stats(20);
--
-- -- 10. Configuracoes
-- SELECT * FROM fn_important_settings();
--
-- -- 11. Locks
-- SELECT * FROM fn_active_locks();
--
-- -- 12. Relatorio resumido (COMECE POR AQUI)
-- SELECT * FROM fn_health_report_summary();
