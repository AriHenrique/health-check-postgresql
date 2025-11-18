-- ================================
-- MANUTENCAO ROTINEIRA - RDS PostgreSQL
-- Script unificado para execucao
-- ================================

-- ================================================
-- PARAMETROS DE CONFIGURACAO - AJUSTE AQUI
-- ================================================
DO $$
DECLARE
    -- >>> CONFIGURACOES PRINCIPAIS - AJUSTE CONFORME NECESSARIO <
    v_app_user TEXT := 'usuario_teste_manutencao';  -- Usuario da aplicacao
    v_tabelas_criticas TEXT[] := ARRAY['<tab1>', '<tab2>'];  -- Tabelas para VACUUM FULL

    v_autovacuum_vacuum_scale_factor NUMERIC := 0.02;
    v_autovacuum_analyze_scale_factor NUMERIC := 0.02;
    v_autovacuum_vacuum_threshold INTEGER := 50;
    v_autovacuum_analyze_threshold INTEGER := 50;

    v_user_lock_timeout TEXT := '5s';
    v_user_statement_timeout TEXT := '30s';
    v_user_deadlock_timeout TEXT := '1s';

    v_db_vacuum_cost_delay TEXT := '5';
    v_db_vacuum_cost_limit TEXT := '400';
    v_db_lock_timeout TEXT := '10s';
    v_db_statement_timeout TEXT := '60s';

BEGIN
    DROP TABLE IF EXISTS config_params;
    CREATE TEMP TABLE config_params (
        param_name TEXT PRIMARY KEY,
        param_value TEXT
    );

    INSERT INTO config_params VALUES
        ('app_user', v_app_user),
        ('autovacuum_vacuum_scale_factor', v_autovacuum_vacuum_scale_factor::TEXT),
        ('autovacuum_analyze_scale_factor', v_autovacuum_analyze_scale_factor::TEXT),
        ('autovacuum_vacuum_threshold', v_autovacuum_vacuum_threshold::TEXT),
        ('autovacuum_analyze_threshold', v_autovacuum_analyze_threshold::TEXT),
        ('user_lock_timeout', v_user_lock_timeout),
        ('user_statement_timeout', v_user_statement_timeout),
        ('user_deadlock_timeout', v_user_deadlock_timeout),
        ('db_vacuum_cost_delay', v_db_vacuum_cost_delay),
        ('db_vacuum_cost_limit', v_db_vacuum_cost_limit),
        ('db_lock_timeout', v_db_lock_timeout),
        ('db_statement_timeout', v_db_statement_timeout),
        ('tabelas_criticas', array_to_string(v_tabelas_criticas, ',')),
        ('execution_timestamp', now()::TEXT);

    RAISE NOTICE '';
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'PARAMETROS DE CONFIGURACAO CARREGADOS';
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Usuario da aplicacao: %', v_app_user;
    RAISE NOTICE 'Tabelas criticas: %', v_tabelas_criticas;
    RAISE NOTICE 'Autovacuum scale factor: %', v_autovacuum_vacuum_scale_factor;
    RAISE NOTICE '=================================================';
END $$;

-- ================================================
-- CONFIGURACOES QUE DEVEM SER FEITAS NO RDS PARAMETER GROUP
-- ================================================
-- IMPORTANTE: Configure estes parametros no Console AWS RDS:
--
-- 1. Acesse RDS > Parameter Groups
-- 2. Selecione seu parameter group
-- 3. Configure estes valores:
--    * autovacuum = on
--    * autovacuum_max_workers = 8
--    * autovacuum_vacuum_scale_factor = 0.02
--    * autovacuum_analyze_scale_factor = 0.02
--    * autovacuum_vacuum_threshold = 50
--    * autovacuum_analyze_threshold = 50
--    * autovacuum_naptime = 30
--    * autovacuum_vacuum_cost_delay = 5
--    * autovacuum_vacuum_cost_limit = 400
-- 4. Apply changes (pode requerer reboot)
-- ================================================

-- ================================================
-- 1) APLICAR AUTOVACUUM EM TODAS AS TABELAS DO BANCO
-- ================================================
DO $$
DECLARE
    tabela RECORD;
    comando TEXT;
    contador INTEGER := 0;
    v_scale_factor TEXT;
    v_analyze_factor TEXT;
    v_threshold TEXT;
    v_analyze_threshold TEXT;
BEGIN
    SELECT param_value INTO v_scale_factor FROM config_params WHERE param_name = 'autovacuum_vacuum_scale_factor';
    SELECT param_value INTO v_analyze_factor FROM config_params WHERE param_name = 'autovacuum_analyze_scale_factor';
    SELECT param_value INTO v_threshold FROM config_params WHERE param_name = 'autovacuum_vacuum_threshold';
    SELECT param_value INTO v_analyze_threshold FROM config_params WHERE param_name = 'autovacuum_analyze_threshold';

    RAISE NOTICE 'Iniciando configuracao de autovacuum nas tabelas...';

    FOR tabela IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        comando := format('ALTER TABLE %I.%I SET (
            autovacuum_vacuum_scale_factor = %s,
            autovacuum_analyze_scale_factor = %s,
            autovacuum_vacuum_threshold = %s,
            autovacuum_analyze_threshold = %s
        )', tabela.schemaname, tabela.tablename, v_scale_factor, v_analyze_factor, v_threshold, v_analyze_threshold);

        EXECUTE comando;
        contador := contador + 1;
        IF contador % 10 = 0 THEN
            RAISE NOTICE 'Progresso: % tabelas configuradas...', contador;
        END IF;
    END LOOP;

    RAISE NOTICE 'Autovacuum configurado em % tabelas', contador;
END $$;

-- ================================================
-- 2) CONFIGURAR TIMEOUTS PARA O ROLE DA APLICACAO
-- ================================================
DO $$
DECLARE
    app_user TEXT;
    v_lock_timeout TEXT;
    v_statement_timeout TEXT;
    v_deadlock_timeout TEXT;
BEGIN
    SELECT param_value INTO app_user FROM config_params WHERE param_name = 'app_user';
    SELECT param_value INTO v_lock_timeout FROM config_params WHERE param_name = 'user_lock_timeout';
    SELECT param_value INTO v_statement_timeout FROM config_params WHERE param_name = 'user_statement_timeout';
    SELECT param_value INTO v_deadlock_timeout FROM config_params WHERE param_name = 'user_deadlock_timeout';

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app_user) THEN
        EXECUTE format('ALTER ROLE %I SET lock_timeout = %L', app_user, v_lock_timeout);
        EXECUTE format('ALTER ROLE %I SET statement_timeout = %L', app_user, v_statement_timeout);
        EXECUTE format('ALTER ROLE %I SET deadlock_timeout = %L', app_user, v_deadlock_timeout);

        RAISE NOTICE 'Configuracoes aplicadas para role: %', app_user;
        RAISE NOTICE '  - lock_timeout = %', v_lock_timeout;
        RAISE NOTICE '  - statement_timeout = %', v_statement_timeout;
        RAISE NOTICE '  - deadlock_timeout = %', v_deadlock_timeout;
    ELSE
        RAISE WARNING 'Usuario % nao existe. Pulando configuracoes de role.', app_user;
    END IF;
END $$;

-- ================================================
-- 3) CONFIGURAR PARAMETROS GERAIS DO DATABASE
-- ================================================
DO $$
DECLARE
    db_name TEXT;
    v_vacuum_delay TEXT;
    v_vacuum_limit TEXT;
    v_lock_timeout TEXT;
    v_statement_timeout TEXT;
    has_permission BOOLEAN := TRUE;
BEGIN
    SELECT current_database() INTO db_name;

    SELECT param_value INTO v_vacuum_delay FROM config_params WHERE param_name = 'db_vacuum_cost_delay';
    SELECT param_value INTO v_vacuum_limit FROM config_params WHERE param_name = 'db_vacuum_cost_limit';
    SELECT param_value INTO v_lock_timeout FROM config_params WHERE param_name = 'db_lock_timeout';
    SELECT param_value INTO v_statement_timeout FROM config_params WHERE param_name = 'db_statement_timeout';

    BEGIN
        EXECUTE format('ALTER DATABASE %I SET vacuum_cost_delay = %L', db_name, v_vacuum_delay);
        EXECUTE format('ALTER DATABASE %I SET vacuum_cost_limit = %L', db_name, v_vacuum_limit);
        EXECUTE format('ALTER DATABASE %I SET lock_timeout = %L', db_name, v_lock_timeout);
        EXECUTE format('ALTER DATABASE %I SET statement_timeout = %L', db_name, v_statement_timeout);

        RAISE NOTICE 'Configuracoes aplicadas para database: %', db_name;
        RAISE NOTICE '  - vacuum_cost_delay = %', v_vacuum_delay;
        RAISE NOTICE '  - vacuum_cost_limit = %', v_vacuum_limit;
        RAISE NOTICE '  - lock_timeout = %', v_lock_timeout;
        RAISE NOTICE '  - statement_timeout = %', v_statement_timeout;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE 'Sem permissao para alterar configuracoes do database. Continuando...';
        has_permission := FALSE;
    END;
END $$;

-- ================================================
-- 4) NOTA SOBRE RECARREGAR CONFIGURACOES
-- ================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'NOTA: As configuracoes aplicadas serao efetivas';
    RAISE NOTICE 'para NOVAS conexoes. Conexoes existentes mantem';
    RAISE NOTICE 'as configuracoes antigas.';
    RAISE NOTICE '=================================================';
END $$;

-- ================================================
-- 5) EXECUTAR VACUUM E ANALYZE EM TODO O BANCO
-- ================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Iniciando VACUUM ANALYZE em todo o banco...';
    RAISE NOTICE 'Isto pode demorar alguns minutos dependendo do tamanho do banco.';
END $$;

VACUUM ANALYZE;

-- ================================================
-- 6) MANUTENCAO PESADA NAS TABELAS CRITICAS
-- ================================================
-- ATENCAO: VACUUM FULL bloqueia as tabelas!
DO $$
DECLARE
    tabela_critica TEXT;
    tabelas_str TEXT;
    tabelas_criticas TEXT[];
    tempo_inicio TIMESTAMP;
    tempo_tabela INTERVAL;
BEGIN
    SELECT param_value INTO tabelas_str FROM config_params WHERE param_name = 'tabelas_criticas';
    tabelas_criticas := string_to_array(tabelas_str, ',');

    IF array_length(tabelas_criticas, 1) > 0 THEN
        RAISE NOTICE '';
        RAISE NOTICE '=== INICIANDO VACUUM FULL NAS TABELAS CRITICAS ===';
        RAISE NOTICE 'ATENCAO: VACUUM FULL bloqueia as tabelas!';

        FOREACH tabela_critica IN ARRAY tabelas_criticas
        LOOP
            IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = tabela_critica) THEN
                tempo_inicio := clock_timestamp();

                RAISE NOTICE 'Iniciando VACUUM FULL em: public.%', tabela_critica;
                EXECUTE format('VACUUM FULL ANALYZE public.%I', tabela_critica);

                RAISE NOTICE 'Iniciando REINDEX em: public.%', tabela_critica;
                EXECUTE format('REINDEX TABLE public.%I', tabela_critica);

                tempo_tabela := clock_timestamp() - tempo_inicio;
                RAISE NOTICE 'Manutencao concluida em: public.% (tempo: %)', tabela_critica, tempo_tabela;
            ELSE
                RAISE NOTICE 'Tabela public.% nao encontrada, pulando...', tabela_critica;
            END IF;
        END LOOP;
    ELSE
        RAISE NOTICE 'Nenhuma tabela critica configurada para VACUUM FULL';
    END IF;
END $$;

-- ================================================
-- 7) RELATORIO FINAL DE STATUS
-- ================================================

SELECT '=== CONFIGURACOES APLICADAS ===' as info
UNION ALL
SELECT param_name || ': ' || param_value
FROM config_params
WHERE param_name NOT IN ('execution_timestamp', 'tabelas_criticas')
ORDER BY 1;

SELECT '=== CONFIGURACOES GLOBAIS DE AUTOVACUUM ===' as info
UNION ALL
SELECT name || ' = ' || setting || COALESCE(' ' || unit, '')
FROM pg_settings
WHERE name LIKE 'autovacuum%'
ORDER BY 1;

WITH dead_tuples AS (
    SELECT
        t.schemaname || '.' || t.relname ||
        ' -> Dead: ' || t.n_dead_tup::text ||
        ' (' || round(100.0 * t.n_dead_tup / NULLIF(t.n_live_tup + t.n_dead_tup, 0), 2)::text || '%)' ||
        ' | Size: ' || pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.relname)) as info,
        t.n_dead_tup
    FROM pg_stat_user_tables t
    WHERE t.schemaname = 'public'
      AND t.n_dead_tup > 0
    ORDER BY t.n_dead_tup DESC
    LIMIT 10
)
SELECT '=== TOP 10 TABELAS COM MAIS DEAD TUPLES ===' as info
UNION ALL
SELECT info FROM dead_tuples;

WITH vacuum_info AS (
    SELECT
        t.relname ||
        ' -> Vacuum: ' || COALESCE(t.last_vacuum::text, 'nunca') ||
        ' | Autovacuum: ' || COALESCE(t.last_autovacuum::text, 'nunca') as info,
        GREATEST(t.last_vacuum, t.last_autovacuum) as last_run
    FROM pg_stat_user_tables t
    WHERE t.schemaname = 'public'
      AND (t.last_vacuum IS NOT NULL OR t.last_autovacuum IS NOT NULL)
    ORDER BY last_run DESC NULLS LAST
    LIMIT 10
)
SELECT '=== ULTIMAS EXECUCOES DE VACUUM ===' as info
UNION ALL
SELECT info FROM vacuum_info;

DO $$
DECLARE
    v_user TEXT;
    v_timestamp TEXT;
    v_db_size TEXT;
    v_table_count INTEGER;
BEGIN
    SELECT param_value INTO v_user FROM config_params WHERE param_name = 'app_user';
    SELECT param_value INTO v_timestamp FROM config_params WHERE param_name = 'execution_timestamp';
    SELECT pg_size_pretty(pg_database_size(current_database())) INTO v_db_size;
    SELECT count(*) INTO v_table_count FROM pg_tables WHERE schemaname = 'public';

    RAISE NOTICE '';
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'RESUMO DA MANUTENCAO';
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Database: %', current_database();
    RAISE NOTICE 'Usuario configurado: %', v_user;
    RAISE NOTICE 'Execucao iniciada em: %', v_timestamp;
    RAISE NOTICE 'Tamanho do banco: %', v_db_size;
    RAISE NOTICE 'Total de tabelas configuradas: %', v_table_count;
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'MANUTENCAO CONCLUIDA COM SUCESSO!';
    RAISE NOTICE '=================================================';
END $$;

DROP TABLE IF EXISTS config_params;

-- ================================================
-- FIM DA MANUTENCAO ROTINEIRA
-- ================================================
