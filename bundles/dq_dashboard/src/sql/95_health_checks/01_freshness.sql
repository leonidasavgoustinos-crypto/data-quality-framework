-- Health check (daily job): fails when the DQ framework has not written results recently,
-- so the dashboard would show stale numbers. Threshold = bundle variable freshness_max_days.
SELECT CASE
         WHEN MAX(run_date) IS NULL
           THEN raise_error('No DQ executions found in ${catalog}.${schema}.v_simple_executions')
         WHEN DATEDIFF(current_date(), MAX(run_date)) > ${freshness_max_days}
           THEN raise_error('Newest DQ execution is ' || DATEDIFF(current_date(), MAX(run_date)) || ' days old (limit ${freshness_max_days})')
         ELSE 'ok'
       END AS freshness
FROM ${catalog}.${schema}.v_simple_executions;
