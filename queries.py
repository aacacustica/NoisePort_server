QUERY_AVG_LAEQ = """
SELECT 
    DATE_FORMAT(Timestamp, '%Y-%m-%d %H:00:00') AS hour,
    10 * LOG10(AVG(POWER(10, LA/10))) AS AVG_LAeq,
    MAX(LAmax) AS max_LAmax,
    MIN(LAmin) AS min_LAmin
FROM acoustic_data
GROUP BY hour;
"""