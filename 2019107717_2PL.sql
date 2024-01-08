WITH TP AS (
    SELECT
        "#t",
        MAX(CASE WHEN op IN ('read_lock', 'write_lock') THEN time END) AS expansionTime, -- Ultimo valor de tempo que possua read_lock/write_lock
		MAX(CASE WHEN op IN ('read_item', 'write_item') THEN time END) AS opTime, -- Ultimo valor de tempo que realiza uma leitura ou escrita
        MIN(CASE WHEN op = 'unlock' THEN time END) AS shrinkTime -- Primeiro valor de tempo que posssua unlock
    FROM
        "Schedule"
    GROUP BY
        "#t"
)
SELECT
    CASE
        WHEN TP.expansionTime < TP.shrinkTime AND TP.expansionTime < TP.opTime
        THEN 1
        ELSE 0
    END AS "RESP"
FROM
   TP
ORDER BY TP."#t"