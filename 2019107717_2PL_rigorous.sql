WITH TP AS (
	SELECT
        "#t",
		MIN(CASE WHEN op IN ('read_lock', 'write_lock') THEN time END) AS beginExpansionTime, -- Primeiro Valor de tempo da fase de expansão
        MAX(CASE WHEN op IN ('read_lock', 'write_lock') THEN time END) AS expansionTime, -- Ultimo valor de tempo da fase de expansão
		MIN(CASE WHEN op IN ('read_item', 'write_item') THEN time END) AS beginOpTime, -- Primeiro Valor de tempo que realiza operação
		MAX(CASE WHEN op IN ('read_item', 'write_item') THEN time END) AS opTime, -- Ultimo valor de tempo que realiza uma leitura ou escrita
        MIN(CASE WHEN op = 'unlock' THEN time END) AS shrinkTime, -- Primeiro valor de tempo que posssua unlock
		MAX(CASE WHEN op = 'unlock' THEN time END) AS endShrinkTime, -- Ultimop valor de tempo da fase de encolhimento
		MIN(CASE WHEN op IN ('abort', 'commit') THEN time END) AS commitAbort -- Ultimo valor de tempo da fase de encolhimento
    FROM
        "Schedule"
    GROUP BY
        "#t"
)
SELECT
    CASE
        WHEN TP.expansionTime < TP.shrinkTime AND TP.commitAbort < TP.shrinkTime -- Expansão termina antes da primeira operação e Expansão termina antes da fase de encolhimento 
        THEN 1
        ELSE 0
    END AS "RESP"
FROM
   TP
ORDER BY TP."#t"