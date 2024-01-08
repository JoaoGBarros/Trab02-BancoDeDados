WITH TP_ATTR AS (
    SELECT
        "#t",
        attr,
		MAX(CASE WHEN op IN ('read_item', 'write_item') THEN time END) AS opTime,
		MAX(CASE WHEN op IN ('read_lock', 'write_lock') THEN time END) AS expansionTime,
        MIN(CASE WHEN op IN ('read_item', 'write_item') THEN time END) AS beginOpTime,
        MAX(CASE WHEN op IN ('read_lock') THEN time END) AS readLockTime,
        MIN(CASE WHEN op = 'unlock' THEN time END) AS shrinkTime,
        MAX(CASE WHEN op = 'unlock' THEN time END) AS endShrinkTime,
        MIN(CASE WHEN op IN ('write_lock') THEN time END) AS writeLockTime
    FROM
        "Schedule"
    WHERE
        attr != '-'
    GROUP BY
        "#t", attr
),
TP_COMMIT_ABORT AS (
    SELECT
        "#t",
        MIN(CASE WHEN op IN ('abort', 'commit') THEN time END) AS commitAbort
    FROM
        "Schedule"
    WHERE
        attr = '-'
    GROUP BY
        "#t", attr
),
TP AS (
    SELECT TP_ATTR."#t",
	attr,
	opTime,
	expansionTime,
	beginOpTime,
	readLockTime,
	shrinkTime,
	endShrinkTime,
	writeLockTime,
	commitAbort
    FROM TP_ATTR 
    LEFT JOIN TP_COMMIT_ABORT ON TP_ATTR."#t" = TP_COMMIT_ABORT."#t"
),

TP_READLOCK AS (
    SELECT * 
    FROM TP 
    WHERE TP.readLockTime IS NOT NULL
),

TP_WRITELOCK AS (
    SELECT *
    FROM TP
    WHERE TP.writeLockTime IS NOT NULL
),

TP_WRITELOCK_STATUS AS (SELECT
    "#t",
    CASE
        WHEN TP_WRITELOCK.writeLockTime < TP_WRITELOCK.beginOpTime
		AND TP_WRITELOCK.endShrinkTime > TP_WRITELOCK.commitAbort
		AND TP_WRITELOCK.expansionTime < TP_WRITELOCK.shrinkTime
		AND TP_WRITELOCK.expansionTime < TP_WRITELOCK.opTime
        THEN 1
        ELSE 0
    END AS "RESP"
	FROM TP_WRITELOCK
),

TP_READLOCK_STATUS AS (SELECT
    "#t",
    CASE
        WHEN TP_READLOCK.readLockTime < TP_READLOCK.beginOpTime 
		AND TP_READLOCK.endShrinkTime < TP_READLOCK.commitAbort
		AND TP_READLOCK.expansionTime < TP_READLOCK.shrinkTime
		AND TP_READLOCK.expansionTime < TP_READLOCK.opTime
        THEN 1
        ELSE 0
    END AS "RESP"
	FROM TP_READLOCK
)

SELECT
    CASE WHEN TP_WRITELOCK_STATUS."RESP" = TP_READLOCK_STATUS."RESP" THEN 1 ELSE 0 END AS "RESP"
FROM
    TP_WRITELOCK_STATUS
JOIN TP_READLOCK_STATUS ON TP_WRITELOCK_STATUS."#t" = TP_WRITELOCK_STATUS."#t"
GROUP BY TP_WRITELOCK_STATUS."#t", TP_WRITELOCK_STATUS."RESP",  TP_READLOCK_STATUS."RESP"
ORDER BY TP_WRITELOCK_STATUS."#t"


