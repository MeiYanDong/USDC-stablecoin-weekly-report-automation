WITH bj_today AS (
    SELECT
        CAST(at_timezone(current_timestamp, 'Asia/Shanghai') AS date) AS today_bj
),

params AS (
    SELECT
        today_bj - INTERVAL '7' DAY AS start_date,
        today_bj - INTERVAL '1' DAY AS end_date
    FROM bj_today
),

evm_volume AS (
    SELECT
        LOWER(token_symbol) AS symbol,
        SUM(amount_usd) AS volume_usd
    FROM stablecoins_evm.transfers
    CROSS JOIN params
    WHERE block_month >= DATE_TRUNC('month', start_date)
      AND block_date >= start_date
      AND block_date <= end_date
      AND token_symbol IS NOT NULL
      AND amount_usd IS NOT NULL
    GROUP BY 1
),

solana_volume AS (
    SELECT
        LOWER(token_symbol) AS symbol,
        SUM(amount_usd) AS volume_usd
    FROM stablecoins_solana.transfers
    CROSS JOIN params
    WHERE block_month >= DATE_TRUNC('month', start_date)
      AND block_date >= start_date
      AND block_date <= end_date
      AND token_symbol IS NOT NULL
      AND amount_usd IS NOT NULL
    GROUP BY 1
),

tron_volume AS (
    SELECT
        LOWER(token_symbol) AS symbol,
        SUM(amount_usd) AS volume_usd
    FROM stablecoins_tron.transfers
    CROSS JOIN params
    WHERE block_month >= DATE_TRUNC('month', start_date)
      AND block_date >= start_date
      AND block_date <= end_date
      AND token_symbol IS NOT NULL
      AND amount_usd IS NOT NULL
    GROUP BY 1
),

combined AS (
    SELECT symbol, volume_usd FROM evm_volume
    UNION ALL
    SELECT symbol, volume_usd FROM solana_volume
    UNION ALL
    SELECT symbol, volume_usd FROM tron_volume
)

SELECT
    symbol,
    SUM(volume_usd) AS volume_7d_usd
FROM combined
GROUP BY 1
HAVING SUM(volume_usd) > 0
ORDER BY volume_7d_usd DESC;
