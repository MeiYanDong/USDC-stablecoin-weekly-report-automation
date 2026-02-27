# USDC 周报推送器

每周一在北京时间 `07:00` 自动拉取稳定币数据，生成 USDC 周报并推送到飞书群。

## 功能概览

- 从 DefiLlama 获取全市场稳定币总供给与 USDC 供给。
- 从 Dune 获取稳定币近 7 天链上转账量结果，计算 USDC 的 7 天转账量份额。
- 将结果追加写入 `data/weekly_history.json`，默认保留最近 52 条，用于下次 WoW 计算。
- 通过 GitHub Actions 定时运行，并将最新历史文件提交回仓库。
- 任意异常都会尽量发送一条飞书失败通知，包含失败原因和已获取的部分指标。

## 指标口径

- `total_supply_usd`
  DefiLlama 全市场稳定币供给，取 `stablecoincharts/all` 最新日期中的 `totalCirculating.peggedUSD`。
- `usdc_supply_usd`
  DefiLlama 稳定币列表中，`pegType = peggedUSD` 且 `symbol = USDC` 的最新供给值。
- `usdc_supply_share`
  `usdc_supply_usd / total_supply_usd`。
- `usdc_transfer_volume_share_7d`
  基于 Dune query 输出的稳定币 7 天链上转账量结果，取 USDC 的近 7 天转账量，除以分母集合近 7 天转账量之和。

## Dune Query 输出约定

推荐在 Dune 上准备一个 query，输出按 stablecoin 聚合后的结果表，至少包含：

- `symbol`
- `volume_7d_usd`

脚本会用 DefiLlama 的 `peggedUSD` top20 作为分母候选集合，再在 Python 中与 Dune query 结果做交集计算。为了兼容一些已有 query，脚本也会尝试识别常见别名列名，例如 `token_symbol`、`asset`、`volume_usd`、`transfer_volume_usd`，但推荐还是固定使用 `symbol` 和 `volume_7d_usd`。

## 分母集合与 missing_symbols

- 分母集合来自 DefiLlama 稳定币列表。
- 仅保留 `pegType = peggedUSD`。
- 按 `circulating.peggedUSD` 降序取前 20 个 symbol。
- symbol 会统一转成小写后与 Dune query 结果做匹配。
- 如果某个 symbol 没有出现在 Dune query 结果中，会跳过该 symbol，并记录到 `missing_symbols`。
- 被记录在 `missing_symbols` 中的 symbol 不计入分母。

## 本地运行

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

PowerShell:

```powershell
$env:DUNE_API_KEY="your-dune-api-key"
$env:DUNE_QUERY_ID="your-dune-query-id"
$env:FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx"
$env:FORCE_RUN="1"
```

Bash:

```bash
export DUNE_API_KEY="your-dune-api-key"
export DUNE_QUERY_ID="your-dune-query-id"
export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx"
export FORCE_RUN="1"
```

说明：

- `DUNE_API_KEY` 必填。
- `DUNE_QUERY_ID` 必填。
- `FEISHU_WEBHOOK_URL` 必填。
- `FORCE_RUN=1` 仅用于手动调试。正常定时运行不需要设置。
- 脚本启动时会自动读取仓库根目录下的 `.env` 文件。

### 3. 执行脚本

```bash
python stablecoin_weekly.py
```

脚本默认只会在北京时间周一 `07:00` 继续执行。手动调试请设置 `FORCE_RUN=1`。

## GitHub Actions 配置

仓库已包含工作流文件 `.github/workflows/stablecoin-weekly.yml`。

### Secrets

在 GitHub 仓库中进入 `Settings -> Secrets and variables -> Actions`，添加以下 secrets：

- `DUNE_API_KEY`
- `DUNE_QUERY_ID`
- `FEISHU_WEBHOOK_URL`

### 触发方式

- `schedule`
  每周一北京时间 `07:00` 触发一次，对应 GitHub Actions 的 cron 为每周日 `23:00 UTC`。
- `workflow_dispatch`
  支持手动触发。工作流会自动把 `FORCE_RUN=1` 注入脚本，方便手动联调。

### 历史文件回写

工作流会在脚本完成后检查 `data/weekly_history.json` 是否变更。如果变更，会使用 `GITHUB_TOKEN` 自动提交并推回仓库，供下一次 WoW 读取。

## 飞书机器人配置

1. 在目标飞书群中添加自定义机器人。
2. 复制机器人 webhook 地址。
3. 将 webhook 填入 GitHub Actions Secret `FEISHU_WEBHOOK_URL`，或本地环境变量 `FEISHU_WEBHOOK_URL`。

脚本优先发送交互式卡片；如果卡片失败，会降级为纯文本消息再试一次。

## 输出与历史文件

- 主脚本：`stablecoin_weekly.py`
- 历史文件：`data/weekly_history.json`
- 工作流：`.github/workflows/stablecoin-weekly.yml`

历史文件每条记录包含：

- `run_time_beijing`
- `start_date`
- `end_date`
- `metrics`
- `missing_symbols`

## 常见故障排查

- 飞书返回 4xx
  检查 webhook 是否正确、机器人是否仍在群里、群机器人安全设置是否限制来源。
- Dune API 报鉴权错误
  检查 `DUNE_API_KEY` 是否有效，是否已经写入本地环境变量或 GitHub Actions Secret。
- Dune query 执行成功但结果为空
  检查 query 是否真的输出了 `symbol` 和 7 天 volume 列，或查询条件是否过严。
- DefiLlama 不通或超时
  脚本会自动重试 3 次；如果仍失败，会发送失败通知并退出。
- `missing_symbols` 不为空
  说明部分 DefiLlama top20 稳定币没有出现在 Dune query 结果中，脚本会自动从分母中跳过这些 symbol。
- 手动执行没有真正跑任务
  本地运行时请确认设置了 `FORCE_RUN=1`。GitHub Actions 的 `workflow_dispatch` 已自动注入该变量。

## Dune SQL 示例

以下 SQL 适合当前脚本，不需要你额外配置 query 参数。它会在 Dune 端直接按北京时间计算“过去 7 天，截止昨天”的窗口。

```sql
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
```

## 实现说明

- 所有金额和百分比格式化都集中在脚本内的统一 util 函数中。
- 所有网络请求都集中在单独函数中，便于后续补测试。
- 所有日志输出到 stdout，适合本地与 GitHub Actions 直接查看。
