# USDC 周报推送器实施 TODO

基于 [docs/需求文档.md](../docs/需求文档.md) 拆解的可执行任务清单。每一项都应在完成后勾选，便于跟踪进度。

## 阶段 0：项目初始化

- [ ] 确认仓库最终目录结构，至少包含 `stablecoin_weekly.py`、`requirements.txt`、`.github/workflows/stablecoin-weekly.yml`、`README.md`、`data/weekly_history.json`、`docs/dune_query.sql`。
- [ ] 新建 `data/` 目录，并初始化 `data/weekly_history.json` 为合法 JSON 数组 `[]`。
- [ ] 明确运行环境为 Python 3.11，并统一脚本入口为 `python stablecoin_weekly.py`。
- [ ] 确认所有配置均来自环境变量：`DUNE_API_KEY`、`DUNE_QUERY_ID`、`FEISHU_WEBHOOK_URL`、`FORCE_RUN`。
- [ ] 规划脚本模块职责，至少拆分为：时间门禁、网络请求、指标计算、历史读写、飞书通知、日志输出。

## 阶段 1：搭建主脚本骨架

- [ ] 创建 `stablecoin_weekly.py`，补齐 `main()` 入口和 `if __name__ == "__main__":` 启动逻辑。
- [ ] 配置标准输出日志，保证关键步骤和异常信息都输出到 stdout。
- [ ] 增加北京时间处理逻辑，统一通过 `Asia/Shanghai` 计算当前时间、统计日期和展示日期。
- [ ] 实现时间门禁：默认仅允许在周一 07:00 Asia/Shanghai 执行。
- [ ] 实现 `FORCE_RUN=1` 旁路逻辑，手动调试时跳过时间门禁。
- [ ] 确保非执行窗口下脚本正常退出且返回码为 0。

## 阶段 2：封装网络请求与重试机制

- [ ] 在 `requirements.txt` 中加入 `requests` 与 `tenacity`。
- [ ] 选择时区依赖方案：优先使用标准库 `zoneinfo`，避免引入不必要第三方依赖。
- [ ] 为所有 HTTP 请求封装统一函数，统一设置超时为 20 秒。
- [ ] 为 DefiLlama 请求接入指数退避重试策略，并记录失败日志。
- [ ] 为 Dune 请求接入指数退避重试策略，并记录失败日志。
- [ ] 为飞书 webhook 请求接入异常捕获与降级处理。
- [ ] 统一处理 HTTP 非 2xx、空响应、JSON 解析失败、字段缺失等异常场景。

## 阶段 3：接入 DefiLlama 数据

- [ ] 实现获取稳定币列表接口：`GET https://stablecoins.llama.fi/stablecoins`。
- [ ] 从稳定币列表中过滤 `pegType == "peggedUSD"` 的资产。
- [ ] 按 `circulating` 降序排序，取 top20 的 `symbol` 作为 Dune 分母候选集合。
- [ ] 将候选 `symbol` 统一转换为小写。
- [ ] 实现获取全市场稳定币供给接口：`GET https://stablecoins.llama.fi/stablecoincharts/all`。
- [ ] 从全市场时间序列中取最新日期的 `totalCirculating.peggedUSD`，得到 `total_supply_usd`。
- [ ] 从稳定币列表中提取 USDC 的最新供给，得到 `usdc_supply_usd`。
- [ ] 计算 `usdc_supply_share = usdc_supply_usd / total_supply_usd`。
- [ ] 为 DefiLlama 数据增加基础校验，避免出现 0 值、空值或结构变化导致的误算。

## 阶段 4：接入 Dune 数据并计算 7 日份额

- [ ] 以 Asia/Shanghai 日期为准，计算 `end_date = 昨天`，`start_date = end_date 往前 6 天`。
- [ ] 在 `docs/dune_query.sql` 中维护一条可直接执行的 Dune SQL。
- [ ] 实现 Dune Query Execute API 调用。
- [ ] 实现 Dune Execution Status 轮询逻辑。
- [ ] 实现 Dune Execution Results 获取逻辑。
- [ ] 解析 Dune query 返回结果中的 `symbol` 与 `volume_7d_usd`。
- [ ] 单独提取 USDC 在统计区间内的 7 天链上转账量总和。
- [ ] 对 top20 候选集合逐个纳入分母，仅累计 Dune 结果中存在且数据有效的 symbol。
- [ ] 将 Dune 缺失的 symbol 记录到 `missing_symbols`。
- [ ] 对缺失的 symbol 不计入分母，最终计算 `usdc_transfer_volume_share_7d`。
- [ ] 若 USDC 本身缺失或分母为 0，定义明确的降级行为并保留错误信息用于通知。

## 阶段 5：统一格式化与结果组装

- [ ] 提取统一的数值格式化工具函数，集中处理金额与百分比显示。
- [ ] 将 USD 金额统一格式化为保留 2 位小数。
- [ ] 将份额统一格式化为百分比并保留 2 位小数。
- [ ] 组装本次周报的结果对象，至少包含 `run_time_beijing`、`start_date`、`end_date`、`metrics`、`missing_symbols`。
- [ ] 在结果对象中保留原始数值字段，避免仅保存格式化后的字符串。
- [ ] 为输出内容补充数据源说明：Supply 来自 DefiLlama，Transfer Volume 来自 Dune。

## 阶段 6：历史落地与 WoW 计算

- [ ] 实现读取 `data/weekly_history.json` 的函数，兼容文件不存在、空文件、非法 JSON 等情况。
- [ ] 实现追加保存本次结果的函数，并保持历史记录为数组结构。
- [ ] 在写入时仅保留最近 52 条记录，移除更早的数据。
- [ ] 实现“上一次周报”读取逻辑，用于与本次结果进行 WoW 对比。
- [ ] 为四个指标分别计算 WoW 变化。
- [ ] 若不存在上次记录，或上次记录缺少对应字段，则将 WoW 显示为 `N/A`。
- [ ] 确保历史写入发生在成功拿到本次可用结果后，避免无效数据污染历史。

## 阶段 7：飞书消息通知

- [ ] 实现飞书自定义机器人 webhook 的 POST JSON 发送逻辑。
- [ ] 优先实现飞书交互式消息卡片，覆盖统计日期、四个指标、WoW、`missing_symbols`、数据源说明。
- [ ] 若 `missing_symbols` 为空，则显示 `none`。
- [ ] 在卡片中明确本次统计日期使用北京时间口径。
- [ ] 设计失败通知内容，至少包含失败原因和已成功取得的部分指标。
- [ ] 当卡片发送失败时，退化为纯文本消息再次尝试发送。
- [ ] 确保脚本在任何异常情况下仍尽量发出一条失败通知。

## 阶段 8：主流程编排与异常兜底

- [ ] 串联完整执行顺序：时间门禁 -> 拉取 DefiLlama -> 执行 Dune query -> 计算指标 -> 读取历史 -> 计算 WoW -> 保存历史 -> 发送飞书。
- [ ] 在主流程中区分“可恢复错误”和“终止错误”，避免局部失败导致整个流程静默退出。
- [ ] 实现顶层异常捕获，统一整理失败原因。
- [ ] 在失败路径中尽可能附带已成功计算出的部分指标。
- [ ] 明确脚本退出码策略，保证 GitHub Actions 可识别成功或失败。

## 阶段 9：补齐依赖与仓库文件

- [ ] 编写 `requirements.txt`，保证在 GitHub Actions Ubuntu runner 上可直接安装。
- [ ] 补齐 `README.md`，覆盖本地运行、环境变量、Actions Secrets、飞书 webhook 配置、指标口径、故障排查。
- [ ] 在 README 中解释链上 transfer volume 的含义及 `missing_symbols` 的业务意义。
- [ ] 在 README 中说明 `FORCE_RUN=1` 的用途和使用方式。

## 阶段 10：配置 GitHub Actions

- [ ] 创建 `.github/workflows/stablecoin-weekly.yml`。
- [ ] 配置 `workflow_dispatch`，支持手动触发。
- [ ] 配置 `schedule` cron：周日 `23:00 UTC`，对应周一 `07:00 Asia/Shanghai`。
- [ ] 设置 workflow `permissions: contents: write`，允许回写历史文件。
- [ ] 使用 `actions/checkout` 拉取仓库代码。
- [ ] 使用 `actions/setup-python` 安装 Python 3.11，并开启 pip cache。
- [ ] 安装 `requirements.txt` 中的依赖。
- [ ] 运行 `python stablecoin_weekly.py`。
- [ ] 通过 env 注入 `DUNE_API_KEY`、`DUNE_QUERY_ID` 与 `FEISHU_WEBHOOK_URL`。
- [ ] 在脚本运行后检查 `data/weekly_history.json` 是否变更。
- [ ] 若历史文件变更，则执行 `git add data/weekly_history.json`。
- [ ] 配置 `git user.name` 为 `github-actions[bot]`。
- [ ] 配置 `git user.email` 为 `github-actions[bot]@users.noreply.github.com`。
- [ ] 使用 `GITHUB_TOKEN` 提交并推送历史文件更新。
- [ ] 确认 workflow 不监听 `push`，避免提交历史文件后形成循环触发。

## 阶段 11：联调与验收

- [ ] 在本地通过 `FORCE_RUN=1` 完整执行一次，验证脚本主流程可跑通。
- [ ] 验证 DefiLlama 指标计算结果是否合理，重点检查 `total_supply_usd` 与 `usdc_supply_share`。
- [ ] 验证 Dune SQL 的北京时间 7 日窗口是否准确覆盖 7 个自然日。
- [ ] 验证 `missing_symbols` 在正常场景和异常场景下都能正确输出。
- [ ] 验证首次运行时 WoW 显示为 `N/A`。
- [ ] 伪造一条历史记录或运行第二次，验证 WoW 计算逻辑正确。
- [ ] 验证飞书卡片展示效果和纯文本降级路径。
- [ ] 验证 GitHub Actions 手动触发可完成历史写回。
- [ ] 验证非周一 07:00 且未设置 `FORCE_RUN=1` 时脚本直接退出且不报错。

## 阶段 12：交付前复核

- [ ] 复查所有函数命名、注释和日志，确保可读性清晰。
- [ ] 复查所有外部请求均已集中在独立函数中，便于后续测试。
- [ ] 复查金额与百分比格式化是否都经过统一 util 函数。
- [ ] 复查历史文件结构是否满足需求文档约定。
- [ ] 复查 README、脚本、workflow 中的环境变量名称保持一致。
- [ ] 复查异常路径是否都能触发飞书失败通知。
