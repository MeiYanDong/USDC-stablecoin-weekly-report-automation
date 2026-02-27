from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests import Response, Session
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

REPORT_TZ = ZoneInfo("Asia/Shanghai")
REPORT_TIME_LABEL = "北京时间"
RUN_WEEKDAY = 0
RUN_HOUR = 7
RUN_MINUTE = 0
REQUEST_TIMEOUT_SECONDS = 20
HISTORY_PATH = Path("data/weekly_history.json")
ENV_PATH = Path(".env")
MAX_HISTORY_ITEMS = 52
CURRENCY_EYI_DIVISOR = 100_000_000

DEFI_LLAMA_STABLECOINS_URL = "https://stablecoins.llama.fi/stablecoins"
DEFI_LLAMA_CHART_URL = "https://stablecoins.llama.fi/stablecoincharts/all"
DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
DUNE_POLL_INTERVAL_SECONDS = 5
DUNE_MAX_POLLS = 120
DUNE_RESULT_PAGE_LIMIT = 1000

METRIC_SPECS = {
    "total_supply_usd": {
        "label": "全市场稳定币总供给",
        "kind": "currency",
    },
    "usdc_supply_usd": {
        "label": "USDC 供给",
        "kind": "currency",
    },
    "usdc_supply_share": {
        "label": "USDC 供给份额",
        "kind": "share",
    },
    "usdc_transfer_volume_share_7d": {
        "label": "USDC 过去 7 天链上转账量份额",
        "kind": "share",
    },
}

SYMBOL_COLUMN_CANDIDATES = (
    "symbol",
    "token_symbol",
    "asset",
    "stablecoin",
    "project",
)
VOLUME_COLUMN_CANDIDATES = (
    "volume_7d_usd",
    "volume_usd_7d",
    "transfer_volume_usd",
    "volume_usd",
    "amount_usd",
    "volume",
)


class WeeklyReportError(RuntimeError):
    """Raised when the weekly report cannot be completed safely."""


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def sanitize_text(text: str) -> str:
    sanitized = text
    for secret_name in ("DUNE_API_KEY", "FEISHU_WEBHOOK_URL"):
        secret_value = os.getenv(secret_name, "").strip()
        if secret_value:
            sanitized = sanitized.replace(secret_value, "***")
    return sanitized


def log(message: str) -> None:
    timestamp = datetime.now(tz=REPORT_TZ).isoformat(timespec="seconds")
    print(f"[{timestamp}] {sanitize_text(message)}", flush=True)


def get_report_now() -> datetime:
    return datetime.now(tz=REPORT_TZ)


def should_run_now(report_now: datetime, force_run: bool) -> bool:
    if force_run:
        return True
    return (
        report_now.weekday() == RUN_WEEKDAY
        and report_now.hour == RUN_HOUR
        and report_now.minute == RUN_MINUTE
    )


def get_report_window(report_now: datetime) -> tuple[date, date]:
    end_date = report_now.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    return start_date, end_date


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise WeeklyReportError(f"Missing required environment variable: {name}")
    return value


def get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def is_force_run_enabled() -> bool:
    return os.getenv("FORCE_RUN", "").strip() == "1"


def format_currency(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value / CURRENCY_EYI_DIVISOR:,.2f} 亿 USD"


def format_percentage(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def format_metric_value(metric_name: str, value: float | None) -> str:
    metric_kind = METRIC_SPECS[metric_name]["kind"]
    if metric_kind == "currency":
        return format_currency(value)
    return format_percentage(value)


def format_wow(metric_name: str, current: float | None, previous: float | None) -> str:
    if current is None or previous is None:
        return "N/A"

    metric_kind = METRIC_SPECS[metric_name]["kind"]
    if metric_kind == "share":
        delta_pp = (current - previous) * 100
        return f"{delta_pp:+.2f}pp"

    if previous == 0:
        return "N/A"

    change_ratio = (current - previous) / previous
    return f"{change_ratio:+.2%}"


def should_retry_request_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        status_code = response.status_code if response is not None else None
        return status_code == 429 or (status_code is not None and status_code >= 500)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


@retry(
    reraise=True,
    retry=retry_if_exception(should_retry_request_error),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
)
def perform_request(session: Session, method: str, url: str, **kwargs: Any) -> Response:
    response = session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
    response.raise_for_status()
    return response


def request_json(session: Session, method: str, url: str, **kwargs: Any) -> Any:
    response = perform_request(session, method, url, **kwargs)
    try:
        return response.json()
    except ValueError as exc:
        raise WeeklyReportError(f"Invalid JSON response from {url}") from exc


def request_feishu(session: Session, webhook_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = perform_request(session, "POST", webhook_url, json=payload)
    try:
        data = response.json()
    except ValueError as exc:
        raise WeeklyReportError("Feishu webhook returned non-JSON response") from exc

    response_code = data.get("code", data.get("StatusCode", 0))
    if response_code not in (0, "0", None):
        message = data.get("msg") or data.get("StatusMessage") or "unknown error"
        raise WeeklyReportError(f"Feishu webhook rejected request: {message}")
    return data


def fetch_defillama_stablecoins(session: Session) -> list[dict[str, Any]]:
    payload = request_json(session, "GET", DEFI_LLAMA_STABLECOINS_URL)
    pegged_assets = payload.get("peggedAssets")
    if not isinstance(pegged_assets, list):
        raise WeeklyReportError("Unexpected DefiLlama stablecoins response structure")
    return pegged_assets


def fetch_defillama_total_chart(session: Session) -> list[dict[str, Any]]:
    payload = request_json(session, "GET", DEFI_LLAMA_CHART_URL)
    if not isinstance(payload, list) or not payload:
        raise WeeklyReportError("Unexpected DefiLlama chart response structure")
    return payload


def get_pegged_usd_circulating(asset: dict[str, Any]) -> float:
    circulating = asset.get("circulating")
    if isinstance(circulating, dict):
        value = circulating.get("peggedUSD")
    else:
        value = circulating
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def build_top20_symbols_and_usdc_supply(pegged_assets: list[dict[str, Any]]) -> tuple[list[str], float]:
    usd_assets: list[tuple[str, float]] = []
    usdc_candidates: list[float] = []

    for asset in pegged_assets:
        if asset.get("pegType") != "peggedUSD":
            continue
        symbol = str(asset.get("symbol") or "").strip()
        if not symbol:
            continue
        circulating = get_pegged_usd_circulating(asset)
        usd_assets.append((symbol, circulating))
        if symbol.upper() == "USDC":
            usdc_candidates.append(circulating)

    if not usd_assets:
        raise WeeklyReportError("No peggedUSD assets found in DefiLlama response")
    if not usdc_candidates:
        raise WeeklyReportError("USDC supply not found in DefiLlama stablecoins list")

    sorted_assets = sorted(usd_assets, key=lambda item: item[1], reverse=True)
    top_symbols: list[str] = []
    seen_symbols: set[str] = set()
    for symbol, _ in sorted_assets:
        normalized_symbol = symbol.lower()
        if normalized_symbol in seen_symbols:
            continue
        seen_symbols.add(normalized_symbol)
        top_symbols.append(normalized_symbol)
        if len(top_symbols) == 20:
            break

    return top_symbols, max(usdc_candidates)


def get_total_supply_usd(chart_points: list[dict[str, Any]]) -> float:
    latest_point = chart_points[-1]
    total_circulating = latest_point.get("totalCirculating")
    if not isinstance(total_circulating, dict):
        raise WeeklyReportError("DefiLlama chart is missing totalCirculating data")

    value = total_circulating.get("peggedUSD")
    if value is None:
        raise WeeklyReportError("DefiLlama chart is missing peggedUSD total supply")

    total_supply_usd = float(value)
    if total_supply_usd <= 0:
        raise WeeklyReportError("DefiLlama total supply is not positive")
    return total_supply_usd


def get_dune_headers(api_key: str) -> dict[str, str]:
    return {
        "X-Dune-API-Key": api_key,
        "Content-Type": "application/json",
    }


def execute_dune_query(
    session: Session,
    api_key: str,
    query_id: int,
) -> str:
    payload = {
        "performance": "medium",
    }
    response = request_json(
        session,
        "POST",
        f"{DUNE_API_BASE_URL}/query/{query_id}/execute",
        headers=get_dune_headers(api_key),
        json=payload,
    )
    execution_id = response.get("execution_id")
    if not execution_id:
        raise WeeklyReportError("Dune execute response is missing execution_id")
    return str(execution_id)


def format_dune_error(status_payload: dict[str, Any]) -> str:
    error_payload = status_payload.get("error")
    if isinstance(error_payload, dict):
        error_message = error_payload.get("message")
        if error_message:
            return str(error_message)
    return f"Dune query execution failed with state {status_payload.get('state', 'unknown')}"


def wait_for_dune_execution(session: Session, api_key: str, execution_id: str) -> dict[str, Any]:
    status_url = f"{DUNE_API_BASE_URL}/execution/{execution_id}/status"
    for attempt in range(DUNE_MAX_POLLS):
        status_payload = request_json(
            session,
            "GET",
            status_url,
            headers=get_dune_headers(api_key),
        )
        state = str(status_payload.get("state") or "")
        if state in ("QUERY_STATE_COMPLETED", "QUERY_STATE_COMPLETED_PARTIAL"):
            return status_payload
        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELED", "QUERY_STATE_EXPIRED"):
            raise WeeklyReportError(format_dune_error(status_payload))
        if status_payload.get("is_execution_finished"):
            raise WeeklyReportError(f"Dune execution finished in unexpected state: {state or 'unknown'}")
        if attempt < DUNE_MAX_POLLS - 1:
            time.sleep(DUNE_POLL_INTERVAL_SECONDS)
    raise WeeklyReportError("Dune query execution timed out while polling for results")


def fetch_dune_result_rows(session: Session, api_key: str, execution_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    allow_partial_results = "true"

    while True:
        result_payload = request_json(
            session,
            "GET",
            f"{DUNE_API_BASE_URL}/execution/{execution_id}/results",
            headers=get_dune_headers(api_key),
            params={
                "offset": offset,
                "limit": DUNE_RESULT_PAGE_LIMIT,
                "allow_partial_results": allow_partial_results,
            },
        )
        result = result_payload.get("result")
        if not isinstance(result, dict):
            raise WeeklyReportError("Dune result payload is missing result rows")

        page_rows = result.get("rows")
        if not isinstance(page_rows, list):
            raise WeeklyReportError("Dune result payload has invalid rows structure")

        for row in page_rows:
            if isinstance(row, dict):
                rows.append(row)

        next_offset = result_payload.get("next_offset")
        if next_offset is None:
            break
        offset = int(next_offset)

    if not rows:
        raise WeeklyReportError("Dune query returned no rows")
    return rows


def get_row_value(row: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    lowered_row = {str(key).lower(): value for key, value in row.items()}
    for candidate in candidates:
        if candidate in lowered_row:
            return lowered_row[candidate]
    return None


def extract_dune_symbol_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for row in rows:
        raw_symbol = get_row_value(row, SYMBOL_COLUMN_CANDIDATES)
        if raw_symbol is None:
            continue
        symbol = str(raw_symbol).strip().lower()
        if not symbol:
            continue

        raw_volume = get_row_value(row, VOLUME_COLUMN_CANDIDATES)
        if raw_volume is None:
            continue
        try:
            volume = float(raw_volume)
        except (TypeError, ValueError):
            continue
        totals[symbol] = totals.get(symbol, 0.0) + volume

    if not totals:
        raise WeeklyReportError(
            "Dune query rows did not contain usable symbol and volume columns. "
            "Expected columns like symbol + volume_7d_usd."
        )
    return totals


def compute_dune_share(dune_symbol_totals: dict[str, float], symbols: list[str]) -> tuple[float | None, list[str], dict[str, float]]:
    missing_symbols: list[str] = []
    denominator_totals: dict[str, float] = {}

    for symbol in symbols:
        if symbol not in dune_symbol_totals:
            missing_symbols.append(symbol)
            continue
        denominator_totals[symbol] = dune_symbol_totals[symbol]

    usdc_total = denominator_totals.get("usdc")
    if usdc_total is None:
        return None, missing_symbols, denominator_totals

    denominator = sum(denominator_totals.values())
    if denominator <= 0:
        raise WeeklyReportError("Dune denominator volume is zero")
    return usdc_total / denominator, missing_symbols, denominator_totals


def load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        return []

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise WeeklyReportError(f"Invalid JSON in {path}") from exc

    if not isinstance(data, list):
        raise WeeklyReportError(f"History file must contain a JSON array: {path}")
    return [item for item in data if isinstance(item, dict)]


def save_history(path: Path, history: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    trimmed_history = history[-MAX_HISTORY_ITEMS:]
    path.write_text(
        json.dumps(trimmed_history, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def get_previous_metrics(history: list[dict[str, Any]]) -> dict[str, float] | None:
    if not history:
        return None
    metrics = history[-1].get("metrics")
    if not isinstance(metrics, dict):
        return None
    return metrics


def build_wow_map(current_metrics: dict[str, float | None], previous_metrics: dict[str, Any] | None) -> dict[str, str]:
    wow_map: dict[str, str] = {}
    for metric_name in METRIC_SPECS:
        previous_value = None
        if isinstance(previous_metrics, dict):
            raw_previous = previous_metrics.get(metric_name)
            if raw_previous is not None:
                try:
                    previous_value = float(raw_previous)
                except (TypeError, ValueError):
                    previous_value = None
        wow_map[metric_name] = format_wow(metric_name, current_metrics.get(metric_name), previous_value)
    return wow_map


def build_history_entry(
    run_time_report_tz: datetime,
    start_date: date,
    end_date: date,
    metrics: dict[str, float | None],
    missing_symbols: list[str],
) -> dict[str, Any]:
    return {
        "run_time_beijing": run_time_report_tz.isoformat(timespec="seconds"),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics": metrics,
        "missing_symbols": missing_symbols,
    }


def build_success_card_payload(
    run_time_report_tz: datetime,
    start_date: date,
    end_date: date,
    metrics: dict[str, float | None],
    wow_map: dict[str, str],
    missing_symbols: list[str],
) -> dict[str, Any]:
    metric_lines = []
    for metric_name, spec in METRIC_SPECS.items():
        metric_lines.append(
            f"**{spec['label']}**：{format_metric_value(metric_name, metrics.get(metric_name))}  \n"
            f"WoW：{wow_map[metric_name]}"
        )

    content = "\n\n".join(
        [
            f"**统计日期（{REPORT_TIME_LABEL}）**：{run_time_report_tz.date().isoformat()}",
            f"**统计区间（{REPORT_TIME_LABEL}）**：{start_date.isoformat()} 至 {end_date.isoformat()}",
            *metric_lines,
            f"**missing_symbols**：{', '.join(missing_symbols) if missing_symbols else 'none'}",
            "**数据源**：Supply 来自 DefiLlama；Transfer Volume 来自 Dune",
        ]
    )

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"USDC 周报 {end_date.isoformat()}",
                },
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": content,
                }
            ],
        },
    }


def build_failure_card_payload(
    run_time_report_tz: datetime,
    error_message: str,
    partial_metrics: dict[str, float | None],
    start_date: date | None,
    end_date: date | None,
    missing_symbols: list[str],
) -> dict[str, Any]:
    metric_lines = []
    for metric_name, spec in METRIC_SPECS.items():
        metric_lines.append(
            f"**{spec['label']}**：{format_metric_value(metric_name, partial_metrics.get(metric_name))}"
        )

    window_text = "N/A"
    if start_date and end_date:
        window_text = f"{start_date.isoformat()} 至 {end_date.isoformat()}"

    content = "\n\n".join(
        [
            f"**执行时间（{REPORT_TIME_LABEL}）**：{run_time_report_tz.isoformat(timespec='seconds')}",
            f"**统计区间（{REPORT_TIME_LABEL}）**：{window_text}",
            f"**失败原因**：{error_message}",
            *metric_lines,
            f"**missing_symbols**：{', '.join(missing_symbols) if missing_symbols else 'none'}",
        ]
    )

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "red",
                "title": {
                    "tag": "plain_text",
                    "content": "USDC 周报执行失败",
                },
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": content,
                }
            ],
        },
    }


def build_text_payload(text: str) -> dict[str, Any]:
    return {
        "msg_type": "text",
        "content": {
            "text": text,
        },
    }


def send_feishu_message(session: Session, webhook_url: str, payload: dict[str, Any], fallback_text: str) -> None:
    try:
        request_feishu(session, webhook_url, payload)
    except Exception as primary_error:
        log(f"Feishu card send failed, retrying as text: {primary_error}")
        request_feishu(session, webhook_url, build_text_payload(fallback_text))


def notify_failure(
    session: Session,
    webhook_url: str | None,
    run_time_report_tz: datetime,
    error_message: str,
    partial_metrics: dict[str, float | None],
    start_date: date | None,
    end_date: date | None,
    missing_symbols: list[str],
) -> None:
    if not webhook_url:
        log("FEISHU_WEBHOOK_URL is not set, skipping failure notification")
        return

    card_payload = build_failure_card_payload(
        run_time_report_tz=run_time_report_tz,
        error_message=error_message,
        partial_metrics=partial_metrics,
        start_date=start_date,
        end_date=end_date,
        missing_symbols=missing_symbols,
    )
    text_lines = [
        f"USDC 周报执行失败：{error_message}",
        f"执行时间（{REPORT_TIME_LABEL}）：{run_time_report_tz.isoformat(timespec='seconds')}",
    ]
    if start_date and end_date:
        text_lines.append(
            f"统计区间（{REPORT_TIME_LABEL}）：{start_date.isoformat()} 至 {end_date.isoformat()}"
        )
    for metric_name, spec in METRIC_SPECS.items():
        text_lines.append(
            f"{spec['label']}：{format_metric_value(metric_name, partial_metrics.get(metric_name))}"
        )
    text_lines.append(f"missing_symbols：{', '.join(missing_symbols) if missing_symbols else 'none'}")
    send_feishu_message(session, webhook_url, card_payload, "\n".join(text_lines))


def run_report() -> int:
    run_time_report_tz = get_report_now()
    force_run = is_force_run_enabled()
    if not should_run_now(run_time_report_tz, force_run):
        log("Current Beijing time is outside Monday 07:00 window. Exiting without work.")
        return 0

    partial_metrics: dict[str, float | None] = {
        "total_supply_usd": None,
        "usdc_supply_usd": None,
        "usdc_supply_share": None,
        "usdc_transfer_volume_share_7d": None,
    }
    missing_symbols: list[str] = []
    start_date: date | None = None
    end_date: date | None = None
    webhook_url = get_optional_env("FEISHU_WEBHOOK_URL")

    with requests.Session() as session:
        try:
            dune_api_key = get_required_env("DUNE_API_KEY")
            dune_query_id = int(get_required_env("DUNE_QUERY_ID"))
            webhook_url = get_required_env("FEISHU_WEBHOOK_URL")

            log("Fetching DefiLlama stablecoin list")
            pegged_assets = fetch_defillama_stablecoins(session)

            log("Fetching DefiLlama total supply chart")
            chart_points = fetch_defillama_total_chart(session)

            top_symbols, usdc_supply_usd = build_top20_symbols_and_usdc_supply(pegged_assets)
            total_supply_usd = get_total_supply_usd(chart_points)

            partial_metrics["total_supply_usd"] = total_supply_usd
            partial_metrics["usdc_supply_usd"] = usdc_supply_usd
            partial_metrics["usdc_supply_share"] = usdc_supply_usd / total_supply_usd

            start_date, end_date = get_report_window(run_time_report_tz)

            log("Executing Dune query for transfer volume share")
            execution_id = execute_dune_query(
                session,
                dune_api_key,
                dune_query_id,
            )
            wait_for_dune_execution(session, dune_api_key, execution_id)
            dune_rows = fetch_dune_result_rows(session, dune_api_key, execution_id)
            dune_symbol_totals = extract_dune_symbol_totals(dune_rows)

            volume_share, missing_symbols, _ = compute_dune_share(dune_symbol_totals, top_symbols)
            partial_metrics["usdc_transfer_volume_share_7d"] = volume_share

            if volume_share is None:
                raise WeeklyReportError("Dune query result did not include usable USDC transfer volume data")

            history = load_history(HISTORY_PATH)
            previous_metrics = get_previous_metrics(history)
            wow_map = build_wow_map(partial_metrics, previous_metrics)

            entry = build_history_entry(
                run_time_report_tz=run_time_report_tz,
                start_date=start_date,
                end_date=end_date,
                metrics=partial_metrics,
                missing_symbols=missing_symbols,
            )
            history.append(entry)
            save_history(HISTORY_PATH, history)

            card_payload = build_success_card_payload(
                run_time_report_tz=run_time_report_tz,
                start_date=start_date,
                end_date=end_date,
                metrics=partial_metrics,
                wow_map=wow_map,
                missing_symbols=missing_symbols,
            )
            text_lines = [
                f"USDC 周报 {end_date.isoformat()}",
                f"统计日期（{REPORT_TIME_LABEL}）：{run_time_report_tz.date().isoformat()}",
                f"统计区间（{REPORT_TIME_LABEL}）：{start_date.isoformat()} 至 {end_date.isoformat()}",
            ]
            for metric_name, spec in METRIC_SPECS.items():
                text_lines.append(
                    f"{spec['label']}：{format_metric_value(metric_name, partial_metrics.get(metric_name))} | WoW {wow_map[metric_name]}"
                )
            text_lines.append(f"missing_symbols：{', '.join(missing_symbols) if missing_symbols else 'none'}")
            text_lines.append("数据源：Supply 来自 DefiLlama；Transfer Volume 来自 Dune")
            send_feishu_message(session, webhook_url, card_payload, "\n".join(text_lines))

            log("Weekly report completed successfully")
            return 0
        except Exception as exc:
            error_message = str(exc)
            log(f"Weekly report failed: {error_message}")
            log(traceback.format_exc())
            log(f"Partial metrics snapshot: {json.dumps(partial_metrics, ensure_ascii=False)}")
            log(f"Missing symbols snapshot: {', '.join(missing_symbols) if missing_symbols else 'none'}")
            try:
                notify_failure(
                    session=session,
                    webhook_url=webhook_url,
                    run_time_report_tz=run_time_report_tz,
                    error_message=error_message,
                    partial_metrics=partial_metrics,
                    start_date=start_date,
                    end_date=end_date,
                    missing_symbols=missing_symbols,
                )
            except Exception as notify_exc:
                log(f"Failed to send failure notification: {notify_exc}")
                log(traceback.format_exc())
            return 1


def main() -> None:
    load_dotenv_file(ENV_PATH)
    sys.exit(run_report())


if __name__ == "__main__":
    main()
