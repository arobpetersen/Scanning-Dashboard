from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import requests

from .config import (
    AIRTABLE_API_KEY_ENV,
    AIRTABLE_BASE_ID_ENV,
    AIRTABLE_EXPORT_SNAPSHOT_LIMIT,
    AIRTABLE_TABLE_THEME_SNAPSHOT_HISTORY,
    AIRTABLE_TABLE_THEMES,
    AIRTABLE_TABLE_TICKER_SNAPSHOT_HISTORY,
    AIRTABLE_TABLE_TICKERS,
)
from .queries import theme_snapshot_history_recent, themes_dimension, ticker_snapshot_history_recent, tickers_dimension


THEME_HISTORY_KEY_FIELD = "export_key"
TICKER_HISTORY_KEY_FIELD = "export_key"
THEMES_KEY_FIELD = "theme_id"
TICKERS_KEY_FIELD = "ticker"
AIRTABLE_BATCH_SIZE = 10
AIRTABLE_LOOKUP_BATCH_SIZE = 10


FIELD_SPECS = {
    "themes": [
        {"name": "theme_id", "type": "number"},
        {"name": "theme_name", "type": "singleLineText"},
        {"name": "category", "type": "singleLineText"},
        {"name": "is_active", "type": "checkbox"},
    ],
    "theme_snapshot_history": [
        {"name": "export_key", "type": "singleLineText"},
        {"name": "theme_id", "type": "number"},
        {"name": "snapshot_time", "type": "dateTime"},
        {"name": "run_id", "type": "number"},
        {"name": "ticker_count", "type": "number"},
        {"name": "avg_1w", "type": "number"},
        {"name": "avg_1m", "type": "number"},
        {"name": "avg_3m", "type": "number"},
        {"name": "positive_1w_breadth_pct", "type": "percent"},
        {"name": "positive_1m_breadth_pct", "type": "percent"},
        {"name": "positive_3m_breadth_pct", "type": "percent"},
        {"name": "composite_score", "type": "number"},
        {"name": "snapshot_source", "type": "singleLineText"},
    ],
    "tickers": [
        {"name": "ticker", "type": "singleLineText"},
        {"name": "latest_market_cap", "type": "currencyOrNumber"},
        {"name": "latest_avg_volume", "type": "number"},
        {"name": "latest_last_updated", "type": "dateTime"},
        {"name": "latest_snapshot_time", "type": "dateTime"},
    ],
    "ticker_snapshot_history": [
        {"name": "export_key", "type": "singleLineText"},
        {"name": "ticker", "type": "singleLineText"},
        {"name": "snapshot_time", "type": "dateTime"},
        {"name": "run_id", "type": "number"},
        {"name": "price", "type": "currencyOrNumber"},
        {"name": "perf_1w", "type": "number"},
        {"name": "perf_1m", "type": "number"},
        {"name": "perf_3m", "type": "number"},
        {"name": "market_cap", "type": "currencyOrNumber"},
        {"name": "avg_volume", "type": "number"},
        {"name": "last_updated", "type": "dateTime"},
        {"name": "snapshot_source", "type": "singleLineText"},
    ],
}


def theme_history_export_key(theme_id: int, run_id: int) -> str:
    return f"theme:{int(theme_id)}:run:{int(run_id)}"


def ticker_history_export_key(ticker: str, run_id: int) -> str:
    return f"ticker:{str(ticker).strip().upper()}:run:{int(run_id)}"


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
        return str(value)
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _records_from_dataframe(df: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    records: list[dict[str, Any]] = []
    for row in df[columns].to_dict(orient="records"):
        records.append({col: _clean_value(value) for col, value in row.items()})
    return records


def build_theme_dimension_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return _records_from_dataframe(df, ["theme_id", "theme_name", "category", "is_active"])


def build_theme_snapshot_history_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    payload = df.copy()
    payload["export_key"] = payload.apply(lambda row: theme_history_export_key(row["theme_id"], row["run_id"]), axis=1)
    return _records_from_dataframe(
        payload,
        [
            "export_key",
            "theme_id",
            "snapshot_time",
            "run_id",
            "ticker_count",
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "positive_1w_breadth_pct",
            "positive_1m_breadth_pct",
            "positive_3m_breadth_pct",
            "composite_score",
            "snapshot_source",
        ],
    )


def build_ticker_dimension_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return _records_from_dataframe(
        df,
        ["ticker", "latest_market_cap", "latest_avg_volume", "latest_last_updated", "latest_snapshot_time"],
    )


def build_ticker_snapshot_history_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    payload = df.copy()
    payload["export_key"] = payload.apply(lambda row: ticker_history_export_key(row["ticker"], row["run_id"]), axis=1)
    return _records_from_dataframe(
        payload,
        [
            "export_key",
            "ticker",
            "snapshot_time",
            "run_id",
            "price",
            "perf_1w",
            "perf_1m",
            "perf_3m",
            "market_cap",
            "avg_volume",
            "last_updated",
            "snapshot_source",
        ],
    )


def build_airtable_export_payloads(conn, snapshot_limit: int = AIRTABLE_EXPORT_SNAPSHOT_LIMIT) -> dict[str, list[dict[str, Any]]]:
    return {
        "themes": build_theme_dimension_records(themes_dimension(conn)),
        "theme_snapshot_history": build_theme_snapshot_history_records(theme_snapshot_history_recent(conn, snapshot_limit=snapshot_limit)),
        "tickers": build_ticker_dimension_records(tickers_dimension(conn)),
        "ticker_snapshot_history": build_ticker_snapshot_history_records(ticker_snapshot_history_recent(conn, snapshot_limit=snapshot_limit)),
    }


def split_records_for_upsert(
    records: list[dict[str, Any]],
    existing_by_key: dict[str, str],
    key_field: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    creates: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    for fields in records:
        key = str(fields[key_field])
        existing_id = existing_by_key.get(key)
        if existing_id:
            updates.append({"id": existing_id, "fields": fields})
        else:
            creates.append(fields)
    return creates, updates


def chunk_records(records: list[dict[str, Any]], chunk_size: int = AIRTABLE_BATCH_SIZE) -> list[list[dict[str, Any]]]:
    return [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]


@dataclass(frozen=True)
class AirtableTableConfig:
    name: str
    key_field: str


TABLE_CONFIGS = {
    "themes": AirtableTableConfig(name=AIRTABLE_TABLE_THEMES, key_field=THEMES_KEY_FIELD),
    "theme_snapshot_history": AirtableTableConfig(name=AIRTABLE_TABLE_THEME_SNAPSHOT_HISTORY, key_field=THEME_HISTORY_KEY_FIELD),
    "tickers": AirtableTableConfig(name=AIRTABLE_TABLE_TICKERS, key_field=TICKERS_KEY_FIELD),
    "ticker_snapshot_history": AirtableTableConfig(name=AIRTABLE_TABLE_TICKER_SNAPSHOT_HISTORY, key_field=TICKER_HISTORY_KEY_FIELD),
}


class AirtableClient:
    def __init__(self, api_key: str, base_id: str, timeout_s: int = 30):
        self.base_id = base_id
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})

    def _url(self, table_name: str) -> str:
        return f"https://api.airtable.com/v0/{self.base_id}/{requests.utils.quote(table_name, safe='')}"

    @staticmethod
    def _raise_for_response(response, context: str) -> None:
        if response.ok:
            return
        status = response.status_code
        body_text = response.text.strip()
        try:
            payload = response.json()
            error_info = payload.get("error", payload)
        except Exception:
            error_info = body_text or "unknown Airtable error"

        if status == 401:
            raise RuntimeError(f"{context}: Airtable rejected the token (401). Check AIRTABLE_API_KEY.")
        if status == 403:
            raise RuntimeError(
                f"{context}: Airtable denied access (403). Check PAT scopes and that the token has access to the target base."
            )
        if status == 404:
            raise RuntimeError(f"{context}: Airtable resource not found (404). Check AIRTABLE_BASE_ID and table names. Detail: {error_info}")
        raise RuntimeError(f"{context}: Airtable request failed with HTTP {status}. Detail: {error_info}")

    def _request(self, method: str, table_name: str, **kwargs):
        response = self.session.request(method, self._url(table_name), timeout=self.timeout_s, **kwargs)
        self._raise_for_response(response, f"table request [{table_name}]")
        return response.json()

    def _metadata_request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, f"https://api.airtable.com/v0/meta/{path.lstrip('/')}", timeout=self.timeout_s, **kwargs)
        self._raise_for_response(response, f"metadata request [{path}]")
        return response.json()

    @staticmethod
    def _formula_for_keys(key_field: str, keys: list[str]) -> str:
        terms = []
        for key in keys:
            safe = str(key).replace("\\", "\\\\").replace("'", "\\'")
            terms.append(f"{{{key_field}}}='{safe}'")
        return terms[0] if len(terms) == 1 else f"OR({','.join(terms)})"

    def find_existing_by_key(self, table_name: str, key_field: str, keys: list[str]) -> dict[str, str]:
        found: dict[str, str] = {}
        pending = [str(key) for key in keys if str(key).strip()]
        for batch in chunk_records([{"key": key} for key in pending], chunk_size=AIRTABLE_LOOKUP_BATCH_SIZE):
            batch_keys = [item["key"] for item in batch]
            formula = self._formula_for_keys(key_field, batch_keys)
            offset = None
            while True:
                params = {"filterByFormula": formula}
                if offset:
                    params["offset"] = offset
                payload = self._request("GET", table_name, params=params)
                for record in payload.get("records", []):
                    fields = record.get("fields", {})
                    if key_field in fields and fields[key_field] is not None:
                        found[str(fields[key_field])] = str(record["id"])
                offset = payload.get("offset")
                if not offset:
                    break
        return found

    def create_records(self, table_name: str, records: list[dict[str, Any]]) -> int:
        created = 0
        for batch in chunk_records(records, chunk_size=AIRTABLE_BATCH_SIZE):
            self._request("POST", table_name, json={"records": [{"fields": item} for item in batch]})
            created += len(batch)
        return created

    def update_records(self, table_name: str, records: list[dict[str, Any]]) -> int:
        updated = 0
        for batch in chunk_records(records, chunk_size=AIRTABLE_BATCH_SIZE):
            self._request("PATCH", table_name, json={"records": batch})
            updated += len(batch)
        return updated

    def get_base_schema(self) -> dict[str, Any]:
        return self._metadata_request("GET", f"bases/{self.base_id}/tables")


def plan_export_actions(
    payloads: dict[str, list[dict[str, Any]]],
    existing_keys_by_dataset: dict[str, dict[str, str]] | None = None,
) -> dict[str, dict[str, Any]]:
    existing_keys_by_dataset = existing_keys_by_dataset or {}
    plan: dict[str, dict[str, Any]] = {}
    for dataset_name, records in payloads.items():
        config = TABLE_CONFIGS[dataset_name]
        existing_by_key = existing_keys_by_dataset.get(dataset_name, {})
        creates, updates = split_records_for_upsert(records, existing_by_key, config.key_field)
        plan[dataset_name] = {
            "table_name": config.name,
            "key_field": config.key_field,
            "records": records,
            "create_records": creates,
            "update_records": updates,
            "create_count": len(creates),
            "update_count": len(updates),
            "total_records": len(records),
        }
    return plan


def export_to_airtable(
    payloads: dict[str, list[dict[str, Any]]],
    client: AirtableClient | None = None,
    dry_run: bool = True,
) -> dict[str, dict[str, Any]]:
    existing_by_dataset: dict[str, dict[str, str]] = {}
    if client is not None:
        for dataset_name, records in payloads.items():
            config = TABLE_CONFIGS[dataset_name]
            keys = [str(record[config.key_field]) for record in records]
            existing_by_dataset[dataset_name] = client.find_existing_by_key(config.name, config.key_field, keys) if keys else {}

    plan = plan_export_actions(payloads, existing_keys_by_dataset=existing_by_dataset)
    if dry_run or client is None:
        return plan

    for dataset_name, details in plan.items():
        config = TABLE_CONFIGS[dataset_name]
        created = client.create_records(config.name, details["create_records"])
        updated = client.update_records(config.name, details["update_records"])
        details["created"] = created
        details["updated"] = updated
    return plan


def preview_payloads(payloads: dict[str, list[dict[str, Any]]], preview_rows: int = 2) -> dict[str, list[dict[str, Any]]]:
    return {dataset_name: records[:preview_rows] for dataset_name, records in payloads.items()}


def summarize_plan(plan: dict[str, dict[str, Any]]) -> dict[str, Any]:
    datasets = {
        dataset_name: {
            "table_name": details["table_name"],
            "key_field": details["key_field"],
            "total_records": details["total_records"],
            "create_count": details["create_count"],
            "update_count": details["update_count"],
        }
        for dataset_name, details in plan.items()
    }
    return {
        "datasets": datasets,
        "total_records": sum(item["total_records"] for item in datasets.values()),
        "total_creates": sum(item["create_count"] for item in datasets.values()),
        "total_updates": sum(item["update_count"] for item in datasets.values()),
    }


def print_plan_summary(plan: dict[str, dict[str, Any]], preview_rows: int = 0) -> None:
    summary = summarize_plan(plan)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if preview_rows <= 0:
        return
    payload_preview = preview_payloads({name: details["records"] for name, details in plan.items()}, preview_rows=preview_rows)
    print(json.dumps(payload_preview, indent=2, sort_keys=True))


def validate_airtable_config(api_key: str | None, base_id: str | None) -> None:
    if not api_key:
        raise RuntimeError(f"Missing Airtable API key. Set {AIRTABLE_API_KEY_ENV}.")
    if not base_id:
        raise RuntimeError(f"Missing Airtable base id. Set {AIRTABLE_BASE_ID_ENV}.")


def expected_airtable_schema() -> dict[str, dict[str, Any]]:
    return {
        dataset_name: {
            "table_name": config.name,
            "key_field": config.key_field,
            "fields": FIELD_SPECS[dataset_name],
        }
        for dataset_name, config in TABLE_CONFIGS.items()
    }


def validate_airtable_schema(client: AirtableClient) -> dict[str, Any]:
    schema = client.get_base_schema()
    tables = schema.get("tables", [])
    tables_by_name = {str(table.get("name")): table for table in tables}
    validation: dict[str, Any] = {"ok": True, "datasets": {}}

    for dataset_name, spec in expected_airtable_schema().items():
        table_name = spec["table_name"]
        table = tables_by_name.get(table_name)
        if not table:
            validation["ok"] = False
            validation["datasets"][dataset_name] = {
                "table_name": table_name,
                "missing_table": True,
                "missing_fields": [field["name"] for field in spec["fields"]],
            }
            continue

        actual_fields = {str(field.get("name")) for field in table.get("fields", [])}
        expected_fields = [field["name"] for field in spec["fields"]]
        missing_fields = [field_name for field_name in expected_fields if field_name not in actual_fields]
        if missing_fields:
            validation["ok"] = False
        validation["datasets"][dataset_name] = {
            "table_name": table_name,
            "missing_table": False,
            "missing_fields": missing_fields,
        }

    return validation


def ensure_airtable_schema(client: AirtableClient) -> dict[str, Any]:
    validation = validate_airtable_schema(client)
    if validation["ok"]:
        return validation

    missing_tables = [details["table_name"] for details in validation["datasets"].values() if details["missing_table"]]
    missing_fields = {
        details["table_name"]: details["missing_fields"]
        for details in validation["datasets"].values()
        if details["missing_fields"]
    }

    parts = []
    if missing_tables:
        parts.append(f"missing tables: {', '.join(sorted(missing_tables))}")
    if missing_fields:
        field_parts = [f"{table} -> {', '.join(fields)}" for table, fields in sorted(missing_fields.items())]
        parts.append(f"missing fields: {'; '.join(field_parts)}")
    raise RuntimeError(
        "Airtable schema preflight failed. Pre-create the expected tables/fields in the target base before first write. "
        + " | ".join(parts)
    )
