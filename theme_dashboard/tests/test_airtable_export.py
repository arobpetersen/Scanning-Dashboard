import unittest

import duckdb

from src.airtable_export import (
    build_theme_snapshot_history_records,
    build_ticker_dimension_records,
    ensure_airtable_schema,
    expected_airtable_schema,
    plan_export_actions,
    split_records_for_upsert,
    theme_history_export_key,
    ticker_history_export_key,
)
from src.queries import theme_snapshot_history_recent, ticker_snapshot_history_recent


class TestAirtableExportKeys(unittest.TestCase):
    def test_export_keys_are_deterministic(self):
        self.assertEqual(theme_history_export_key(12, 34), "theme:12:run:34")
        self.assertEqual(ticker_history_export_key(" nvda ", 56), "ticker:NVDA:run:56")


class TestAirtableExportQueries(unittest.TestCase):
    def test_recent_history_queries_are_bounded_per_entity(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1w_breadth_pct double,
                positive_1m_breadth_pct double,
                positive_3m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint,
                ticker varchar,
                price double,
                perf_1w double,
                perf_1m double,
                perf_3m double,
                market_cap double,
                avg_volume double,
                short_interest_pct double,
                float_shares double,
                adr_pct double,
                last_updated timestamp,
                snapshot_source varchar
            )
            """
        )

        for run_id in range(1, 6):
            ts = f"2026-03-0{run_id} 22:00:00"
            conn.execute("insert into refresh_runs values (?, 'success', ?)", [run_id, ts])
            for theme_id in (1, 2):
                conn.execute(
                    "insert into theme_snapshots values (?, ?, ?, 10, 1, 2, 3, 40, 50, 60, ?, 'live')",
                    [run_id, ts, theme_id, run_id],
                )
            for ticker in ("AAA", "BBB"):
                conn.execute(
                    "insert into ticker_snapshots values (?, ?, 10, 1, 2, 3, 1000, 2000, null, null, null, ?, 'live')",
                    [run_id, ticker, ts],
                )

        theme_out = theme_snapshot_history_recent(conn, snapshot_limit=2)
        ticker_out = ticker_snapshot_history_recent(conn, snapshot_limit=3)

        self.assertEqual(theme_out.groupby("theme_id").size().to_dict(), {1: 2, 2: 2})
        self.assertEqual(ticker_out.groupby("ticker").size().to_dict(), {"AAA": 3, "BBB": 3})
        self.assertEqual(theme_out.iloc[0]["run_id"], 5)
        self.assertEqual(ticker_out.iloc[0]["run_id"], 5)
        conn.close()


class TestAirtableExportPayloads(unittest.TestCase):
    def test_theme_history_payload_includes_export_key(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1w_breadth_pct double,
                positive_1m_breadth_pct double,
                positive_3m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        df = conn.execute(
            """
            select 7 as run_id,
                   timestamp '2026-03-11 22:00:00' as snapshot_time,
                   3 as theme_id,
                   12 as ticker_count,
                   1.1 as avg_1w,
                   2.2 as avg_1m,
                   3.3 as avg_3m,
                   55.0 as positive_1w_breadth_pct,
                   60.0 as positive_1m_breadth_pct,
                   65.0 as positive_3m_breadth_pct,
                   4.4 as composite_score,
                   'live' as snapshot_source
            """
        ).df()
        records = build_theme_snapshot_history_records(df)
        self.assertEqual(records[0]["export_key"], "theme:3:run:7")
        self.assertEqual(records[0]["snapshot_source"], "live")
        conn.close()

    def test_ticker_dimension_payload_preserves_nulls_and_timestamps(self):
        conn = duckdb.connect(":memory:")
        df = conn.execute(
            """
            select 'NVDA' as ticker,
                   125900000000.0 as latest_market_cap,
                   55825862.0 as latest_avg_volume,
                   timestamp '2026-03-11 21:00:00' as latest_last_updated,
                   timestamp '2026-03-11 22:00:00' as latest_snapshot_time
            """
        ).df()
        records = build_ticker_dimension_records(df)
        self.assertEqual(records[0]["ticker"], "NVDA")
        self.assertEqual(records[0]["latest_market_cap"], 125900000000.0)
        self.assertIn("2026-03-11T21:00:00", records[0]["latest_last_updated"])
        conn.close()


class TestAirtableExportUpsertPlanning(unittest.TestCase):
    def test_split_records_for_upsert_prevents_duplicate_append_plan(self):
        records = [
            {"export_key": "theme:1:run:10", "theme_id": 1},
            {"export_key": "theme:1:run:11", "theme_id": 1},
        ]
        creates, updates = split_records_for_upsert(records, {"theme:1:run:10": "recA"}, "export_key")
        self.assertEqual(len(creates), 1)
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["id"], "recA")

    def test_plan_export_actions_counts_updates_and_creates(self):
        payloads = {
            "themes": [{"theme_id": 1, "theme_name": "AI", "category": "Tech", "is_active": True}],
            "theme_snapshot_history": [{"export_key": "theme:1:run:10", "theme_id": 1, "run_id": 10}],
            "tickers": [{"ticker": "NVDA", "latest_market_cap": 1.0, "latest_avg_volume": 2.0, "latest_last_updated": None, "latest_snapshot_time": None}],
            "ticker_snapshot_history": [{"export_key": "ticker:NVDA:run:10", "ticker": "NVDA", "run_id": 10}],
        }
        plan = plan_export_actions(
            payloads,
            existing_keys_by_dataset={
                "themes": {"1": "recTheme"},
                "ticker_snapshot_history": {"ticker:NVDA:run:10": "recTickerHist"},
            },
        )
        self.assertEqual(plan["themes"]["update_count"], 1)
        self.assertEqual(plan["themes"]["create_count"], 0)
        self.assertEqual(plan["theme_snapshot_history"]["create_count"], 1)
        self.assertEqual(plan["ticker_snapshot_history"]["update_count"], 1)


class TestAirtableSchemaValidation(unittest.TestCase):
    def test_expected_schema_declares_all_tables(self):
        schema = expected_airtable_schema()
        self.assertEqual(set(schema.keys()), {"themes", "theme_snapshot_history", "tickers", "ticker_snapshot_history"})
        self.assertEqual(schema["themes"]["table_name"], "Themes")
        self.assertEqual(schema["theme_snapshot_history"]["key_field"], "export_key")

    def test_ensure_airtable_schema_raises_for_missing_table_or_fields(self):
        class FakeClient:
            def get_base_schema(self):
                return {
                    "tables": [
                        {
                            "name": "Themes",
                            "fields": [
                                {"name": "theme_id"},
                                {"name": "theme_name"},
                            ],
                        },
                        {
                            "name": "Tickers",
                            "fields": [
                                {"name": "ticker"},
                                {"name": "latest_market_cap"},
                                {"name": "latest_avg_volume"},
                                {"name": "latest_last_updated"},
                                {"name": "latest_snapshot_time"},
                            ],
                        },
                    ]
                }

        with self.assertRaisesRegex(RuntimeError, "missing tables: Theme Snapshot History, Ticker Snapshot History"):
            ensure_airtable_schema(FakeClient())

    def test_ensure_airtable_schema_passes_when_expected_tables_and_fields_exist(self):
        expected = expected_airtable_schema()

        class FakeClient:
            def get_base_schema(self):
                return {
                    "tables": [
                        {
                            "name": spec["table_name"],
                            "fields": [{"name": field["name"]} for field in spec["fields"]],
                        }
                        for spec in expected.values()
                    ]
                }

        result = ensure_airtable_schema(FakeClient())
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
