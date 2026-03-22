from __future__ import annotations

import unittest
from unittest.mock import patch

import duckdb

from src.database import SCHEMA_SQL
from src.scanner_research_cache import _PROFILE_CACHE
from src.scanner_research_analysis import (
    build_candidate_analysis,
    candidate_analysis,
    preprocessed_theme_entry,
)
from src.scanner_research_heuristics import (
    fit_label_from_details,
    truncate_existing_theme_suggestions,
)
from src.scanner_research_merge import (
    ai_research_draft_for_strategy,
    best_suggested_theme_fit_details,
    merge_ai_with_heuristic_draft,
    normalize_ai_draft_payload,
)
from src.scanner_research_models import ResearchDraft
from src.scanner_research_profiles import load_company_profile_with_cache, theme_catalog_context
from src.scanner_research_service import (
    generate_research_draft,
    get_or_create_research_draft,
    load_research_review,
    load_research_review_summary,
    persist_research_review,
)


class TestResearchDraftContract(unittest.TestCase):
    def test_research_draft_normalizes_and_separates_payload_groups(self):
        draft = ResearchDraft.from_mapping(
            {
                "ticker": " nvda ",
                "company_name": " NVIDIA ",
                "possible_similar_tickers": [" amd ", "", "smci"],
                "suggested_existing_themes": [{"theme_id": 1, "theme_name": "AI - Infrastructure"}],
                "possible_new_theme": " AI Server Systems ",
                "confidence": " high ",
                "recommended_action": " consider_new_theme ",
                "generated_at": "2026-03-22 10:00:00",
                "research_mode": "openai",
                "domain_anchor": "accelerated computing",
                "candidate_theme_ideas": ["AI Server Systems", "Data Center Power"],
                "validation_debug": {"strategy": "description_theme_generation"},
                "draft_source": "fresh_generation",
            }
        )

        self.assertEqual(draft.ticker, "NVDA")
        self.assertEqual(draft.possible_similar_tickers, ["AMD", "SMCI"])
        self.assertEqual(
            draft.domain_payload()["possible_new_theme"],
            "AI Server Systems",
        )
        self.assertEqual(
            draft.workflow_payload()["research_mode"],
            "openai",
        )
        self.assertEqual(
            draft.debug_payload()["domain_anchor"],
            "accelerated computing",
        )
        self.assertEqual(
            draft.ui_payload()["draft_source"],
            "fresh_generation",
        )
        self.assertEqual(
            draft.to_dict()["theme_generation_strategy"],
            "description_theme_generation",
        )

    def test_heuristic_primitives_preserve_fit_labels_and_truncation(self):
        self.assertEqual(
            fit_label_from_details({"score": 18, "direct_role_fit": True}),
            "direct_fit",
        )
        self.assertEqual(
            fit_label_from_details({"score": 8, "indirect_only_fit": True}),
            "adjacent_fit",
        )
        self.assertEqual(
            truncate_existing_theme_suggestions([{"theme_id": 1}, {"theme_id": 2}, {"theme_id": 3}, {"theme_id": 4}], limit=2),
            [{"theme_id": 1}, {"theme_id": 2}],
        )


class TestScannerResearchService(unittest.TestCase):
    def test_generate_research_draft_records_fallback_metadata_in_contract(self):
        baseline_payload = {
            "ticker": "NVDA",
            "company_name": "NVIDIA",
            "short_company_description": "Accelerated computing company.",
            "suggested_existing_themes": [{"theme_id": 1, "theme_name": "AI - Infrastructure"}],
            "recommended_action": "watch_only",
            "confidence": "medium",
            "research_timing_summary": {"baseline_total_ms": 5},
        }

        with (
            patch("src.scanner_research_service.candidate_context", return_value={"ticker": "NVDA"}),
            patch("src.scanner_research_service.theme_catalog_context", return_value=[{"theme_id": 1, "theme_name": "AI - Infrastructure", "category": "AI"}]),
            patch("src.scanner_research_service.preprocessed_catalog", side_effect=lambda catalog: catalog),
            patch("src.scanner_research_service.load_company_profile_with_cache", return_value={"company_name": "NVIDIA", "description": "Accelerated computing company."}),
            patch("src.scanner_research_service.ai_research_draft_for_strategy", side_effect=RuntimeError("HTTP 429 rate limit")),
            patch("src.scanner_research_service.baseline_research_draft", return_value=baseline_payload),
            patch("src.scanner_research._extract_openai_error_details", return_value={"message": "HTTP 429 rate limit"}),
            patch("src.scanner_research._format_openai_error_summary", return_value="HTTP 429 rate limit"),
        ):
            draft = generate_research_draft(object(), "NVDA")

        self.assertIsInstance(draft, ResearchDraft)
        self.assertEqual(draft.ticker, "NVDA")
        self.assertEqual(draft.research_mode, "heuristic_fallback")
        self.assertEqual(draft.fallback_reason, "HTTP 429 rate limit")
        self.assertEqual(draft.research_error, {"message": "HTTP 429 rate limit"})
        self.assertEqual(draft.theme_generation_strategy, "description_theme_generation")
        self.assertIn("total_ms", draft.research_timing_summary)

    def test_get_or_create_research_draft_reuses_existing_draft_and_returns_page_facing_feedback(self):
        existing = ResearchDraft.from_mapping(
            {
                "ticker": "AAOI",
                "generated_at": "2026-03-22 10:00:00",
                "theme_generation_strategy": "description_theme_generation",
            }
        )

        result = get_or_create_research_draft(
            object(),
            "AAOI",
            existing_draft=existing,
            force_refresh=False,
        )

        self.assertTrue(result.reused)
        self.assertEqual(result.draft_source, "reused_session_draft")
        self.assertEqual(result.draft.draft_source, "reused_session_draft")
        self.assertIn("Reused existing advisory research draft", result.feedback_message)

    def test_research_review_service_accepts_research_draft_and_returns_summary(self):
        conn = duckdb.connect(":memory:")
        draft = ResearchDraft.from_mapping(
            {
                "ticker": "PLTR",
                "generated_at": "2026-03-22 12:00:00",
                "theme_generation_strategy": "description_theme_generation",
                "research_mode": "heuristic_fallback",
                "recommended_action": "consider_new_theme",
                "confidence": "medium",
                "possible_new_theme": "Defense Software",
                "domain_anchor": "government software",
                "dominant_business_role": "software_tooling",
                "candidate_theme_ideas": ["Defense Software"],
            }
        )

        saved = persist_research_review(
            conn,
            "PLTR",
            draft,
            outcome_class="direct_fit_correct",
            reviewer_notes="Looks right.",
        )
        loaded = load_research_review(conn, "PLTR", draft)
        summary = load_research_review_summary(conn, limit=5)

        self.assertEqual(saved["outcome_class"], "direct_fit_correct")
        self.assertEqual(loaded["ticker"], "PLTR")
        self.assertEqual(summary["counts_by_outcome"]["direct_fit_correct"], 1)
        self.assertEqual(summary["recent_reviews"][0]["ticker"], "PLTR")


class TestExtractedResearchHelpers(unittest.TestCase):
    def setUp(self):
        _PROFILE_CACHE.clear()

    def tearDown(self):
        _PROFILE_CACHE.clear()

    def test_preprocessed_theme_entry_adds_expected_analysis_fields(self):
        entry = preprocessed_theme_entry(
            {
                "theme_id": 7,
                "theme_name": "Optical Networking",
                "category": "AI - Infrastructure",
                "theme_description": "Optical networking systems and fiber interconnects.",
                "representative_tickers": ["AAOI", "CIEN"],
            }
        )

        self.assertIn("_theme_tokens", entry)
        self.assertIn("_theme_roles", entry)
        self.assertIn("optical_networking", set(entry.get("_theme_roles") or set()))

    def test_candidate_analysis_reuses_cached_result_for_same_inputs(self):
        profile = {
            "company_name": "Applied Optoelectronics",
            "description": "Designs optical networking components and transceivers for data center connectivity.",
            "sic_description": "Communications equipment",
        }
        candidate = {
            "ticker": "AAOI",
            "recommendation_reason": "Persistent uncovered optical networking candidate",
        }

        first = candidate_analysis(profile, candidate)
        second = candidate_analysis(profile, candidate)

        self.assertIs(first, second)
        self.assertIn("optical_networking", set(first.get("candidate_roles") or set()))
        rebuilt = build_candidate_analysis(profile, candidate)
        self.assertEqual(first["dominant_economic_role"], rebuilt["dominant_economic_role"])

    def test_theme_catalog_context_and_profile_cache_helpers_preserve_current_behavior(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI - Infrastructure', 'AI', TRUE)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA'), (1, 'SMCI')")

        catalog = theme_catalog_context(conn)

        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["theme_name"], "AI - Infrastructure")
        self.assertEqual(catalog[0]["representative_tickers"], ["NVDA", "SMCI"])

        with (
            patch("src.scanner_research_profiles.load_company_profile", side_effect=[{"company_name": "NVIDIA", "description": "GPU maker"}, {}]),
        ):
            fresh = load_company_profile_with_cache("NVDA")
            cached = load_company_profile_with_cache("NVDA")

        self.assertEqual(fresh["_profile_source"], "live_lookup")
        self.assertEqual(cached["_profile_source"], "cached_live_lookup")
        self.assertEqual(cached["company_name"], "NVIDIA")

    def test_normalize_ai_draft_payload_repairs_sparse_ai_output(self):
        payload = normalize_ai_draft_payload(
            {
                "suggested_existing_themes": [{"theme_id": 1, "why_it_might_fit": "AI servers"}],
                "recommended_action": "bad_value",
                "caveats": ["", "Needs review"],
            },
            candidate={"ticker": "SMCI"},
            profile={"company_name": "Super Micro", "description": "Builds AI servers"},
            catalog=[{"theme_id": 1, "theme_name": "AI - Infrastructure", "category": "AI", "representative_tickers": ["NVDA"]}],
            context_meta={"filtered_theme_count": 1},
        )

        self.assertEqual(payload["company_name"], "Super Micro")
        self.assertEqual(payload["recommended_action"], "watch_only")
        self.assertEqual(payload["suggested_existing_themes"][0]["theme_name"], "AI - Infrastructure")
        self.assertEqual(payload["caveats"], ["Needs review"])

    def test_merge_reconciliation_promotes_precise_new_theme_when_existing_fit_is_adjacent(self):
        catalog = [
            {
                "theme_id": 1,
                "theme_name": "Networking Infrastructure",
                "category": "Communications",
                "representative_tickers": ["CIEN"],
                "why_it_might_fit": "Broad networking adjacency",
            }
        ]
        heuristic = {
            "company_name": "Applied Optoelectronics",
            "short_company_description": "Provides optical interconnect modules for data centers.",
            "suggested_existing_themes": [
                {
                    "theme_id": 1,
                    "theme_name": "Networking Infrastructure",
                    "category": "Communications",
                    "why_it_might_fit": "Broad networking adjacency",
                    "fit_label": "adjacent_fit",
                }
            ],
            "possible_new_theme": "Optical Networking",
            "recommended_action": "consider_new_theme",
            "confidence": "medium",
            "rationale": "Optical Networking is more precise than broad adjacent themes.",
        }
        ai_draft = {
            "rationale": "Optical Networking is more precise than adjacent end-market themes.",
            "suggested_existing_themes": heuristic["suggested_existing_themes"],
            "recommended_action": "watch_only",
            "confidence": "high",
        }

        with (
            patch("src.scanner_research_merge.best_suggested_theme_fit_details", return_value={"score": 9, "direct_role_fit": False, "indirect_only_fit": True}),
            patch("src.scanner_research._has_strong_role_evidence", return_value=True),
            patch("src.scanner_research._candidate_new_theme_label", return_value="Optical Networking"),
            patch("src.scanner_research._supports_distinct_new_theme_label", return_value=True),
            patch("src.scanner_research._candidate_roles", return_value={"optical_networking"}),
            patch("src.scanner_research._annotate_existing_theme_suggestions", side_effect=lambda suggestions, *_args, **_kwargs: suggestions),
            patch("src.scanner_research._prioritize_operating_role_suggestions", side_effect=lambda suggestions, **_kwargs: suggestions),
            patch("src.scanner_research._proposed_new_theme_category", return_value="Systems & Infrastructure"),
        ):
            merged = merge_ai_with_heuristic_draft(
                ai_draft,
                heuristic,
                catalog,
                {"description": "Optical transceivers and interconnects"},
                {"ticker": "AAOI"},
            )

        self.assertEqual(merged["recommended_action"], "consider_new_theme")
        self.assertEqual(merged["possible_new_theme"], "Optical Networking")
        self.assertEqual(merged["possible_new_theme_category"], "Systems & Infrastructure")

    def test_ai_research_draft_for_strategy_keeps_contract_fields_with_sparse_ai_response(self):
        candidate = {"ticker": "SMCI"}
        catalog = [{"theme_id": 1, "theme_name": "AI - Infrastructure", "category": "AI", "representative_tickers": ["NVDA"]}]
        profile = {"company_name": "Super Micro", "description": "Builds AI server systems"}
        heuristic = {
            "suggested_existing_themes": [],
            "possible_new_theme": "AI Server Systems",
            "possible_new_theme_category": "Systems & Infrastructure",
            "recommended_action": "consider_new_theme",
            "rationale": "Heuristic baseline",
            "domain_anchor": "ai infrastructure",
            "dominant_business_role": "server_systems",
            "candidate_theme_ideas": ["AI Server Systems"],
            "matched_theme_candidates": [],
            "validation_debug": {"strategy": "description_theme_generation"},
            "research_timing_summary": {},
        }

        with (
            patch("src.scanner_research._baseline_research_draft", return_value=heuristic),
            patch("src.scanner_research._prefilter_ai_theme_catalog", return_value=(catalog, {"filtered_theme_count": 1})),
            patch("src.scanner_research._call_openai_research", return_value={"recommended_action": "watch_only"}),
            patch("src.scanner_research._estimate_context_size_chars", return_value=123),
            patch("src.scanner_research_merge.merge_ai_with_heuristic_draft", side_effect=lambda ai, base, *_args, **_kwargs: {**base, **ai}),
            patch("src.scanner_research_merge.openai_api_key", return_value="test-key"),
        ):
            draft = ai_research_draft_for_strategy(
                candidate,
                catalog,
                profile,
                strategy="description_theme_generation",
            )

        self.assertEqual(draft["theme_generation_strategy"], "description_theme_generation")
        self.assertEqual(draft["domain_anchor"], "ai infrastructure")
        self.assertEqual(draft["recommended_action"], "watch_only")
        self.assertIn("ai_request_ms", draft["research_timing_summary"])

    def test_best_suggested_theme_fit_details_prefers_highest_scoring_theme(self):
        catalog = [
            {"theme_id": 1, "theme_name": "A", "category": "Cat"},
            {"theme_id": 2, "theme_name": "B", "category": "Cat"},
        ]
        profile = {"description": "desc"}
        candidate = {"ticker": "ABC"}

        with (
            patch("src.scanner_research._candidate_analysis", return_value={"candidate_roles": set()}),
            patch("src.scanner_research._preprocessed_catalog", return_value=catalog),
            patch(
                "src.scanner_research._theme_fit_details",
                side_effect=[{"score": 5, "direct_role_fit": False, "indirect_only_fit": True}, {"score": 12, "direct_role_fit": True, "indirect_only_fit": False}],
            ),
        ):
            best = best_suggested_theme_fit_details(
                [{"theme_id": 1}, {"theme_id": 2}],
                catalog,
                profile,
                candidate,
            )

        self.assertEqual(best["score"], 12)
