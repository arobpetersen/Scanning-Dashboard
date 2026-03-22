from __future__ import annotations

"""Legacy scanner research compatibility surface.

The stable workflow now lives in the dedicated scanner_research_* modules.
This module intentionally remains as:
- a compatibility facade for existing callers and patch points
- the home of the deepest primitive inference engine that has not been moved yet

Cleanup here should prefer removing clearly dead residue while keeping
compatibility helpers stable.
"""

import json
import re
import time
from datetime import UTC, datetime

import requests

from .ai_proposals import sanitize_context
from .config import AI_MODEL, OPENAI_API_KEY_ENV, openai_api_key
from .provider_live import LiveProvider
from .scanner_research_cache import (
    _DESCRIPTION_ANALYSIS_CACHE,
    _PROFILE_CACHE,
    _THEME_PREPROCESS_CACHE,
)
from .scanner_audit import scanner_candidate_summary


RESEARCH_DRAFT_SYSTEM_PROMPT = """You are an advisory equity theme research assistant.
Return STRICT JSON with fields:
- ticker
- company_name
- short_company_description
- possible_similar_tickers (array of strings)
- suggested_existing_themes (array of objects with theme_id, theme_name, category, why_it_might_fit, fit_label)
- possible_new_theme
- confidence
- rationale
- caveats (array of strings)
- recommended_action

Rules:
- Advisory only. Never imply governed theme membership should be auto-applied.
- Ground suggested_existing_themes only in the provided governed theme catalog context.
- If evidence is weak, say so explicitly.
- Always provide a non-empty rationale of 2-4 concise sentences.
- The rationale must explain the company's business role/value-chain position and whether the best governed-theme fit is direct, adjacent, or weak.
- If no strong governed-theme fit exists, explicitly say that and still provide a useful rationale.
- If the company's role is more precise than the current governed themes, provide a concise possible_new_theme label when practical.
- Explicitly compare the best existing governed-theme fit versus the best narrow business-role / possible_new_theme label.
- Prefer the more precise classification over broad adjacency. If existing themes are only adjacent, keep them secondary and choose consider_new_theme when appropriate.
- Prefer concise, economically meaningful new-theme labels such as Optical Networking, Data Center Optics, Optical Interconnects, AI Fiber Optics, Semiconductor Materials, Semiconductor Substrates, or Compound Semiconductor Materials.
- Avoid vague labels like Advanced Infrastructure, Next-Gen Connectivity, Future Technology Platforms, or High Growth Materials.
- Keep outputs compact and operational. Do not leave required fields blank.
- recommended_action must be one of: add_to_existing_theme_review, consider_new_theme, watch_only, reject_for_now.
"""

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "group",
    "holdings",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "company",
    "technologies",
    "technology",
    "systems",
    "common",
    "stock",
    "class",
    "global",
    "international",
    "services",
}

CONCEPT_KEYWORDS: dict[str, set[str]] = {
    "cybersecurity": {"cybersecurity", "cyber", "security", "identity", "endpoint", "threat", "zero", "trust"},
    "cloud": {"cloud", "saas", "platform", "infrastructure", "observability", "devops"},
    "ai_compute": {"ai", "artificial", "gpu", "accelerated", "inference", "training", "compute", "datacenter", "data center"},
    "semiconductor": {"semiconductor", "chip", "chips", "fab", "wafer", "silicon", "processor"},
    "data_analytics": {"analytics", "data", "integration", "decision", "intelligence", "ontology"},
    "defense": {"defense", "military", "government", "battlefield", "aerospace"},
    "robotics": {"robotics", "automation", "autonomous", "industrial", "factory"},
    "space": {"space", "satellite", "launch", "orbital", "rocket"},
    "payments": {"payments", "payment", "merchant", "fintech", "card", "transaction"},
    "biotech": {"biotech", "therapeutic", "drug", "pharma", "clinical", "biology"},
    "energy": {"energy", "solar", "battery", "nuclear", "power", "grid"},
    "software": {"software", "application", "enterprise", "workflow"},
}

GENERIC_CONCEPTS = {"cloud", "software"}
WEAK_CONCEPTS = GENERIC_CONCEPTS | {"data_analytics"}
WEAK_ROLE_SIGNALS = {"software_tooling"}
WEAK_ARCHETYPE_SIGNALS = {"software_devops_cloud"}
WEAK_ECONOMIC_ROLE_SIGNALS = {"software_service_provider"}

ROLE_KEYWORDS: dict[str, set[str]] = {
    "optical_networking": {
        "optical",
        "optics",
        "transceiver",
        "transceivers",
        "fiber",
        "photonic",
        "photonics",
        "interconnect",
        "coherent",
        "broadband",
        "networking",
        "network",
    },
    "semiconductor_materials": {
        "substrate",
        "substates",
        "substrates",
        "wafer",
        "wafers",
        "epitaxy",
        "ingot",
        "gallium arsenide",
        "indium phosphide",
        "compound semiconductor",
        "compound semiconductors",
        "semiconductor materials",
        "semiconductor substrate",
    },
    "semiconductor_equipment": {
        "lithography",
        "etch",
        "deposition",
        "metrology",
        "inspection",
        "fab equipment",
        "packaging equipment",
        "process equipment",
    },
    "chip_designer": {"fabless", "asic", "gpu", "cpu", "processor", "chip designer"},
    "server_systems": {"server", "servers", "rack-scale", "rack", "system", "systems", "accelerated computing"},
    "power_generation": {"utility", "utilities", "generation", "electricity", "power plant", "nuclear plant", "renewable generation"},
    "power_equipment": {"transformer", "inverter", "switchgear", "power conversion", "grid equipment", "electrical equipment"},
    "software_tooling": {"software", "platform", "tooling", "workflow", "analytics", "integration", "observability"},
    "robotics_automation": {"robotics", "automation", "autonomous", "factory", "industrial automation"},
    "devices_endpoints": {"device", "devices", "endpoint", "consumer electronics", "handset"},
    "healthcare_equipment": {"imaging system", "diagnostic equipment", "medical device", "medical equipment", "surgical system"},
}

END_MARKET_KEYWORDS: dict[str, set[str]] = {
    "ai": {"ai", "artificial intelligence", "inference", "training", "accelerated"},
    "data_center": {"data center", "data-center", "datacenter", "hyperscale", "server", "rack-scale"},
    "telecom": {"telecom", "telecommunications", "carrier", "broadband", "network"},
    "semiconductor_market": {"semiconductor", "chip", "fab", "wafer"},
    "healthcare": {"healthcare", "medical", "clinical", "hospital"},
    "industrial": {"industrial", "factory", "manufacturing"},
    "defense_market": {"defense", "military", "government"},
    "energy_market": {"energy", "power", "grid", "utility"},
}

THEME_NEW_LABELS = {
    "cybersecurity": "Cybersecurity",
    "ai_compute": "AI Infrastructure",
    "data_analytics": "Data Analytics Platforms",
    "defense": "Defense Tech",
    "robotics": "Robotics & Automation",
    "space": "Space Infrastructure",
    "payments": "Digital Payments",
    "biotech": "Biotech Platforms",
    "energy": "Energy Transition",
    "semiconductor": "Semiconductors",
}

ROLE_NEW_LABELS = {
    "optical_networking": "Optical Networking",
    "semiconductor_materials": "Semiconductor Materials",
    "semiconductor_equipment": "Semiconductor Equipment",
    "chip_designer": "Chip Designers",
    "server_systems": "AI Server Systems",
    "power_generation": "Power Generation",
    "power_equipment": "Power Equipment",
    "software_tooling": "Enterprise Software Tooling",
    "robotics_automation": "Robotics & Automation",
    "devices_endpoints": "Connected Devices",
    "healthcare_equipment": "Healthcare Equipment",
}

ROLE_DISPLAY_NAMES = {
    "optical_networking": "optical networking and interconnect",
    "semiconductor_materials": "semiconductor materials and substrates",
    "semiconductor_equipment": "semiconductor equipment",
    "chip_designer": "chip design",
    "server_systems": "server systems",
    "power_generation": "power generation",
    "power_equipment": "power equipment",
    "software_tooling": "software tooling",
    "robotics_automation": "robotics and automation",
    "devices_endpoints": "devices and endpoints",
    "healthcare_equipment": "healthcare equipment",
}

DIRECT_PHRASE_THEME_FAMILIES = (
    {
        "aliases": ("drone", "drones"),
        "base_label": "Drones",
        "industry_phrases": ("drone industry", "drones industry"),
        "component_phrases": ("drone component", "drone components"),
        "module_phrases": ("drone module", "drone modules"),
        "systems_phrases": ("drone system", "drone systems"),
        "equipment_phrases": ("drone equipment", "drone equipments"),
        "component_label": "Drone Components",
        "systems_label": "Drone Systems",
        "equipment_label": "Drone Equipment",
    },
    {
        "aliases": ("uav", "uavs", "unmanned aerial vehicle", "unmanned aerial vehicles"),
        "base_label": "Unmanned Systems",
        "industry_phrases": ("uav industry", "uavs industry", "unmanned aerial vehicle industry"),
        "component_phrases": ("uav component", "uav components", "uav module", "uav modules"),
        "module_phrases": ("uav module", "uav modules"),
        "systems_phrases": ("uav system", "uav systems", "unmanned aerial vehicle systems"),
        "equipment_phrases": ("uav equipment", "uav equipments"),
        "component_label": "UAV Components",
        "systems_label": "UAV Systems",
        "equipment_label": "UAV Equipment",
    },
    {
        "aliases": ("unmanned system", "unmanned systems", "unmanned aircraft system", "unmanned aircraft systems"),
        "base_label": "Unmanned Systems",
        "industry_phrases": ("unmanned systems industry", "unmanned aircraft systems industry"),
        "component_phrases": (
            "unmanned system component",
            "unmanned system components",
            "unmanned systems component",
            "unmanned systems components",
            "unmanned aircraft system components",
        ),
        "module_phrases": (
            "unmanned system module",
            "unmanned system modules",
            "unmanned systems module",
            "unmanned systems modules",
            "unmanned aircraft system modules",
        ),
        "systems_phrases": ("unmanned system", "unmanned systems", "unmanned aircraft systems"),
        "equipment_phrases": ("unmanned systems equipment", "unmanned aircraft systems equipment"),
        "component_label": "Unmanned Systems Components",
        "systems_label": "Unmanned Systems",
        "equipment_label": "Unmanned Systems Equipment",
    },
)

GENERIC_PRODUCT_HEAD_LABELS = {
    "component": "Components",
    "components": "Components",
    "module": "Components",
    "modules": "Components",
    "system": "Systems",
    "systems": "Systems",
    "equipment": "Equipment",
    "device": "Devices",
    "devices": "Devices",
}

GENERIC_PRODUCT_MODIFIER_STOPWORDS = {
    "advanced",
    "american",
    "analytics",
    "build",
    "building",
    "builds",
    "business",
    "cloud",
    "commercial",
    "critical",
    "data",
    "digital",
    "enterprise",
    "future",
    "generic",
    "global",
    "industrial",
    "infrastructure",
    "integrated",
    "intelligent",
    "maker",
    "makers",
    "manufacture",
    "manufactured",
    "manufactures",
    "manufacturing",
    "mission",
    "next",
    "nextgen",
    "next-gen",
    "platform",
    "platforms",
    "produce",
    "producer",
    "producers",
    "produces",
    "provides",
    "provider",
    "providers",
    "service",
    "services",
    "selling",
    "sells",
    "smart",
    "software",
    "solution",
    "solutions",
    "strategic",
    "system",
    "systems",
    "technology",
    "technologies",
    "workflow",
}

AUTONOMOUS_COMPONENT_DRIFT_TOKENS = {
    "ai",
    "artificial intelligence",
    "robotics",
    "robotic",
    "edge computing",
    "edge ai",
    "semiconductor",
    "semiconductors",
    "equipment",
}

AUTONOMOUS_SYSTEM_DIRECT_TOKENS = {
    "drone",
    "drones",
    "uav",
    "uavs",
    "unmanned",
    "autonomous system",
    "autonomous systems",
}

DIGITAL_ASSET_INFRASTRUCTURE_DIRECT_TOKENS = {
    "stablecoin",
    "stablecoins",
    "digital asset",
    "blockchain payments",
    "on-chain payments",
    "onchain payments",
    "crypto payments",
    "payment rails",
    "settlement infrastructure",
    "tokenized dollar",
    "digital dollar",
    "crypto",
    "blockchain settlement",
    "digital asset settlement",
}

DIGITAL_ASSET_INFRASTRUCTURE_GENERIC_DRIFT_TOKENS = {
    "payment",
    "payments",
    "fintech",
    "bank",
    "banking",
    "software",
    "cloud",
    "devops",
    "observability",
}

UPSTREAM_OIL_GAS_DIRECT_TOKENS = {
    "upstream oil and gas",
    "oil and gas exploration and production",
    "exploration and production",
    "e&p",
    "working interest",
    "working interests",
    "operated interest",
    "operated interests",
    "oil and gas assets",
    "oil and gas properties",
    "onshore oil and gas assets",
}

UPSTREAM_OIL_GAS_DRIFT_TOKENS = {
    "lng",
    "liquefied natural gas",
    "energy transition",
    "renewable",
    "renewables",
    "clean energy",
    "decarbonization",
    "solar",
    "battery",
    "hydrogen",
}

UPSTREAM_OIL_GAS_GEOGRAPHY_BUCKET_TOKENS = {
    "permian",
    "marcellus",
    "haynesville",
    "bakken",
    "eagle ford",
    "appalachia",
    "midland basin",
    "delaware basin",
}

CONNECTED_OPERATIONS_SOFTWARE_TOKENS = {
    "software",
    "platform",
    "workflow",
    "monitoring",
    "operations data",
    "telematics",
    "analytics",
}

CONNECTED_OPERATIONS_PHYSICAL_TOKENS = {
    "connected assets",
    "connected devices",
    "assets",
    "fleet",
    "fleets",
    "vehicle",
    "vehicles",
    "equipment",
    "physical operations",
    "field operations",
    "operations",
    "third-party systems",
}

CONNECTED_OPERATIONS_DRIFT_TOKENS = {
    "luxury",
    "apparel",
    "fashion",
    "retail",
    "european",
    "asian",
    "premium brand",
}

DESCRIPTION_NATIVE_DESCRIPTOR_RULES = (
    {
        "label": "Optical Interconnects",
        "phrases": (
            "optical interconnect",
            "optical interconnects",
            "optical engine",
            "optical engines",
            "optical interposer",
            "optical interposers",
            "optical module",
            "optical modules",
            "fiber-optic networking products",
            "optoelectronic components",
            "co-packaged optics",
        ),
        "layers": {"component", "module", "system"},
        "family": "optical_networking",
    },
    {
        "label": "Photonics Platform",
        "phrases": ("photonics platform", "photonic interconnect platform"),
        "layers": {"platform", "system"},
        "family": "optical_networking",
    },
    {
        "label": "Additive Manufacturing",
        "phrases": (
            "additive manufacturing",
            "additive manufacturing systems",
            "additive manufacturing platform",
            "additive manufacturing software",
        ),
        "layers": {"system", "platform", "software_application"},
        "family": "industrial_additive_manufacturing",
    },
    {
        "label": "Industrial 3D Printing",
        "phrases": (
            "industrial 3d printing",
            "industrial 3d printer",
            "industrial 3d printers",
            "3d printing systems",
            "3d printer systems",
            "industrial printer systems",
        ),
        "layers": {"system", "device"},
        "family": "industrial_additive_manufacturing",
    },
    {
        "label": "Industrial Manufacturing Systems",
        "phrases": (
            "manufacturing systems",
            "production systems",
            "fabrication systems",
            "manufacturing software",
            "production software",
            "fabrication software",
        ),
        "layers": {"system", "platform", "software_application"},
        "family": "industrial_additive_manufacturing",
    },
    {
        "label": "Digital Asset Market Infrastructure",
        "phrases": (
            "digital asset platform",
            "digital asset exchange",
            "digital asset market infrastructure",
            "crypto market infrastructure",
            "exchange infrastructure",
            "institutional crypto services",
            "digital asset custody",
            "digital asset trading platform",
            "crypto exchange infrastructure",
        ),
        "layers": {"platform", "network_service"},
        "family": "digital_asset_infrastructure",
    },
    {
        "label": "Stablecoins / Digital Assets Infrastructure",
        "phrases": (
            "stablecoin",
            "stablecoins",
            "stablecoin infrastructure",
            "digital dollar",
            "tokenized dollar",
            "tokenized cash",
            "digital asset financial infrastructure",
            "digital asset infrastructure",
        ),
        "layers": {"platform", "network_service"},
        "family": "digital_asset_infrastructure",
    },
    {
        "label": "Blockchain Payments",
        "phrases": (
            "blockchain payments",
            "on-chain payments",
            "onchain payments",
            "blockchain payment rails",
            "crypto payments rails",
            "payments rails",
            "settlement infrastructure",
            "on-chain settlement",
            "digital asset settlement",
        ),
        "layers": {"platform", "network_service"},
        "family": "digital_asset_infrastructure",
    },
    {
        "label": "Crypto Payments Infrastructure",
        "phrases": (
            "crypto payments infrastructure",
            "blockchain payments infrastructure",
            "stablecoin payments infrastructure",
            "digital asset payments infrastructure",
        ),
        "layers": {"platform", "network_service"},
        "family": "digital_asset_infrastructure",
    },
    {
        "label": "Oil & Gas Exploration & Production",
        "phrases": (
            "oil and gas exploration and production",
            "exploration and production",
            "upstream oil and gas",
            "oil and gas assets",
            "oil and gas properties",
            "working interests",
            "operated interests",
            "onshore oil and gas assets",
        ),
        "layers": {"extractive_resource"},
        "family": "upstream_oil_gas",
    },
    {
        "label": "Upstream Oil & Gas",
        "phrases": (
            "upstream oil and gas",
            "oil and gas producer",
            "oil and gas producers",
            "upstream producer",
            "upstream producers",
        ),
        "layers": {"extractive_resource"},
        "family": "upstream_oil_gas",
    },
    {
        "label": "IoT / Connected Operations Platform",
        "phrases": (
            "connected operations platform",
            "connected operations software",
            "industrial iot platform",
            "iot platform",
            "connected asset platform",
            "fleet telematics platform",
        ),
        "layers": {"platform", "software_application"},
        "family": "connected_operations_iot",
    },
    {
        "label": "Industrial / Fleet Telematics Software",
        "phrases": (
            "fleet telematics software",
            "telematics software",
            "fleet operations software",
            "vehicle telematics platform",
        ),
        "layers": {"platform", "software_application"},
        "family": "connected_operations_iot",
    },
    {
        "label": "Consumer Fintech",
        "phrases": (
            "banking app",
            "neobank",
            "overdraft protection",
            "credit building",
            "credit builder",
            "financial management tools",
            "consumer financial services",
            "short-term liquidity",
            "digital banking",
            "consumer banking app",
        ),
        "layers": {"platform", "software_application", "network_service"},
        "family": "consumer_fintech",
    },
    {
        "label": "Digital Banking",
        "phrases": ("banking app", "neobank", "overdraft protection"),
        "layers": {"platform", "software_application", "network_service"},
        "family": "consumer_fintech",
    },
    {
        "label": "Memory & Storage",
        "phrases": (
            "memory semiconductors",
            "nand flash",
            "flash memory",
            "ssd",
            "ssds",
            "storage devices",
            "memory and storage products",
            "memory products",
            "storage products",
        ),
        "layers": {"component", "device"},
        "family": "memory_storage",
    },
    {
        "label": "Software-Defined Radio",
        "phrases": ("software-defined radio", "software defined radio", "sdr"),
        "layers": {"system", "device"},
        "family": "wireless_systems",
    },
    {
        "label": "Wireless Communications Infrastructure",
        "phrases": (
            "wireless communications infrastructure",
            "wireless infrastructure",
            "radio infrastructure",
            "wireless communications equipment",
            "communications equipment",
            "wireless equipment",
        ),
        "layers": {"system", "platform", "network_service"},
        "family": "wireless_systems",
    },
    {
        "label": "Autonomous Systems",
        "phrases": ("autonomous systems", "autonomous systems hardware", "autonomous systems platform"),
        "layers": {"system", "device"},
        "family": "autonomous_systems",
    },
)

EXTRACTIVE_RESOURCE_SIGNALS = {
    "mining",
    "mine",
    "mined",
    "concentrate",
    "concentrates",
    "ore",
    "ores",
    "extraction",
    "extract",
    "processing",
    "processed mineral",
    "processed mineral output",
    "mineral processing",
    "mine project",
    "mine projects",
    "mine asset",
    "mine assets",
    "shipping concentrate",
}

EXTRACTIVE_RESOURCE_COMMODITIES = {
    "tungsten": "Tungsten",
    "copper": "Copper",
    "lithium": "Lithium",
    "nickel": "Nickel",
    "uranium": "Uranium",
    "gold": "Gold",
    "silver": "Silver",
    "graphite": "Graphite",
    "cobalt": "Cobalt",
    "molybdenum": "Molybdenum",
    "rare earth": "Rare Earth",
    "rare earths": "Rare Earth",
    "iron ore": "Iron Ore",
}

VALUE_CHAIN_LAYER_KEYWORDS = {
    "merchant_input": {"materials", "material", "chemicals", "chemical", "coatings", "coating", "substrates", "substrate", "wafers", "wafer", "consumables", "consumable", "compound"},
    "component": {"components", "component", "parts", "part", "inputs", "input"},
    "module": {"modules", "module"},
    "device": {"devices", "device", "ssd", "ssds"},
    "system": {"systems", "system", "equipment"},
    "platform": {"platform", "platforms", "infrastructure", "exchange"},
    "software_application": {"software", "application", "app", "apps", "tools", "tooling"},
    "network_service": {"network", "service", "services", "market infrastructure", "custody"},
    "extractive_resource": {"mining", "mine", "ore", "concentrate", "extraction", "mineral processing"},
}

UMBRELLA_SIGNAL_KEYWORDS = {
    "cloud": {"cloud", "devops", "observability"},
    "broad_software": {"software", "platform", "enterprise software"},
    "cybersecurity": {"cybersecurity", "cyber", "security"},
    "ai_infrastructure": {"ai infrastructure", "ai data centers", "hyperscale", "gpu cluster"},
}

MERCHANT_INPUT_EVIDENCE_KEYWORDS = {
    "materials",
    "material",
    "substrates",
    "substrate",
    "coatings",
    "coating",
    "chemicals",
    "chemical",
    "consumables",
    "consumable",
    "wafer",
    "wafers",
    "compound",
    "inputs",
}

MERCHANT_INPUT_PROCESS_CONTEXTS = {
    "wafer-level",
    "wafer level",
    "manufacturing technique",
    "manufacturing techniques",
    "packaging technique",
    "packaging techniques",
    "fabrication technique",
    "fabrication techniques",
    "process technology",
    "process technologies",
    "manufacturing process",
    "manufacturing processes",
}

INDUSTRIAL_ADDITIVE_MANUFACTURING_SIGNALS = {
    "additive manufacturing",
    "industrial 3d printing",
    "3d printing",
    "3d printer",
    "3d printers",
    "industrial printer",
    "industrial printers",
}

INDUSTRIAL_MANUFACTURING_SYSTEM_SIGNALS = {
    "printer systems",
    "printing systems",
    "production systems",
    "manufacturing systems",
    "fabrication systems",
    "industrial manufacturing software",
    "manufacturing software",
    "production software",
    "fabrication software",
}

INDUSTRIAL_MANUFACTURING_CONTEXT_SIGNALS = {
    "industrial",
    "manufacturing",
    "production",
    "fabrication",
    "factory",
    "printer system",
    "hardware",
}

INDUSTRIAL_MANUFACTURING_DRIFT_TOKENS = {
    "luxury",
    "luxury goods",
    "europe",
    "european",
    "asia",
    "asian",
    "china",
    "chinese",
    "japan",
    "japanese",
    "india",
    "indian",
    "germany",
    "german",
    "france",
    "french",
    "italy",
    "italian",
    "america",
    "american",
    "latin america",
    "middle east",
}

MISSING_THEME_CATEGORY_BY_FAMILY = {
    "industrial_additive_manufacturing": "Additive Manufacturing / Industrial 3D Printing",
    "optical_networking": "Optical Networking",
    "digital_asset_infrastructure": "Crypto Infrastructure / Digital Assets",
    "consumer_fintech": "Financial Technology",
    "memory_storage": "Semiconductors / Storage",
    "wireless_systems": "Wireless Infrastructure",
    "autonomous_systems": "Autonomous Systems",
    "upstream_oil_gas": "Oil & Gas / Upstream",
    "connected_operations_iot": "Industrial Software / IoT",
    "extractive_resources": "Metals & Mining",
}

END_MARKET_DISPLAY_NAMES = {
    "ai": "AI",
    "data_center": "data centers",
    "telecom": "telecom",
    "semiconductor_market": "semiconductors",
    "healthcare": "healthcare",
    "industrial": "industrial markets",
    "defense_market": "defense/government",
    "energy_market": "energy and power",
}

ROLE_FAMILY = {
    "optical_networking": "communications_hardware",
    "semiconductor_materials": "semiconductor_supply_chain",
    "semiconductor_equipment": "semiconductor_supply_chain",
    "chip_designer": "semiconductor_products",
    "server_systems": "compute_hardware",
    "power_generation": "energy",
    "power_equipment": "energy",
    "software_tooling": "software",
    "robotics_automation": "industrial_automation",
    "devices_endpoints": "devices",
    "healthcare_equipment": "healthcare_devices",
}

ARCHETYPE_KEYWORDS: dict[str, set[str]] = {
    "fintech_payments_lending": {
        "payments",
        "payment",
        "merchant",
        "checkout",
        "transaction",
        "transactions",
        "fintech",
        "lending",
        "loan",
        "loans",
        "installment",
        "installments",
        "buy now pay later",
        "bnpl",
        "consumer finance",
    },
    "digital_identity_security": {
        "identity",
        "verification",
        "verify",
        "verified",
        "credential",
        "credentials",
        "biometric",
        "biometrics",
        "authentication",
        "identity platform",
        "access control",
        "secure identity",
    },
    "semiconductor_materials_electronics_materials": {
        "semiconductor materials",
        "compound semiconductor",
        "compound semiconductors",
        "electronics materials",
        "electronic materials",
        "substrate",
        "substrates",
        "wafer",
        "wafers",
        "epitaxy",
        "ingot",
        "gallium arsenide",
        "indium phosphide",
        "packaging materials",
        "specialty materials",
    },
    "ai_infrastructure_data_centers": {
        "data center",
        "data centers",
        "data-center",
        "datacenter",
        "datacenters",
        "hyperscale",
        "ai infrastructure",
        "gpu cluster",
        "compute cluster",
        "data-center capacity",
        "colocation",
        "server infrastructure",
    },
    "aerospace_defense_space_systems": {
        "defense",
        "satellite",
        "payload",
        "space systems",
        "defense systems",
        "aerospace",
        "government programs",
        "orbital",
    },
    "networking_interconnect": {
        "optical",
        "optics",
        "transceiver",
        "transceivers",
        "fiber",
        "interconnect",
        "coherent",
        "networking",
        "communications infrastructure",
    },
    "software_devops_cloud": {
        "software",
        "cloud",
        "devops",
        "observability",
        "saas",
        "platform",
        "workflow",
        "enterprise software",
        "tooling",
    },
    "healthcare_devices_services": {
        "animal health",
        "cro",
        "cmo",
        "dental",
        "medical",
        "healthcare",
        "clinical",
        "diagnostic",
        "hospital",
        "medical device",
    },
    "industrial_materials_chemicals": {
        "chemicals",
        "chemical",
        "coatings",
        "paints",
        "specialty chemicals",
        "materials",
        "industrial materials",
        "specialty materials",
    },
}

ARCHETYPE_DISPLAY_NAMES = {
    "fintech_payments_lending": "fintech/payments/lending",
    "digital_identity_security": "digital identity/security",
    "semiconductor_materials_electronics_materials": "semiconductor and electronics materials",
    "ai_infrastructure_data_centers": "AI infrastructure/data centers",
    "aerospace_defense_space_systems": "aerospace/defense/space systems",
    "networking_interconnect": "networking/interconnect",
    "software_devops_cloud": "software/devops/cloud",
    "healthcare_devices_services": "healthcare devices/services",
    "industrial_materials_chemicals": "industrial materials/chemicals",
}

ARCHETYPE_FAMILY = {
    "fintech_payments_lending": "finance",
    "digital_identity_security": "identity_security",
    "semiconductor_materials_electronics_materials": "semiconductor_materials",
    "ai_infrastructure_data_centers": "ai_infrastructure",
    "aerospace_defense_space_systems": "defense_space",
    "networking_interconnect": "communications_hardware",
    "software_devops_cloud": "software",
    "healthcare_devices_services": "healthcare",
    "industrial_materials_chemicals": "materials",
}

ARCHETYPE_ADJACENCY = {
    ("ai_infrastructure", "communications_hardware"),
    ("ai_infrastructure", "semiconductor_materials"),
    ("communications_hardware", "semiconductor_materials"),
    ("finance", "software"),
    ("identity_security", "software"),
    ("materials", "semiconductor_materials"),
}

ECONOMIC_ROLE_KEYWORDS: dict[str, set[str]] = {
    "component_supplier": {
        "component",
        "components",
        "module",
        "modules",
        "engine",
        "engines",
        "transceiver",
        "transceivers",
        "interposer",
        "optical engine",
        "light source",
        "supplier",
        "supplies",
    },
    "materials_supplier": {
        "materials",
        "substrate",
        "substrates",
        "wafer",
        "wafers",
        "compound semiconductor",
        "electronic materials",
        "electronics materials",
        "specialty chemicals",
        "packaging materials",
    },
    "end_platform_operator": {
        "operates",
        "operator",
        "platform operator",
        "marketplace",
        "network operator",
        "runs",
        "operates and builds",
        "campus",
        "campuses",
        "capacity",
    },
    "infrastructure_operator": {
        "builds and operates",
        "owns and operates",
        "data-center capacity",
        "hyperscale campus",
        "server campus",
        "infrastructure operator",
        "colocation",
    },
    "software_service_provider": {
        "software",
        "platform",
        "service",
        "services",
        "saas",
        "workflow",
        "observability",
        "analytics platform",
    },
    "financial_platform": {
        "payments platform",
        "digital checkout",
        "consumer lending",
        "merchant",
        "fintech platform",
        "installment payments",
        "financial platform",
    },
    "identity_verification_platform": {
        "identity verification",
        "biometric",
        "authentication",
        "credential",
        "member authentication",
        "identity platform",
    },
    "defense_systems_manufacturer": {
        "defense systems",
        "mission systems",
        "satellite systems",
        "space systems",
        "payload",
        "aircraft systems",
        "defense manufacturer",
        "aerospace systems",
        "mission hardware",
        "aerospace and government",
    },
}

ECONOMIC_ROLE_DISPLAY_NAMES = {
    "component_supplier": "component supplier",
    "materials_supplier": "materials supplier",
    "end_platform_operator": "end platform/operator",
    "infrastructure_operator": "infrastructure/operator",
    "software_service_provider": "software/service provider",
    "financial_platform": "financial platform",
    "identity_verification_platform": "identity verification platform",
    "defense_systems_manufacturer": "defense/space systems manufacturer",
}

ROLE_ALIGNMENT = {
    "optical_networking": {"component_supplier"},
    "semiconductor_materials": {"materials_supplier"},
    "semiconductor_equipment": {"component_supplier"},
    "chip_designer": {"component_supplier"},
    "server_systems": {"end_platform_operator", "infrastructure_operator", "component_supplier"},
    "software_tooling": {"software_service_provider"},
    "power_generation": {"infrastructure_operator"},
    "power_equipment": {"component_supplier"},
    "healthcare_equipment": {"component_supplier"},
}

ARCHETYPE_ALIGNMENT = {
    "fintech_payments_lending": {"financial_platform"},
    "digital_identity_security": {"identity_verification_platform", "software_service_provider"},
    "semiconductor_materials_electronics_materials": {"materials_supplier", "component_supplier"},
    "ai_infrastructure_data_centers": {"infrastructure_operator", "end_platform_operator"},
    "networking_interconnect": {"component_supplier"},
    "software_devops_cloud": {"software_service_provider"},
    "healthcare_devices_services": {"component_supplier", "software_service_provider"},
    "aerospace_defense_space_systems": {"defense_systems_manufacturer"},
}

VAGUE_NEW_THEME_LABEL_TOKENS = {
    "advanced",
    "business services",
    "future",
    "next-gen",
    "next generation",
    "platform",
    "platforms",
    "services",
    "solutions",
    "technology services",
    "infrastructure services",
    "high growth",
}

GENERIC_FACTOR_THEME_TOKENS = {
    "growth",
    "high growth",
    "momentum",
    "quality",
    "value",
    "large cap",
    "small cap",
    "mid cap",
    "factor",
    "style",
    "leaders",
}

RESEARCH_STRATEGIES = {"description_theme_generation"}
RESEARCH_REVIEW_OUTCOMES = {
    "direct_fit_correct",
    "adjacent_fit_acceptable",
    "should_have_been_tentative",
    "false_positive",
    "missed_obvious_theme",
}

DOMAIN_ANCHOR_LABELS = {
    "fintech_payments_lending": "fintech",
    "digital_identity_security": "digital identity/security",
    "semiconductor_materials_electronics_materials": "semiconductor/electronics",
    "ai_infrastructure_data_centers": "AI infrastructure/data centers",
    "aerospace_defense_space_systems": "aerospace/defense/space",
    "networking_interconnect": "networking/communications",
    "software_devops_cloud": "software/cloud",
    "healthcare_devices_services": "healthcare",
    "industrial_materials_chemicals": "industrials/materials",
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _now_perf() -> float:
    return time.perf_counter()


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 1)


def _normalize_research_strategy(value: object, fallback: str = "description_theme_generation") -> str:
    normalized = _normalize_text(value) or fallback
    return normalized if normalized in RESEARCH_STRATEGIES else fallback


def _normalize_research_review_outcome(value: object) -> str:
    normalized = _normalize_text(value)
    return normalized if normalized in RESEARCH_REVIEW_OUTCOMES else ""


def _compact_error_reason(exc: Exception) -> str:
    text = _normalize_text(exc)
    if not text:
        return "Research generation error."
    return _sanitize_error_text(text, limit=140)


def _sanitize_error_text(text: object, *, limit: int = 200) -> str:
    text = _normalize_text(text)
    if not text:
        return ""
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?i)bearer\s+[a-z0-9_\-\.]+", "bearer [redacted]", text)
    text = re.sub(r"(?i)api[_ -]?key[=:]\s*[^ ,;]+", "api_key=[redacted]", text)
    return text[:limit]


def _normalize_optional_theme_label(value: object) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text.lower()
    if normalized in {
        _normalize_text(rule.get("label")).lower()
        for rule in DESCRIPTION_NATIVE_DESCRIPTOR_RULES
        if _normalize_text(rule.get("label"))
    }:
        return text
    empty_markers = {
        "none",
        "none suggested",
        "no suggestion",
        "no strong fit",
        "no strong existing fit",
        "n/a",
        "na",
        "null",
    }
    if normalized in empty_markers:
        return None
    if any(token in normalized for token in VAGUE_NEW_THEME_LABEL_TOKENS):
        return None
    return text


def _fit_label_from_details(fit_details: dict[str, object]) -> str:
    from .scanner_research_heuristics import fit_label_from_details

    return fit_label_from_details(fit_details)


def _annotate_suggestion_fit(
    suggestion: dict[str, object],
    fit_details: dict[str, object],
) -> dict[str, object]:
    from .scanner_research_heuristics import annotate_suggestion_fit

    return annotate_suggestion_fit(suggestion, fit_details)


def _truncate_existing_theme_suggestions(suggestions: list[dict[str, object]], *, limit: int = 3) -> list[dict[str, object]]:
    from .scanner_research_heuristics import truncate_existing_theme_suggestions

    return truncate_existing_theme_suggestions(suggestions, limit=limit)


def _extract_openai_error_details(exc: Exception) -> dict[str, object]:
    details: dict[str, object] = {
        "error_class": exc.__class__.__name__,
        "model": AI_MODEL,
    }
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            details["status_code"] = int(status_code)
        try:
            body = response.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            error_body = body.get("error")
            if isinstance(error_body, dict):
                error_type = _sanitize_error_text(error_body.get("type"), limit=80)
                error_message = _sanitize_error_text(error_body.get("message"), limit=200)
                if error_type:
                    details["error_type"] = error_type
                if error_message:
                    details["error_message"] = error_message
        if "error_message" not in details:
            response_text = _sanitize_error_text(getattr(response, "text", ""), limit=200)
            if response_text:
                details["error_message"] = response_text
    if "error_message" not in details:
        details["error_message"] = _compact_error_reason(exc)
    return details


def _format_openai_error_summary(details: dict[str, object]) -> str:
    status = details.get("status_code")
    error_type = _normalize_text(details.get("error_type"))
    message = _sanitize_error_text(details.get("error_message"), limit=140)
    model = _normalize_text(details.get("model")) or AI_MODEL
    parts = ["OpenAI request failed"]
    if status:
        parts.append(f"HTTP {status}")
    if error_type:
        parts.append(error_type)
    summary = ": ".join([parts[0], " ".join(parts[1:])]) if len(parts) > 1 else parts[0]
    if message:
        summary += f" - {message}"
    if model:
        summary += f" (model: {model})"
    return summary[:220]


def _tokenize(*parts: object) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        for token in re.findall(r"[a-z0-9]+", _normalize_text(part).lower()):
            if len(token) >= 3 and token not in STOPWORDS:
                tokens.add(token)
    return tokens


def _contains_phrase(text: str, keyword: str) -> bool:
    if " " in keyword:
        pattern = r"\b" + r"[\s\-/]+".join(re.escape(part) for part in keyword.split()) + r"\b"
        return bool(re.search(pattern, text))
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text))


def _has_specific_domain_signal(*parts: object) -> bool:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    if not text:
        return False
    for concept, keywords in CONCEPT_KEYWORDS.items():
        if concept in WEAK_CONCEPTS:
            continue
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            return True
    for role, keywords in ROLE_KEYWORDS.items():
        if role in WEAK_ROLE_SIGNALS:
            continue
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            return True
    for archetype, keywords in ARCHETYPE_KEYWORDS.items():
        if archetype in WEAK_ARCHETYPE_SIGNALS:
            continue
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            return True
    return False


def _infer_concepts(*parts: object) -> set[str]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    concepts: set[str] = set()
    for concept, keywords in CONCEPT_KEYWORDS.items():
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            concepts.add(concept)
    return concepts


def _infer_signals(keyword_map: dict[str, set[str]], *parts: object) -> set[str]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    signals: set[str] = set()
    for signal, keywords in keyword_map.items():
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            signals.add(signal)
    return signals


def _count_signal_hits(keyword_map: dict[str, set[str]], *parts: object) -> dict[str, int]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    counts: dict[str, int] = {}
    for signal, keywords in keyword_map.items():
        count = sum(1 for keyword in keywords if _contains_phrase(text, keyword))
        if count:
            counts[signal] = count
    return counts


def _representative_ticker_role_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["AAOI", "CIEN", "LITE", "FN", "INFN"]):
        hints.add("optical_networking")
    if any(symbol in joined for symbol in ["AXT", "WOLF", "COHR", "ONTO"]):
        hints.add("semiconductor_materials")
    if any(symbol in joined for symbol in ["ASML", "AMAT", "LRCX", "KLAC", "ONTO"]):
        hints.add("semiconductor_equipment")
    if any(symbol in joined for symbol in ["NVDA", "AMD", "AVGO", "MRVL", "MCHP"]):
        hints.add("chip_designer")
    if any(symbol in joined for symbol in ["SMCI", "DELL", "HPE"]):
        hints.add("server_systems")
    if any(symbol in joined for symbol in ["CRWD", "PANW", "ZS", "FTNT", "PLTR", "SNOW", "DDOG"]):
        hints.add("software_tooling")
    if any(symbol in joined for symbol in ["GEV", "ETN", "HUBB", "VRT"]):
        hints.add("power_equipment")
    if any(symbol in joined for symbol in ["CEG", "VST", "NEE"]):
        hints.add("power_generation")
    return hints


def _representative_ticker_market_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["CRWD", "PANW", "ZS", "FTNT"]):
        hints.update({"cybersecurity", "data_center"})
    if any(symbol in joined for symbol in ["NVDA", "AMD", "AVGO", "SMCI", "MU"]):
        hints.update({"ai_compute", "semiconductor", "ai", "data_center", "semiconductor_market"})
    if any(symbol in joined for symbol in ["PLTR", "SNOW", "DDOG"]):
        hints.add("data_analytics")
    if any(symbol in joined for symbol in ["LMT", "NOC", "KTOS", "PLTR"]):
        hints.update({"defense", "defense_market"})
    if any(symbol in joined for symbol in ["RKLB", "ASTS", "LUNR"]):
        hints.add("space")
    if any(symbol in joined for symbol in ["AAOI", "LITE", "CIEN", "INFN", "FN"]):
        hints.update({"telecom", "data_center"})
    if any(symbol in joined for symbol in ["AXT", "WOLF", "ONTO", "AMAT", "LRCX", "ASML"]):
        hints.add("semiconductor_market")
    if any(symbol in joined for symbol in ["CEG", "VST", "GEV", "ETN", "VRT"]):
        hints.add("energy_market")
    return hints


def _theme_concepts(theme_entry: dict[str, object]) -> set[str]:
    return _infer_concepts(
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_market_hints(list(theme_entry.get("representative_tickers") or []))


def _candidate_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    roles = _infer_signals(ROLE_KEYWORDS, *parts)
    role_counts = _count_signal_hits(ROLE_KEYWORDS, *parts)
    combined_text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    strong_optical_terms = {"optical", "optics", "fiber", "transceiver", "transceivers", "photonic", "photonics", "coherent"}
    if "optical_networking" in roles:
        optical_hits = int(role_counts.get("optical_networking") or 0)
        has_strong_optical_term = any(_contains_phrase(combined_text, term) for term in strong_optical_terms)
        if optical_hits < 2 and not has_strong_optical_term:
            roles.discard("optical_networking")
    strong_device_terms = {"endpoint", "endpoints", "consumer electronics", "handset"}
    if "devices_endpoints" in roles:
        device_hits = int(role_counts.get("devices_endpoints") or 0)
        has_strong_device_term = any(_contains_phrase(combined_text, term) for term in strong_device_terms)
        if device_hits < 2 and not has_strong_device_term:
            roles.discard("devices_endpoints")
    explicit_software_service_terms = {"cloud", "saas", "observability", "workflow", "devops", "enterprise software"}
    if "software_tooling" in roles and _contains_phrase(combined_text, "software-defined radio"):
        if not any(_contains_phrase(combined_text, term) for term in explicit_software_service_terms):
            roles.discard("software_tooling")
    if "semiconductor_materials" in roles and not _merchant_input_evidence(combined_text):
        roles.discard("semiconductor_materials")
    return roles


def _candidate_end_markets(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )


def _theme_roles(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    )
    roles = _infer_signals(
        ROLE_KEYWORDS,
        *parts,
    ) | _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
    role_counts = _count_signal_hits(ROLE_KEYWORDS, *parts)
    combined_text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    strong_optical_terms = {"optical", "optics", "fiber", "transceiver", "transceivers", "photonic", "photonics", "coherent"}
    if "optical_networking" in roles:
        optical_hits = int(role_counts.get("optical_networking") or 0)
        has_strong_optical_term = any(_contains_phrase(combined_text, term) for term in strong_optical_terms)
        representative_roles = _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
        if optical_hits < 2 and not has_strong_optical_term and "optical_networking" not in representative_roles:
            roles.discard("optical_networking")
    strong_device_terms = {"endpoint", "endpoints", "consumer electronics", "handset"}
    if "devices_endpoints" in roles:
        device_hits = int(role_counts.get("devices_endpoints") or 0)
        has_strong_device_term = any(_contains_phrase(combined_text, term) for term in strong_device_terms)
        representative_roles = _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
        if device_hits < 2 and not has_strong_device_term and "devices_endpoints" not in representative_roles:
            roles.discard("devices_endpoints")
    return roles


def _theme_end_markets(theme_entry: dict[str, object]) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_market_hints(list(theme_entry.get("representative_tickers") or []))


def _ranked_archetypes(scores: dict[str, int], *, threshold: int) -> set[str]:
    if not scores:
        return set()
    strongest = max(scores.values())
    return {
        archetype
        for archetype, score in scores.items()
        if score >= threshold and score >= strongest - 1
    }


def _infer_archetype_scores(
    *parts: object,
    roles: set[str] | None = None,
    concepts: set[str] | None = None,
    markets: set[str] | None = None,
) -> dict[str, int]:
    scores = _count_signal_hits(ARCHETYPE_KEYWORDS, *parts)
    roles = roles or set()
    concepts = concepts or set()
    markets = markets or set()
    if "semiconductor_materials" in roles:
        scores["semiconductor_materials_electronics_materials"] = scores.get("semiconductor_materials_electronics_materials", 0) + 5
    if "optical_networking" in roles:
        scores["networking_interconnect"] = scores.get("networking_interconnect", 0) + 4
    if "software_tooling" in roles:
        scores["software_devops_cloud"] = scores.get("software_devops_cloud", 0) + 3
    if "healthcare_equipment" in roles:
        scores["healthcare_devices_services"] = scores.get("healthcare_devices_services", 0) + 4
    if "payments" in concepts:
        scores["fintech_payments_lending"] = scores.get("fintech_payments_lending", 0) + 4
    if "cybersecurity" in concepts and any(
        _contains_phrase(" ".join(_normalize_text(part).lower() for part in parts), token)
        for token in {"identity", "verification", "biometric", "authentication"}
    ):
        scores["digital_identity_security"] = scores.get("digital_identity_security", 0) + 4
    if "ai_compute" in concepts:
        scores["ai_infrastructure_data_centers"] = scores.get("ai_infrastructure_data_centers", 0) + 3
    elif "data_center" in markets and roles & {"optical_networking", "server_systems", "power_equipment"}:
        scores["ai_infrastructure_data_centers"] = scores.get("ai_infrastructure_data_centers", 0) + 2
    if "defense" in concepts or "space" in concepts or "defense_market" in markets:
        scores["aerospace_defense_space_systems"] = scores.get("aerospace_defense_space_systems", 0) + 4
    if "healthcare" in markets or "biotech" in concepts:
        scores["healthcare_devices_services"] = scores.get("healthcare_devices_services", 0) + 2
    if "telecom" in markets:
        scores["networking_interconnect"] = scores.get("networking_interconnect", 0) + 1
    if "semiconductor_market" in markets or "semiconductor" in concepts:
        scores["semiconductor_materials_electronics_materials"] = scores.get("semiconductor_materials_electronics_materials", 0) + 1
    if "cloud" in concepts or "software" in concepts:
        scores["software_devops_cloud"] = scores.get("software_devops_cloud", 0) + 1
    if "energy_market" in markets and "networking_interconnect" in scores:
        scores["networking_interconnect"] -= 1
    return {key: value for key, value in scores.items() if value > 0}


def _candidate_archetypes(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    return _ranked_archetypes(
        _infer_archetype_scores(
            *parts,
            roles=_candidate_roles(profile, candidate, *extra_parts),
            concepts=_candidate_concepts(profile, candidate),
            markets=_candidate_end_markets(profile, candidate, *extra_parts),
        ),
        threshold=3,
    )


def _theme_archetypes(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
        " ".join(str(value or "") for value in list(theme_entry.get("representative_tickers") or [])),
    )
    return _ranked_archetypes(
        _infer_archetype_scores(
            *parts,
            roles=_theme_roles(theme_entry),
            concepts=_theme_concepts(theme_entry),
            markets=_theme_end_markets(theme_entry),
        ),
        threshold=2,
    )


def _archetype_relation(candidate_archetypes: set[str], theme_archetypes: set[str]) -> str:
    if not candidate_archetypes or not theme_archetypes:
        return "unknown"
    if candidate_archetypes & theme_archetypes:
        return "direct"
    candidate_families = {ARCHETYPE_FAMILY.get(value) for value in candidate_archetypes if ARCHETYPE_FAMILY.get(value)}
    theme_families = {ARCHETYPE_FAMILY.get(value) for value in theme_archetypes if ARCHETYPE_FAMILY.get(value)}
    if candidate_families & theme_families:
        return "adjacent"
    for left in candidate_families:
        for right in theme_families:
            pair = tuple(sorted((left, right)))
            if pair in ARCHETYPE_ADJACENCY:
                return "adjacent"
    return "incompatible"


def _infer_economic_role_scores(
    *parts: object,
    roles: set[str] | None = None,
    archetypes: set[str] | None = None,
) -> dict[str, int]:
    scores = _count_signal_hits(ECONOMIC_ROLE_KEYWORDS, *parts)
    roles = roles or set()
    archetypes = archetypes or set()
    for role in roles:
        for economic_role in ROLE_ALIGNMENT.get(role, set()):
            scores[economic_role] = scores.get(economic_role, 0) + 3
    for archetype in archetypes:
        for economic_role in ARCHETYPE_ALIGNMENT.get(archetype, set()):
            scores[economic_role] = scores.get(economic_role, 0) + 2
    return {key: value for key, value in scores.items() if value > 0}


def _candidate_economic_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    scores = _infer_economic_role_scores(
        *parts,
        roles=_candidate_roles(profile, candidate, *extra_parts),
        archetypes=_candidate_archetypes(profile, candidate, *extra_parts),
    )
    return _ranked_archetypes(scores, threshold=3)


def _theme_economic_roles(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
        " ".join(str(value or "") for value in list(theme_entry.get("representative_tickers") or [])),
    )
    scores = _infer_economic_role_scores(
        *parts,
        roles=_theme_roles(theme_entry),
        archetypes=_theme_archetypes(theme_entry),
    )
    return _ranked_archetypes(scores, threshold=2)


def _dominant_economic_role(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    roles = sorted(_candidate_economic_roles(profile, candidate, *extra_parts))
    return roles[0] if roles else ""


def _domain_anchor(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    descriptor_families = _descriptor_families(
        _description_native_business_descriptors(
            " ".join(
                [
                    _normalize_text(profile.get("company_name")),
                    _normalize_text(profile.get("description")),
                    _normalize_text(profile.get("sic_description")),
                    _normalize_text(candidate.get("recommendation_reason")),
                    *[_normalize_text(part) for part in extra_parts],
                ]
            )
        )
    )
    if "industrial_additive_manufacturing" in descriptor_families:
        return "industrial manufacturing/additive"
    if "connected_operations_iot" in descriptor_families:
        return "industrial software/iot"
    if "extractive_resources" in descriptor_families:
        return "mining/resources"
    archetypes = sorted(_candidate_archetypes(profile, candidate, *extra_parts))
    if archetypes:
        return DOMAIN_ANCHOR_LABELS.get(archetypes[0], archetypes[0].replace("_", "/"))
    concepts = _candidate_concepts(profile, candidate)
    if "defense" in concepts or "space" in concepts:
        return "aerospace/defense/space"
    if "payments" in concepts:
        return "fintech"
    if "semiconductor" in concepts:
        return "semiconductor/electronics"
    return "unclear"


def _candidate_theme_ideas_from_description(profile: dict[str, object], candidate: dict[str, object]) -> list[str]:
    description = " ".join(
        [
            _normalize_text(profile.get("company_name")),
            _normalize_text(profile.get("description")),
            _normalize_text(profile.get("sic_description")),
            _normalize_text(candidate.get("recommendation_reason")),
        ]
    ).lower()
    archetypes = _candidate_archetypes(profile, candidate)
    domain_anchor = _domain_anchor(profile, candidate)
    dominant_role = _dominant_economic_role(profile, candidate)
    ideas: list[str] = []
    native_descriptors = _description_native_business_descriptors(description)
    descriptor_families = _descriptor_families(native_descriptors)
    strong_digital_asset_infrastructure = _has_strong_digital_asset_infrastructure_signals(description)
    connected_operations_iot_signals = "connected_operations_iot" in descriptor_families
    explicit_cloud_software_signals = any(
        token in description
        for token in ["cloud", "saas", "observability", "workflow", "devops", "enterprise software"]
    )
    memory_storage_signals = any(token in description for token in ["memory", "nand", "flash", "storage controller", "storage controllers"])
    lending_signals = any(token in description for token in ["lending", "loan", "loans", "installment", "buy-now-pay-later", "bnpl", "consumer finance"])
    payments_signals = any(token in description for token in ["payment", "payments", "merchant acceptance", "wallet", "checkout"])
    identity_signals = any(token in description for token in ["identity verification", "digital identity", "authentication", "identity"])

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in ideas:
            ideas.append(normalized)

    phrase_based_ideas = _sort_theme_ideas_by_specificity(
        _direct_phrase_theme_ideas(description) + _generic_product_phrase_theme_ideas(description)
    )
    merchant_input_evidence = _merchant_input_evidence(description)
    for descriptor in native_descriptors:
        add(descriptor)

    if memory_storage_signals:
        add("Semiconductor Memory")
        add("Data Storage")
    if "semiconductor_materials_electronics_materials" in archetypes and merchant_input_evidence:
        add("Semiconductor Materials")
        if "substrate" in description or "substrates" in description:
            add("Semiconductor Substrates")
        if "electronics materials" in description or "packaging materials" in description:
            add("Electronics Materials")
    if identity_signals:
        add("Identity Verification")
        add("Digital Identity")
    elif "digital_identity_security" in archetypes:
        add("Digital Identity")
        add("Identity Verification")
    if lending_signals:
        if payments_signals and not strong_digital_asset_infrastructure:
            add("Digital Payments")
            add("Fintech Payments")
        add("Consumer Lending")
    elif payments_signals and not strong_digital_asset_infrastructure:
        add("Digital Payments")
        add("Fintech Payments")
    if "fintech_payments_lending" in archetypes and not strong_digital_asset_infrastructure:
        add("Fintech Payments")
        add("Digital Payments")
        if lending_signals:
            add("Consumer Lending")
    if "ai_infrastructure_data_centers" in archetypes:
        add("AI Data Centers")
        add("AI Infrastructure")
        if dominant_role in {"infrastructure_operator", "end_platform_operator"}:
            add("Data Center Infrastructure")
    if "aerospace_defense_space_systems" in archetypes:
        if "missile" in description:
            add("Missile Defense Systems")
        if "propulsion" in description or "engine" in description:
            add("Aerospace Propulsion")
        add("Defense Systems")
        add("Space Systems")
    if "networking_interconnect" in archetypes:
        if dominant_role != "materials_supplier":
            add("Optical Interconnects")
            if "data center" in description or "hyperscale" in description:
                add("Data Center Optics")
            add("Optical Networking")
    if (
        "software_devops_cloud" in archetypes
        and dominant_role == "software_service_provider"
        and not strong_digital_asset_infrastructure
        and not connected_operations_iot_signals
        and (
        explicit_cloud_software_signals or not native_descriptors
        )
    ):
        add("Cloud Software")
    if (domain_anchor == "unclear" and not dominant_role) or not ideas:
        for label in phrase_based_ideas:
            add(label)
    if not ideas:
        fallback = _candidate_new_theme_label(profile, candidate)
        add(fallback)
    return _collapse_autonomy_stack_labels(_sort_theme_ideas_by_specificity(ideas), description)[:5]


def _direct_phrase_theme_ideas(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    ideas: list[str] = []

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in ideas:
            ideas.append(normalized)

    for family in DIRECT_PHRASE_THEME_FAMILIES:
        aliases = tuple(family.get("aliases") or ())
        if not any(_contains_phrase(normalized_text, alias) for alias in aliases):
            continue
        direct_specific_match = False
        component_phrases = tuple(family.get("component_phrases") or ())
        module_phrases = tuple(family.get("module_phrases") or ())
        systems_phrases = tuple(family.get("systems_phrases") or ())
        equipment_phrases = tuple(family.get("equipment_phrases") or ())
        industry_phrases = tuple(family.get("industry_phrases") or ())
        if any(_contains_phrase(normalized_text, phrase) for phrase in component_phrases):
            add(str(family.get("component_label") or f"{family.get('base_label')} Components"))
            direct_specific_match = True
        if any(_contains_phrase(normalized_text, phrase) for phrase in module_phrases):
            add(str(family.get("component_label") or f"{family.get('base_label')} Components"))
            direct_specific_match = True
        if any(_contains_phrase(normalized_text, phrase) for phrase in systems_phrases):
            add(str(family.get("systems_label") or family.get("base_label")))
            direct_specific_match = True
        if any(_contains_phrase(normalized_text, phrase) for phrase in equipment_phrases):
            add(str(family.get("equipment_label") or f"{family.get('base_label')} Equipment"))
            direct_specific_match = True
        if direct_specific_match or any(_contains_phrase(normalized_text, phrase) for phrase in industry_phrases):
            add(str(family.get("base_label") or ""))
    return _sort_theme_ideas_by_specificity(ideas)[:5]


def _canonicalize_direct_family_theme_label(label: str | None) -> str | None:
    normalized = _normalize_optional_theme_label(label)
    if not normalized:
        return None
    normalized_lower = normalized.lower()
    for family in DIRECT_PHRASE_THEME_FAMILIES:
        aliases = tuple(family.get("aliases") or ())
        if not any(_contains_phrase(normalized_lower, alias) for alias in aliases):
            continue
        if any(_contains_phrase(normalized_lower, token) for token in ("component", "components", "module", "modules")):
            return str(family.get("component_label") or f"{family.get('base_label')} Components")
        if any(_contains_phrase(normalized_lower, token) for token in ("system", "systems")):
            return str(family.get("systems_label") or family.get("base_label"))
        if any(_contains_phrase(normalized_lower, token) for token in ("equipment", "equipments")):
            return str(family.get("equipment_label") or f"{family.get('base_label')} Equipment")
        return str(family.get("base_label") or normalized)
    return normalized


def _theme_idea_specificity_rank(label: str) -> tuple[int, int]:
    normalized = _normalize_optional_theme_label(label) or ""
    normalized_lower = normalized.lower()
    family_index = len(DIRECT_PHRASE_THEME_FAMILIES)
    family_specificity = 9
    for idx, family in enumerate(DIRECT_PHRASE_THEME_FAMILIES):
        aliases = tuple(family.get("aliases") or ())
        if not any(_contains_phrase(normalized_lower, alias) for alias in aliases):
            continue
        family_index = idx
        if any(_contains_phrase(normalized_lower, token) for token in ("component", "components", "module", "modules")):
            family_specificity = 0
        elif any(_contains_phrase(normalized_lower, token) for token in ("system", "systems", "equipment", "equipments")):
            family_specificity = 1
        else:
            family_specificity = 2
        break
    return family_index, family_specificity


def _sort_theme_ideas_by_specificity(ideas: list[str]) -> list[str]:
    if len(ideas) <= 1:
        return ideas
    indexed = list(enumerate(ideas))
    indexed.sort(key=lambda item: (_theme_idea_specificity_rank(item[1]), item[0]))
    return [item[1] for item in indexed]


def _generic_product_phrase_theme_ideas(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    ideas: list[str] = []

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in ideas:
            ideas.append(normalized)

    for match in re.finditer(
        r"\b([a-z][a-z0-9-]*(?:\s+[a-z][a-z0-9-]*){0,2})\s+(components?|modules?|systems?|equipment|devices?)\b",
        normalized_text,
    ):
        modifier_text = str(match.group(1) or "").strip()
        head = str(match.group(2) or "").strip()
        head_label = GENERIC_PRODUCT_HEAD_LABELS.get(head)
        if not head_label:
            continue
        modifier_tokens = [
            token
            for token in re.findall(r"[a-z0-9-]+", modifier_text)
            if token not in GENERIC_PRODUCT_MODIFIER_STOPWORDS and token not in STOPWORDS
        ]
        if not modifier_tokens:
            continue
        if all(token in {"mission", "critical", "platform", "software", "workflow"} for token in modifier_tokens):
            continue
        cleaned_modifier = " ".join(token.capitalize() for token in modifier_tokens[-2:])
        add(f"{cleaned_modifier} {head_label}")
    return ideas[:4]


def _extractive_resource_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    if not any(_contains_phrase(normalized_text, signal) for signal in EXTRACTIVE_RESOURCE_SIGNALS):
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    commodity_labels = [
        label
        for token, label in EXTRACTIVE_RESOURCE_COMMODITIES.items()
        if _contains_phrase(normalized_text, token)
    ]
    for commodity_label in commodity_labels:
        add(f"{commodity_label} Mining")
        if _contains_phrase(normalized_text, "concentrate") or _contains_phrase(normalized_text, "ore"):
            add(f"{commodity_label} Mineral Processing")
    if commodity_labels:
        add("Metals & Mining")
        if {"tungsten", "lithium", "nickel", "uranium", "graphite", "cobalt", "rare earth", "rare earths"} & {
            token for token in EXTRACTIVE_RESOURCE_COMMODITIES if _contains_phrase(normalized_text, token)
        }:
            add("Critical Minerals Mining")
    elif _contains_phrase(normalized_text, "mineral processing"):
        add("Mining & Mineral Processing")
    elif _contains_phrase(normalized_text, "mining") or _contains_phrase(normalized_text, "mine"):
        add("Metals & Mining")
    return descriptors[:5]


def _upstream_oil_gas_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _normalize_optional_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    if any(_contains_phrase(normalized_text, token) for token in UPSTREAM_OIL_GAS_DIRECT_TOKENS):
        add("Oil & Gas Exploration & Production")
        add("Upstream Oil & Gas")
        return descriptors[:5]

    oil_gas_tokens = (
        _contains_phrase(normalized_text, "oil and gas")
        or _contains_phrase(normalized_text, "natural gas")
        or (_contains_phrase(normalized_text, "oil") and _contains_phrase(normalized_text, "gas"))
    )
    operational_tokens = sum(
        int(_contains_phrase(normalized_text, token))
        for token in (
            "exploration",
            "explore",
            "development",
            "develop",
            "production",
            "produce",
            "producing",
            "acquisition",
            "acquire",
            "acquiring",
            "assets",
            "properties",
            "working interests",
            "operated interests",
            "onshore",
            "basin",
            "acreage",
            "wells",
            "reserves",
        )
    )
    if oil_gas_tokens and operational_tokens >= 2:
        add("Oil & Gas Exploration & Production")
        if any(_contains_phrase(normalized_text, token) for token in ("upstream", "working interests", "operated interests", "onshore", "basin", "acreage", "wells", "reserves")):
            add("Upstream Oil & Gas")
    return descriptors[:5]


def _connected_operations_platform_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _normalize_optional_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    software_hits = sum(int(_contains_phrase(normalized_text, token)) for token in CONNECTED_OPERATIONS_SOFTWARE_TOKENS)
    physical_hits = sum(int(_contains_phrase(normalized_text, token)) for token in CONNECTED_OPERATIONS_PHYSICAL_TOKENS)
    operational_hits = sum(
        int(_contains_phrase(normalized_text, token))
        for token in {
            "connected assets",
            "connected devices",
            "fleet",
            "fleets",
            "vehicle",
            "vehicles",
            "equipment",
            "physical operations",
            "field operations",
            "operations data",
            "telematics",
            "monitoring",
            "workflow",
            "third-party systems",
        }
    )
    if software_hits >= 2 and physical_hits >= 2 and operational_hits >= 3:
        add("IoT / Connected Operations Platform")
        if (
            any(_contains_phrase(normalized_text, token) for token in ("fleet", "fleets", "vehicle", "vehicles", "telematics"))
            and not any(_contains_phrase(normalized_text, token) for token in ("connected assets", "physical operations", "field operations", "third-party systems"))
        ):
            add("Industrial / Fleet Telematics Software")
    return descriptors[:5]


def _industrial_additive_manufacturing_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _normalize_optional_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    additive_signals = any(_contains_phrase(normalized_text, phrase) for phrase in INDUSTRIAL_ADDITIVE_MANUFACTURING_SIGNALS)
    system_signals = any(_contains_phrase(normalized_text, phrase) for phrase in INDUSTRIAL_MANUFACTURING_SYSTEM_SIGNALS)
    industrial_context = any(_contains_phrase(normalized_text, phrase) for phrase in INDUSTRIAL_MANUFACTURING_CONTEXT_SIGNALS)
    printer_hardware_signals = any(
        _contains_phrase(normalized_text, phrase)
        for phrase in {
            "printer",
            "printers",
            "printing system",
            "printing systems",
            "hardware system",
            "hardware systems",
        }
    )
    software_signals = any(_contains_phrase(normalized_text, phrase) for phrase in {"software", "software platform", "workflow software"})
    hardware_tied_software = software_signals and (system_signals or printer_hardware_signals) and industrial_context

    if additive_signals:
        add("Additive Manufacturing")
    if additive_signals and (industrial_context or system_signals or printer_hardware_signals):
        add("Industrial 3D Printing")
    if system_signals and (additive_signals or industrial_context or hardware_tied_software):
        add("Industrial Manufacturing Systems")
    elif hardware_tied_software and additive_signals:
        add("Industrial Manufacturing Systems")
    return descriptors[:5]


def _autonomy_stack_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _normalize_optional_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    autonomy_signals = any(
        _contains_phrase(normalized_text, token)
        for token in (
            "autonomous",
            "autonomy",
            "robotics",
            "robotic",
            "drone",
            "drones",
            "uav",
            "uavs",
            "unmanned",
            "uncrewed",
        )
    )
    if not autonomy_signals:
        return []

    direct_phrase_ideas = _direct_phrase_theme_ideas(normalized_text)
    if any(
        any(token in _normalize_text(label).lower() for token in ("components", "component", "equipment"))
        for label in direct_phrase_ideas
    ):
        return []

    has_uncrewed_signals = any(
        _contains_phrase(normalized_text, token)
        for token in (
            "drone",
            "drones",
            "uav",
            "uavs",
            "unmanned",
            "uncrewed",
            "unmanned platform",
            "unmanned platforms",
            "uncrewed system",
            "uncrewed systems",
            "unmanned system",
            "unmanned systems",
        )
    )
    has_robotics_signals = any(
        _contains_phrase(normalized_text, token)
        for token in (
            "robotics",
            "robotic",
            "autonomous robotics",
            "robotics hardware",
        )
    )
    has_autonomous_vehicle_signals = any(
        _contains_phrase(normalized_text, token)
        for token in (
            "autonomous vehicle",
            "autonomous vehicles",
            "driverless vehicle",
            "driverless vehicles",
        )
    )

    if has_uncrewed_signals or has_robotics_signals:
        add("Autonomous Robotics / Uncrewed Systems")
        add("Autonomous Systems")
    elif has_autonomous_vehicle_signals:
        add("Autonomous Vehicles")
    elif any(_contains_phrase(normalized_text, token) for token in ("autonomous systems", "autonomous system")):
        add("Autonomous Systems")
    return descriptors[:3]


def _theme_looks_industrial_manufacturing_drift(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(_contains_phrase(theme_text, token) for token in INDUSTRIAL_MANUFACTURING_DRIFT_TOKENS)


def _theme_looks_autonomous_component_drift(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    if any(_contains_phrase(theme_text, token) for token in AUTONOMOUS_SYSTEM_DIRECT_TOKENS):
        return False
    return any(_contains_phrase(theme_text, token) for token in AUTONOMOUS_COMPONENT_DRIFT_TOKENS)


def _theme_looks_autonomous_product_family_bucket(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    if not any(_contains_phrase(theme_text, token) for token in AUTONOMOUS_SYSTEM_DIRECT_TOKENS):
        return False
    return not any(_contains_phrase(theme_text, token) for token in ("component", "components", "module", "modules", "part", "parts"))


def _has_strong_digital_asset_infrastructure_signals(text: object) -> bool:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return False
    if any(_contains_phrase(normalized_text, token) for token in DIGITAL_ASSET_INFRASTRUCTURE_DIRECT_TOKENS):
        return True
    return (
        ("stablecoin" in normalized_text or "stablecoins" in normalized_text)
        and any(token in normalized_text for token in ("payments", "settlement", "infrastructure", "rails"))
    )


def _theme_supports_digital_asset_infrastructure(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(_contains_phrase(theme_text, token) for token in DIGITAL_ASSET_INFRASTRUCTURE_DIRECT_TOKENS)


def _theme_looks_digital_asset_infrastructure_drift(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    if _theme_supports_digital_asset_infrastructure(theme_entry):
        return False
    return any(_contains_phrase(theme_text, token) for token in DIGITAL_ASSET_INFRASTRUCTURE_GENERIC_DRIFT_TOKENS)


def _theme_supports_upstream_oil_gas(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(_contains_phrase(theme_text, token) for token in UPSTREAM_OIL_GAS_DIRECT_TOKENS) or (
        _contains_phrase(theme_text, "oil")
        and _contains_phrase(theme_text, "gas")
        and any(_contains_phrase(theme_text, token) for token in ("exploration", "production", "upstream"))
    )


def _theme_looks_upstream_oil_gas_drift(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    if _theme_supports_upstream_oil_gas(theme_entry):
        return False
    return any(_contains_phrase(theme_text, token) for token in UPSTREAM_OIL_GAS_DRIFT_TOKENS)


def _theme_looks_upstream_oil_gas_geography_bucket(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(_contains_phrase(theme_text, token) for token in UPSTREAM_OIL_GAS_GEOGRAPHY_BUCKET_TOKENS)


def _theme_supports_connected_operations_iot(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    software_hits = sum(int(_contains_phrase(theme_text, token)) for token in CONNECTED_OPERATIONS_SOFTWARE_TOKENS)
    physical_hits = sum(int(_contains_phrase(theme_text, token)) for token in CONNECTED_OPERATIONS_PHYSICAL_TOKENS)
    return software_hits >= 2 and physical_hits >= 1 and any(
        _contains_phrase(theme_text, token)
        for token in ("connected operations", "industrial iot", "connected asset", "fleet", "telematics")
    )


def _theme_looks_connected_operations_iot_drift(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    if _theme_supports_connected_operations_iot(theme_entry):
        return False
    return any(_contains_phrase(theme_text, token) for token in CONNECTED_OPERATIONS_DRIFT_TOKENS)


def _description_rule_business_descriptors(text: object) -> list[str]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return []
    descriptors: list[str] = []

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in descriptors:
            descriptors.append(normalized)

    for rule in DESCRIPTION_NATIVE_DESCRIPTOR_RULES:
        if any(_contains_phrase(normalized_text, phrase) for phrase in tuple(rule.get("phrases") or ())):
            add(str(rule.get("label") or ""))
    return descriptors[:5]


def _descriptor_text_support_score(descriptor: str, normalized_text: str) -> int:
    score = 0
    descriptor_lower = _normalize_text(descriptor).lower()
    if descriptor_lower and _contains_phrase(normalized_text, descriptor_lower):
        score += 8
    for token in _tokenize(descriptor_lower):
        if token in STOPWORDS or len(token) <= 2:
            continue
        if _contains_phrase(normalized_text, token):
            score += 2
    layers = _descriptor_value_chain_layers([descriptor])
    if {"materials", "merchant_input", "component", "module", "device"} & layers:
        score += 5
    elif {"system", "extractive_resource"} & layers:
        score += 4
    elif {"platform", "network_service"} & layers:
        score += 3
    elif {"software_application"} & layers:
        score += 1
    families = _descriptor_families([descriptor])
    if families:
        score += 1
    return score


def _is_autonomy_stack_label(label: object) -> bool:
    normalized = _normalize_text(label).lower()
    if not normalized:
        return False
    return normalized in {
        "ai - robotics",
        "robotics & automation",
        "industrial robotics",
        "autonomous systems",
        "autonomous vehicles",
        "autonomous robotics / uncrewed systems",
        "uncrewed systems",
        "unmanned systems",
    }


def _is_communications_stack_label(label: object) -> bool:
    normalized = _normalize_text(label).lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "software-defined radio",
            "software defined radio",
            "sdr",
            "wireless communications",
            "communications equipment",
            "wireless equipment",
            "radio infrastructure",
            "communications infrastructure",
        )
    )


def _collapse_autonomy_stack_labels(labels: list[str], normalized_text: str) -> list[str]:
    if not labels:
        return []
    text = _normalize_text(normalized_text).lower()
    autonomy_indices = [idx for idx, label in enumerate(labels) if _is_autonomy_stack_label(label)]
    communication_indices = [idx for idx, label in enumerate(labels) if _is_communications_stack_label(label)]
    if not autonomy_indices:
        deduped: list[str] = []
        for label in labels:
            if label not in deduped:
                deduped.append(label)
        return deduped

    has_uncrewed_signals = any(
        _contains_phrase(text, token)
        for token in (
            "drone",
            "drones",
            "uav",
            "uavs",
            "unmanned",
            "uncrewed",
            "unmanned aircraft",
            "uncrewed aircraft",
            "unmanned platform",
            "unmanned platforms",
        )
    ) or any(
        any(token in _normalize_text(label).lower() for token in ("drone", "uav", "unmanned", "uncrewed"))
        for label in labels
    )
    has_robotics_signals = any(
        _contains_phrase(text, token)
        for token in (
            "robotics",
            "robotic",
            "autonomous robotics",
        )
    ) or any("robotics" in _normalize_text(label).lower() for label in labels)
    has_autonomous_vehicle_signals = any(
        _contains_phrase(text, token)
        for token in (
            "autonomous vehicle",
            "autonomous vehicles",
            "driverless vehicle",
            "driverless vehicles",
        )
    ) or any("autonomous vehicles" in _normalize_text(label).lower() for label in labels)

    if has_uncrewed_signals or has_robotics_signals:
        autonomy_primary = "Autonomous Robotics / Uncrewed Systems"
    elif has_autonomous_vehicle_signals:
        autonomy_primary = "Autonomous Vehicles"
    else:
        autonomy_primary = "Autonomous Systems"

    communications_secondary = None
    if any(
        _contains_phrase(text, token)
        for token in ("software-defined radio", "software defined radio", "sdr")
    ) or any("software-defined radio" in _normalize_text(label).lower() for label in labels):
        communications_secondary = "Software-Defined Radio"
    elif communication_indices:
        communications_secondary = "Wireless Communications Infrastructure"

    cleaned: list[str] = []
    inserted_primary = False
    inserted_secondary = False
    primary_insert_index = min(autonomy_indices)
    for idx, label in enumerate(labels):
        if idx == primary_insert_index and not inserted_primary:
            cleaned.append(autonomy_primary)
            inserted_primary = True
            if communications_secondary:
                cleaned.append(communications_secondary)
                inserted_secondary = True
        if idx in autonomy_indices or idx in communication_indices:
            continue
        if label not in cleaned:
            cleaned.append(label)

    if not inserted_primary:
        cleaned.insert(0, autonomy_primary)
    if communications_secondary and not inserted_secondary and communications_secondary not in cleaned:
        insert_at = cleaned.index(autonomy_primary) + 1 if autonomy_primary in cleaned else 0
        cleaned.insert(insert_at, communications_secondary)

    deduped: list[str] = []
    for label in cleaned:
        if label not in deduped:
            deduped.append(label)
    return deduped


def _rank_description_business_descriptors(descriptors: list[tuple[str, int]], normalized_text: str) -> list[str]:
    if len(descriptors) <= 1:
        return _collapse_autonomy_stack_labels([item[0] for item in descriptors], normalized_text)
    indexed = list(enumerate(descriptors))
    indexed.sort(
        key=lambda item: (
            item[1][1],
            -_descriptor_text_support_score(item[1][0], normalized_text),
            _theme_idea_specificity_rank(item[1][0]),
            len(_normalize_text(item[1][0])),
            item[0],
        )
    )
    ranked = [item[1][0] for item in indexed]
    selected: list[str] = []
    seen_family_keys: set[str] = set()
    for descriptor in ranked:
        family_key = "|".join(sorted(_descriptor_families([descriptor]))) or _normalize_text(descriptor).lower()
        if family_key not in seen_family_keys or len(selected) == 0:
            selected.append(descriptor)
            seen_family_keys.add(family_key)
        if len(selected) >= 3:
            break
    return _collapse_autonomy_stack_labels(selected, normalized_text)[:3]


def _refine_ranked_description_descriptors(descriptors: list[str], normalized_text: str) -> list[str]:
    text = _normalize_text(normalized_text).lower()
    if not text:
        return list(descriptors or [])[:3]

    refined: list[str] = []

    def add(label: str | None) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in refined:
            refined.append(normalized)

    optical_signals = any(
        _contains_phrase(text, token)
        for token in (
            "optical",
            "optics",
            "transceiver",
            "transceivers",
            "fiber",
            "fiber-optic",
            "interconnect",
            "photonic",
            "photonics",
        )
    )
    additive_signals = any(_contains_phrase(text, token) for token in INDUSTRIAL_ADDITIVE_MANUFACTURING_SIGNALS)
    mining_signals = any(_contains_phrase(text, token) for token in EXTRACTIVE_RESOURCE_SIGNALS)
    tungsten_signals = _contains_phrase(text, "tungsten")
    upstream_signals = bool(_upstream_oil_gas_descriptors(text))
    payments_and_lending_signals = (
        any(_contains_phrase(text, token) for token in ("payment", "payments", "merchant", "checkout"))
        and any(_contains_phrase(text, token) for token in ("lending", "loan", "loans", "installment", "bnpl", "consumer finance"))
    )
    memory_signals = any(
        _contains_phrase(text, token)
        for token in ("semiconductor memory", "memory semiconductors", "nand flash", "flash memory", "storage controller", "storage controllers")
    )

    if optical_signals:
        if any(_contains_phrase(text, token) for token in ("interconnect", "optical module", "optical modules")):
            add("Optical Interconnects")
        add("Optical Networking")

    if additive_signals:
        add("Additive Manufacturing")
        if any(_contains_phrase(text, token) for token in ("industrial 3d printing", "3d printer", "3d printers", "industrial printer", "industrial printers")):
            add("Industrial 3D Printing")
        if any(_contains_phrase(text, token) for token in ("manufacturing systems", "printer systems", "printing systems", "production systems", "fabrication systems", "manufacturing software", "production software")):
            add("Industrial Manufacturing Systems")

    if tungsten_signals and mining_signals:
        add("Tungsten Mining")
        if any(_contains_phrase(text, token) for token in ("concentrate", "ore", "mineral processing")):
            add("Critical Minerals Mining")
        add("Metals & Mining")
    elif mining_signals:
        for label in _extractive_resource_descriptors(text):
            if "mineral processing" not in _normalize_text(label).lower():
                add(label)

    if upstream_signals:
        add("Oil & Gas Exploration & Production")
        if any(_contains_phrase(text, token) for token in ("upstream", "working interests", "operated interests", "onshore", "acreage", "wells", "reserves")):
            add("Upstream Oil & Gas")

    if payments_and_lending_signals:
        add("Digital Payments")
        add("Fintech Payments")
        add("Consumer Lending")

    if memory_signals:
        add("Semiconductor Memory")
        add("Memory & Storage")
        add("Data Storage")

    generic_communications_labels = {
        "Wireless Communications Infrastructure",
        "Communications Equipment",
    }
    generic_processing_labels = {"Tungsten Mineral Processing"}

    for label in descriptors:
        normalized = _canonicalize_direct_family_theme_label(label)
        if not normalized:
            continue
        if optical_signals and normalized in generic_communications_labels:
            continue
        if tungsten_signals and normalized in generic_processing_labels:
            continue
        if upstream_signals and normalized == "Upstream Oil & Gas" and "Oil & Gas Exploration & Production" in refined:
            continue
        if payments_and_lending_signals and normalized == "Consumer Lending" and {"Digital Payments", "Fintech Payments"} & set(refined):
            continue
        add(normalized)

    return refined[:3]


def _description_business_descriptor_bundle(text: object) -> dict[str, object]:
    normalized_text = _normalize_text(text).lower()
    if not normalized_text:
        return {"descriptors": [], "value_chain_layers": set(), "descriptor_families": set()}

    descriptor_candidates: list[tuple[str, int]] = []

    def add(label: str | None, *, source_rank: int) -> None:
        normalized = _canonicalize_direct_family_theme_label(label)
        if normalized and normalized not in {item[0] for item in descriptor_candidates}:
            descriptor_candidates.append((normalized, source_rank))

    for label in _direct_phrase_theme_ideas(normalized_text):
        add(label, source_rank=0)
    for label in _description_rule_business_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _connected_operations_platform_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _autonomy_stack_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _industrial_additive_manufacturing_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _extractive_resource_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _upstream_oil_gas_descriptors(normalized_text):
        add(label, source_rank=1)
    for label in _generic_product_phrase_theme_ideas(normalized_text):
        add(label, source_rank=3)

    descriptors = _refine_ranked_description_descriptors(
        _rank_description_business_descriptors(descriptor_candidates, normalized_text),
        normalized_text,
    )[:3]
    return {
        "descriptors": descriptors,
        "value_chain_layers": _descriptor_value_chain_layers(descriptors),
        "descriptor_families": _descriptor_families(descriptors),
    }


def _description_native_business_descriptors(text: object) -> list[str]:
    return list(_description_business_descriptor_bundle(text).get("descriptors") or [])


def _descriptor_value_chain_layers(descriptors: list[str]) -> set[str]:
    layers: set[str] = set()
    for descriptor in descriptors:
        normalized = _normalize_text(descriptor).lower()
        for layer, keywords in VALUE_CHAIN_LAYER_KEYWORDS.items():
            if any(_contains_phrase(normalized, keyword) for keyword in keywords):
                layers.add(layer)
    return layers


def _descriptor_families(descriptors: list[str]) -> set[str]:
    families: set[str] = set()
    for descriptor in descriptors:
        normalized = _normalize_text(descriptor).lower()
        for rule in DESCRIPTION_NATIVE_DESCRIPTOR_RULES:
            if _normalize_text(rule.get("label")).lower() == normalized:
                family = _normalize_text(rule.get("family"))
                if family:
                    families.add(family)
    if any("optical" in _normalize_text(descriptor).lower() or "photonic" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("optical_networking")
    if any("digital asset" in _normalize_text(descriptor).lower() or "crypto" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("digital_asset_infrastructure")
    if any("memory" in _normalize_text(descriptor).lower() or "storage" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("memory_storage")
    if any("radio" in _normalize_text(descriptor).lower() or "wireless" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("wireless_systems")
    if any("autonomous" in _normalize_text(descriptor).lower() or "drone" in _normalize_text(descriptor).lower() or "unmanned" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("autonomous_systems")
    if any("banking" in _normalize_text(descriptor).lower() or "fintech" in _normalize_text(descriptor).lower() for descriptor in descriptors):
        families.add("consumer_fintech")
    if any(
        "connected operations" in _normalize_text(descriptor).lower()
        or "fleet telematics" in _normalize_text(descriptor).lower()
        or "industrial iot" in _normalize_text(descriptor).lower()
        for descriptor in descriptors
    ):
        families.add("connected_operations_iot")
    if any(
        "upstream oil" in _normalize_text(descriptor).lower()
        or "exploration & production" in _normalize_text(descriptor).lower()
        or "oil & gas" in _normalize_text(descriptor).lower()
        for descriptor in descriptors
    ):
        families.add("upstream_oil_gas")
    if any(
        "mining" in _normalize_text(descriptor).lower()
        or "mineral processing" in _normalize_text(descriptor).lower()
        or "metals & mining" in _normalize_text(descriptor).lower()
        or "critical minerals" in _normalize_text(descriptor).lower()
        for descriptor in descriptors
    ):
        families.add("extractive_resources")
    return families


def _merchant_input_evidence(text: object) -> bool:
    normalized_text = _normalize_text(text).lower()
    if not any(_contains_phrase(normalized_text, token) for token in MERCHANT_INPUT_EVIDENCE_KEYWORDS):
        return False
    sales_patterns = (
        "supplier of",
        "suppliers of",
        "maker of",
        "makers of",
        "provider of",
        "providers of",
        "manufactures",
        "manufacture",
        "produces",
        "producer of",
        "sells",
        "selling",
        "supplies",
        "supply",
    )
    if any(_contains_phrase(normalized_text, pattern) for pattern in sales_patterns):
        return True
    if any(_contains_phrase(normalized_text, pattern) for pattern in MERCHANT_INPUT_PROCESS_CONTEXTS):
        return False
    return True


def _umbrella_signals(text: object) -> set[str]:
    normalized_text = _normalize_text(text).lower()
    signals: set[str] = set()
    for signal, keywords in UMBRELLA_SIGNAL_KEYWORDS.items():
        if any(_contains_phrase(normalized_text, keyword) for keyword in keywords):
            signals.add(signal)
    return signals


def _theme_looks_merchant_input(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return _merchant_input_evidence(theme_text)


def _theme_looks_generic_umbrella(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return bool(_umbrella_signals(theme_text) or _looks_generic_theme(theme_entry) or _is_generic_factor_theme(theme_entry))


def _theme_cluster_key(theme_entry: dict[str, object]) -> str:
    if _theme_looks_generic_umbrella(theme_entry):
        umbrellas = sorted(_umbrella_signals(" ".join(
            [
                _normalize_text(theme_entry.get("theme_name")),
                _normalize_text(theme_entry.get("category")),
                _normalize_text(theme_entry.get("theme_description")),
            ]
        )))
        if umbrellas:
            return "umbrella:" + umbrellas[0]
    archetypes = sorted(theme_entry.get("_theme_archetypes") or [])
    if archetypes:
        return "archetype:" + str(archetypes[0])
    return "theme:" + _normalize_text(theme_entry.get("theme_name")).lower()


def _generated_idea_analysis(idea: str) -> dict[str, object]:
    normalized = _normalize_text(idea)
    concepts = _infer_concepts(normalized)
    roles = _infer_signals(ROLE_KEYWORDS, normalized)
    markets = _infer_signals(END_MARKET_KEYWORDS, normalized)
    archetypes = _ranked_archetypes(
        _infer_archetype_scores(normalized, roles=roles, concepts=concepts, markets=markets),
        threshold=1,
    )
    economic_roles = _ranked_archetypes(
        _infer_economic_role_scores(normalized, roles=roles, archetypes=archetypes),
        threshold=1,
    )
    idea_layers = _descriptor_value_chain_layers([normalized])
    idea_families = _descriptor_families([normalized])
    if "autonomous_systems" in idea_families:
        roles.discard("server_systems")
        roles.discard("software_tooling")
        archetypes.discard("ai_infrastructure_data_centers")
        archetypes.discard("software_devops_cloud")
        economic_roles.discard("software_service_provider")
        economic_roles.discard("infrastructure_operator")
        economic_roles.discard("end_platform_operator")
    if "software_tooling" in roles and _contains_phrase(normalized.lower(), "software-defined radio"):
        roles.discard("software_tooling")
        economic_roles.discard("software_service_provider")
        archetypes.discard("software_devops_cloud")
    if "digital_asset_infrastructure" in idea_families and _has_strong_digital_asset_infrastructure_signals(normalized):
        roles.discard("software_tooling")
        archetypes.discard("software_devops_cloud")
        economic_roles.discard("software_service_provider")
    if not _merchant_input_evidence(normalized) and idea_layers & {
        "component",
        "module",
        "device",
        "system",
        "platform",
        "software_application",
        "network_service",
    } or (not _merchant_input_evidence(normalized) and idea_families):
        roles.discard("semiconductor_materials")
        economic_roles.discard("materials_supplier")
        archetypes.discard("semiconductor_materials_electronics_materials")
    return {
        "idea_tokens": _tokenize(normalized),
        "idea_concepts": concepts,
        "idea_roles": roles,
        "idea_markets": markets,
        "idea_archetypes": archetypes,
        "idea_economic_roles": economic_roles,
    }


def _theme_match_from_generated_idea(
    idea: str,
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    domain_anchor: str,
    dominant_business_role: str,
) -> dict[str, object]:
    prepared_theme = theme_entry if "_theme_tokens" in theme_entry else _preprocessed_theme_entry(theme_entry)
    normalized_idea = _normalize_text(idea).lower()
    normalized_theme_name = _normalize_text(prepared_theme.get("theme_name")).lower()
    candidate_analysis = _candidate_analysis(profile, candidate)
    generic_business_model_only = bool(candidate_analysis.get("generic_business_model_only"))
    clear_business_descriptor = bool(candidate_analysis.get("clear_business_descriptor"))
    merchant_input_evidence = bool(candidate_analysis.get("merchant_input_evidence"))
    value_chain_layers = set(candidate_analysis.get("value_chain_layers") or set())
    descriptor_families = set(candidate_analysis.get("descriptor_families") or set())
    idea_analysis = _generated_idea_analysis(idea)
    idea_tokens = set(idea_analysis.get("idea_tokens") or set())
    idea_concepts = set(idea_analysis.get("idea_concepts") or set())
    idea_roles = set(idea_analysis.get("idea_roles") or set())
    idea_markets = set(idea_analysis.get("idea_markets") or set())
    idea_archetypes = set(idea_analysis.get("idea_archetypes") or set())
    idea_economic_roles = set(idea_analysis.get("idea_economic_roles") or set())
    theme_tokens = set(prepared_theme.get("_theme_tokens") or set())
    theme_concepts = set(prepared_theme.get("_theme_concepts") or set())
    theme_roles = set(prepared_theme.get("_theme_roles") or set())
    theme_markets = set(prepared_theme.get("_theme_markets") or set())
    theme_archetypes = set(prepared_theme.get("_theme_archetypes") or set())
    theme_economic_roles = set(prepared_theme.get("_theme_economic_roles") or set())
    token_overlap = sorted(idea_tokens & theme_tokens)
    specific_overlap = sorted((idea_concepts & theme_concepts) - WEAK_CONCEPTS)
    generic_overlap = sorted((idea_concepts & theme_concepts) & WEAK_CONCEPTS)
    role_overlap = sorted(idea_roles & theme_roles)
    market_overlap = sorted(idea_markets & theme_markets)
    archetype_overlap = sorted(idea_archetypes & theme_archetypes)
    economic_role_overlap = sorted(idea_economic_roles & theme_economic_roles)
    archetype_relation = _archetype_relation(idea_archetypes, theme_archetypes)
    fit_details = _theme_fit_details(prepared_theme, profile, candidate, candidate_analysis=candidate_analysis)
    score = (
        len(role_overlap) * 18
        + len(economic_role_overlap) * 14
        + len(archetype_overlap) * 12
        + len(specific_overlap) * 9
        + len(market_overlap) * 4
        + len(generic_overlap) * 2
        + min(2, len(token_overlap)) * 2
        + min(10, max(0, int(fit_details.get("score") or 0)) // 2)
    )
    if normalized_idea and normalized_theme_name:
        if normalized_idea == normalized_theme_name:
            score += 16
        elif normalized_idea in normalized_theme_name or normalized_theme_name in normalized_idea:
            score += 8
    if archetype_relation == "direct":
        score += 6
    elif archetype_relation == "adjacent":
        score += 2
    elif archetype_relation == "incompatible":
        score -= 18
    if idea_roles and theme_roles and not role_overlap:
        idea_families = {ROLE_FAMILY.get(role) for role in idea_roles if ROLE_FAMILY.get(role)}
        theme_families = {ROLE_FAMILY.get(role) for role in theme_roles if ROLE_FAMILY.get(role)}
        if idea_families and theme_families:
            score -= 12 if idea_families.isdisjoint(theme_families) else 4
    if idea_economic_roles and theme_economic_roles and not economic_role_overlap:
        score -= 12
    if idea_archetypes and theme_archetypes and archetype_relation == "incompatible":
        score -= 8
    if len(token_overlap) <= 1 and not role_overlap and not specific_overlap and not economic_role_overlap and not archetype_overlap:
        score -= 10
    if token_overlap and len(token_overlap) == 1 and token_overlap[0] in {"network", "networks", "materials", "systems", "devices", "device"}:
        score -= 6
    if _is_generic_factor_theme(prepared_theme) and bool(candidate_analysis.get("strong_role_evidence")):
        score -= 12
    weak_generic_business_model_overlap = generic_business_model_only and set(role_overlap) <= WEAK_ROLE_SIGNALS and set(economic_role_overlap) <= WEAK_ECONOMIC_ROLE_SIGNALS and not specific_overlap
    descriptor_led_generic_umbrella_overlap = (
        clear_business_descriptor
        and _theme_looks_generic_umbrella(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= WEAK_ECONOMIC_ROLE_SIGNALS
        and not specific_overlap
    )
    if weak_generic_business_model_overlap:
        score -= 8
    if descriptor_led_generic_umbrella_overlap:
        score -= 18
    industrial_manufacturing_drift_overlap = (
        "industrial_additive_manufacturing" in descriptor_families
        and _theme_looks_industrial_manufacturing_drift(prepared_theme)
        and not role_overlap
        and not specific_overlap
    )
    if industrial_manufacturing_drift_overlap:
        score -= 18
    autonomous_component_drift_overlap = (
        "autonomous_systems" in descriptor_families
        and bool({"component", "module"} & value_chain_layers)
        and _theme_looks_autonomous_component_drift(prepared_theme)
        and not role_overlap
        and not specific_overlap
        and not economic_role_overlap
    )
    if autonomous_component_drift_overlap:
        score -= 18
        direct_role_fit = False
    autonomous_product_family_overlap = (
        "autonomous_systems" in descriptor_families
        and bool({"component", "module"} & value_chain_layers)
        and _theme_looks_autonomous_product_family_bucket(prepared_theme)
        and not role_overlap
        and not economic_role_overlap
    )
    if autonomous_product_family_overlap:
        score -= 12
        direct_role_fit = False
        indirect_only_fit = True
    digital_asset_infrastructure_drift_overlap = (
        "digital_asset_infrastructure" in descriptor_families
        and _theme_looks_digital_asset_infrastructure_drift(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(specific_overlap) <= {"payments"}
        and set(economic_role_overlap) <= {"financial_platform", "software_service_provider", "end_platform_operator"}
    )
    if digital_asset_infrastructure_drift_overlap:
        score -= 18
        direct_role_fit = False
        indirect_only_fit = True
    upstream_oil_gas_drift_overlap = (
        "upstream_oil_gas" in descriptor_families
        and _theme_looks_upstream_oil_gas_drift(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= {"software_service_provider", "end_platform_operator", "infrastructure_operator"}
    )
    if upstream_oil_gas_drift_overlap:
        score -= 18
        direct_role_fit = False
        indirect_only_fit = True
    upstream_oil_gas_geography_overlap = (
        "upstream_oil_gas" in descriptor_families
        and _theme_looks_upstream_oil_gas_geography_bucket(prepared_theme)
        and not token_overlap
        and not specific_overlap
    )
    if upstream_oil_gas_geography_overlap:
        score -= 14
    connected_operations_iot_direct_support = (
        "connected_operations_iot" in descriptor_families
        and _theme_supports_connected_operations_iot(prepared_theme)
    )
    if connected_operations_iot_direct_support:
        score += 24
    connected_operations_iot_drift_overlap = (
        "connected_operations_iot" in descriptor_families
        and (
            _theme_looks_connected_operations_iot_drift(prepared_theme)
            or (
                _theme_looks_generic_umbrella(prepared_theme)
                and not _theme_supports_connected_operations_iot(prepared_theme)
            )
        )
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= {"software_service_provider", "end_platform_operator", "infrastructure_operator"}
        and not specific_overlap
    )
    if connected_operations_iot_drift_overlap:
        score -= 18
        direct_role_fit = False
        indirect_only_fit = True
    theme_anchor_alignment = "unclear"
    if domain_anchor != "unclear":
        theme_anchor = _domain_anchor(
            {"company_name": "", "description": _normalize_text(theme_entry.get("theme_description")), "sic_description": _normalize_text(theme_entry.get("category"))},
            {"recommendation_reason": _normalize_text(theme_entry.get("theme_name"))},
        )
        if theme_anchor == domain_anchor:
            score += 4
            theme_anchor_alignment = "match"
        elif theme_anchor != "unclear":
            score -= 8
            theme_anchor_alignment = "mismatch"
    core_overlap = bool(role_overlap or economic_role_overlap or specific_overlap)
    if weak_generic_business_model_overlap or descriptor_led_generic_umbrella_overlap:
        core_overlap = False
    if not core_overlap and market_overlap:
        score -= 8
    if not core_overlap and archetype_relation == "adjacent":
        score -= 4
    if dominant_business_role and dominant_business_role not in list(fit_details.get("theme_economic_roles") or []):
        if list(fit_details.get("theme_economic_roles") or []):
            score -= 8
    if bool(fit_details.get("indirect_only_fit")) and not core_overlap:
        score -= 6
    if generic_business_model_only and not core_overlap and archetype_relation != "direct":
        score -= 4
    if not merchant_input_evidence and _theme_looks_merchant_input(prepared_theme):
        score -= 12
        if not role_overlap and not specific_overlap and not economic_role_overlap:
            score = min(score, 4)
    if clear_business_descriptor and _theme_looks_generic_umbrella(prepared_theme) and not role_overlap and not specific_overlap and not economic_role_overlap:
        score -= 10
    if descriptor_led_generic_umbrella_overlap:
        score = min(score, 8 if market_overlap else 6)
    if industrial_manufacturing_drift_overlap:
        score = min(score, 4)
    if autonomous_component_drift_overlap:
        score = min(score, 4)
    if autonomous_product_family_overlap:
        score = min(score, 9 if specific_overlap else 7)
    if digital_asset_infrastructure_drift_overlap:
        score = min(score, 5)
    if upstream_oil_gas_drift_overlap:
        score = min(score, 5)
    if upstream_oil_gas_geography_overlap:
        score = min(score, 4)
    if connected_operations_iot_drift_overlap:
        score = min(score, 5)
    if clear_business_descriptor and "end_market_only" not in value_chain_layers and market_overlap and not core_overlap:
        score -= 5
    if archetype_relation == "incompatible" and not role_overlap and not specific_overlap and not economic_role_overlap:
        score = min(score, 2)
    if not role_overlap and not specific_overlap and not archetype_overlap and not economic_role_overlap and len(token_overlap) <= 1:
        score = min(score, 4)
    effective_role_overlap = bool(role_overlap or connected_operations_iot_direct_support)
    if weak_generic_business_model_overlap or descriptor_led_generic_umbrella_overlap or autonomous_component_drift_overlap or autonomous_product_family_overlap or digital_asset_infrastructure_drift_overlap or upstream_oil_gas_drift_overlap or upstream_oil_gas_geography_overlap or connected_operations_iot_drift_overlap:
        effective_role_overlap = False
    if effective_role_overlap:
        fit_label = "direct_fit"
    elif (economic_role_overlap and not descriptor_led_generic_umbrella_overlap) or archetype_relation in {"direct", "adjacent"} or specific_overlap or market_overlap:
        fit_label = "adjacent_fit"
    else:
        fit_label = "broad_fit"
    return {
        "score": score,
        "fit_details": fit_details,
        "token_overlap": token_overlap,
        "specific_overlap": specific_overlap,
        "role_overlap": role_overlap,
        "market_overlap": market_overlap,
        "archetype_overlap": archetype_overlap,
        "economic_role_overlap": economic_role_overlap,
        "archetype_relation": archetype_relation,
        "theme_anchor_alignment": theme_anchor_alignment,
        "fit_label": fit_label,
        "theme_entry": prepared_theme,
        "idea": idea,
    }


def _description_generated_match_is_actionable(match: dict[str, object]) -> bool:
    fit_details = dict(match.get("fit_details") or {})
    score = int(match.get("score") or 0)
    fit_label = str(match.get("fit_label") or "")
    role_overlap = bool(match.get("role_overlap"))
    economic_overlap = bool(match.get("economic_role_overlap"))
    specific_overlap = bool(match.get("specific_overlap"))
    archetype_overlap = bool(match.get("archetype_overlap"))
    market_overlap = bool(match.get("market_overlap"))
    anchor_alignment = str(match.get("theme_anchor_alignment") or "unclear")
    direct_archetype = str(match.get("archetype_relation") or "") == "direct"
    core_overlap = role_overlap or economic_overlap or specific_overlap

    if score <= 0:
        return False
    if bool(fit_details.get("descriptor_led_generic_umbrella_overlap")) and not specific_overlap:
        return False
    if fit_label == "direct_fit":
        return score >= 15 and (core_overlap or archetype_overlap or direct_archetype)
    if fit_label == "adjacent_fit":
        if not (core_overlap or archetype_overlap or direct_archetype):
            return False
        if economic_overlap and not (role_overlap or specific_overlap or archetype_overlap or direct_archetype):
            return score >= 18 and anchor_alignment != "mismatch"
        if bool(fit_details.get("indirect_only_fit")) and not core_overlap:
            return False
        if market_overlap and not core_overlap and anchor_alignment == "mismatch":
            return False
        return score >= 12
    return False


def _description_match_gate_reason(match: dict[str, object]) -> str:
    fit_details = dict(match.get("fit_details") or {})
    score = int(match.get("score") or 0)
    fit_label = str(match.get("fit_label") or "")
    role_overlap = bool(match.get("role_overlap"))
    economic_overlap = bool(match.get("economic_role_overlap"))
    specific_overlap = bool(match.get("specific_overlap"))
    archetype_overlap = bool(match.get("archetype_overlap"))
    market_overlap = bool(match.get("market_overlap"))
    anchor_alignment = str(match.get("theme_anchor_alignment") or "unclear")
    direct_archetype = str(match.get("archetype_relation") or "") == "direct"
    core_overlap = role_overlap or economic_overlap or specific_overlap

    if score <= 0:
        return "Rejected: non-positive score after role/domain penalties."
    if bool(fit_details.get("descriptor_led_generic_umbrella_overlap")) and not specific_overlap:
        return "Rejected: generic umbrella theme overlapped only on weak software/business-model signals."
    if fit_label == "direct_fit":
        if score < 15:
            return "Rejected: direct fit label but score stayed below direct-fit threshold."
        if not (core_overlap or archetype_overlap or direct_archetype):
            return "Rejected: direct fit label lacked role, concept, or archetype support."
        return "Passed: direct fit cleared score threshold with core role/domain support."
    if fit_label == "adjacent_fit":
        if not (core_overlap or archetype_overlap or direct_archetype):
            return "Rejected: adjacent fit relied on weak text overlap without role/domain support."
        if bool(fit_details.get("indirect_only_fit")) and not core_overlap:
            return "Rejected: adjacent fit was only indirect and lacked direct role/concept overlap."
        if market_overlap and not core_overlap and anchor_alignment == "mismatch":
            return "Rejected: market adjacency conflicted with the dominant domain anchor."
        if score < 12:
            return "Rejected: adjacent fit stayed below actionable score threshold."
        return "Passed: adjacent fit had enough role/domain evidence to remain reviewable."
    return "Rejected: broad fit was not actionable for description-first review."


def _description_match_debug_entry(match: dict[str, object]) -> dict[str, object]:
    fit_details = dict(match.get("fit_details") or {})
    entry = dict(match.get("theme_entry") or {})
    actionable = _description_generated_match_is_actionable(match)
    return {
        "idea": _normalize_text(match.get("idea")) or "n/a",
        "theme_name": _normalize_text(entry.get("theme_name")) or "n/a",
        "score": int(match.get("score") or 0),
        "fit_label": str(match.get("fit_label") or "broad_fit"),
        "actionable": actionable,
        "gate_reason": _description_match_gate_reason(match),
        "why": _normalize_text(fit_details.get("why")) or "No grounded fit reason.",
        "theme_anchor_alignment": str(match.get("theme_anchor_alignment") or "unclear"),
        "role_overlap": list(match.get("role_overlap") or []),
        "economic_role_overlap": list(match.get("economic_role_overlap") or []),
        "specific_overlap": list(match.get("specific_overlap") or []),
        "market_overlap": list(match.get("market_overlap") or []),
    }


def _description_match_support_text(match: dict[str, object]) -> str:
    fit_details = dict(match.get("fit_details") or {})
    fit_reason = _normalize_text(fit_details.get("why"))
    if fit_reason:
        return fit_reason
    idea = _normalize_text(match.get("idea"))
    if idea:
        return f"Generated idea: {idea}"
    return "Description-first match was tentative."


def _looks_generic_theme(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(token in theme_text for token in ["software", "cloud", "technology", "tech", "platform"])


def _is_generic_factor_theme(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(token in theme_text for token in GENERIC_FACTOR_THEME_TOKENS)


def _has_strong_role_evidence(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> bool:
    analysis = _candidate_analysis(profile, candidate, *extra_parts)
    return bool(analysis.get("strong_role_evidence"))


def _analysis_is_generic_business_model_only(analysis: dict[str, object]) -> bool:
    roles = set(analysis.get("candidate_roles") or set())
    archetypes = set(analysis.get("candidate_archetypes") or set())
    concepts = set(analysis.get("candidate_concepts") or set())
    economic_roles = set(analysis.get("candidate_economic_roles") or set())
    if not (roles or archetypes or concepts or economic_roles):
        return False
    return (
        not (roles - WEAK_ROLE_SIGNALS)
        and not (archetypes - WEAK_ARCHETYPE_SIGNALS)
        and not (concepts - WEAK_CONCEPTS)
        and not (economic_roles - WEAK_ECONOMIC_ROLE_SIGNALS)
    )


def _candidate_concepts(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    return _infer_concepts(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )


def _format_signal_names(values: set[str], display_map: dict[str, str], limit: int = 2) -> str:
    return ", ".join(display_map.get(value, value.replace("_", " ")) for value in sorted(values)[:limit])


def _dominant_role(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    roles = _candidate_roles(profile, candidate, *extra_parts)
    if roles:
        return sorted(roles)[0]
    return ""


def _theme_fit_details(
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    candidate_analysis: dict[str, object] | None = None,
) -> dict[str, object]:
    prepared_theme = theme_entry if "_theme_tokens" in theme_entry else _preprocessed_theme_entry(theme_entry)
    analysis = candidate_analysis or _candidate_analysis(profile, candidate)
    theme_tokens = set(prepared_theme.get("_theme_tokens") or set())
    profile_tokens = set(analysis.get("profile_tokens") or set())
    candidate_concepts = set(analysis.get("candidate_concepts") or set())
    theme_concepts = set(prepared_theme.get("_theme_concepts") or set())
    candidate_roles = set(analysis.get("candidate_roles") or set())
    candidate_markets = set(analysis.get("candidate_markets") or set())
    candidate_archetypes = set(analysis.get("candidate_archetypes") or set())
    candidate_economic_roles = set(analysis.get("candidate_economic_roles") or set())
    strong_role_evidence = bool(analysis.get("strong_role_evidence"))
    generic_business_model_only = bool(analysis.get("generic_business_model_only"))
    clear_business_descriptor = bool(analysis.get("clear_business_descriptor"))
    merchant_input_evidence = bool(analysis.get("merchant_input_evidence"))
    value_chain_layers = set(analysis.get("value_chain_layers") or set())
    descriptor_families = set(analysis.get("descriptor_families") or set())
    theme_roles = set(prepared_theme.get("_theme_roles") or set())
    theme_markets = set(prepared_theme.get("_theme_markets") or set())
    theme_archetypes = set(prepared_theme.get("_theme_archetypes") or set())
    theme_economic_roles = set(prepared_theme.get("_theme_economic_roles") or set())
    specific_overlap = sorted((candidate_concepts & theme_concepts) - WEAK_CONCEPTS)
    generic_overlap = sorted((candidate_concepts & theme_concepts) & WEAK_CONCEPTS)
    role_overlap = sorted(candidate_roles & theme_roles)
    market_overlap = sorted(candidate_markets & theme_markets)
    token_overlap = sorted((theme_tokens & profile_tokens) - STOPWORDS)
    archetype_overlap = sorted(candidate_archetypes & theme_archetypes)
    economic_role_overlap = sorted(candidate_economic_roles & theme_economic_roles)
    archetype_relation = _archetype_relation(candidate_archetypes, theme_archetypes)
    dominant_economic_role = str(analysis.get("dominant_economic_role") or "")

    score = (
        len(role_overlap) * 14
        + len(economic_role_overlap) * 12
        + len(archetype_overlap) * 10
        + len(specific_overlap) * 7
        + len(market_overlap) * 3
        + len(generic_overlap)
        + min(2, len(token_overlap))
    )
    direct_role_fit = bool(role_overlap)
    weak_generic_business_model_overlap = generic_business_model_only and set(role_overlap) <= WEAK_ROLE_SIGNALS and set(economic_role_overlap) <= WEAK_ECONOMIC_ROLE_SIGNALS and not specific_overlap
    descriptor_led_generic_umbrella_overlap = (
        clear_business_descriptor
        and _theme_looks_generic_umbrella(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= WEAK_ECONOMIC_ROLE_SIGNALS
        and not specific_overlap
    )
    if weak_generic_business_model_overlap:
        direct_role_fit = False
    if descriptor_led_generic_umbrella_overlap:
        direct_role_fit = False
    autonomous_product_family_overlap = False
    indirect_only_fit = not direct_role_fit and bool(market_overlap or generic_overlap or specific_overlap or archetype_relation == "adjacent")
    if theme_roles and candidate_roles and not role_overlap:
        candidate_families = {ROLE_FAMILY.get(role) for role in candidate_roles}
        theme_families = {ROLE_FAMILY.get(role) for role in theme_roles}
        if candidate_families.isdisjoint(theme_families):
            score -= 4 if market_overlap else 10
        else:
            score -= 4
    if archetype_relation == "direct":
        score += 4
    elif archetype_relation == "adjacent":
        score += 1
    elif archetype_relation == "incompatible":
        score -= 18 if candidate_archetypes and theme_archetypes else 8
    if candidate_economic_roles and theme_economic_roles and not economic_role_overlap:
        score -= 10 if dominant_economic_role else 6
    if not role_overlap and market_overlap:
        score -= 4
    if bool(prepared_theme.get("_looks_generic_theme")) and not role_overlap:
        score -= 4
    if generic_business_model_only and not specific_overlap and not economic_role_overlap:
        score -= 6
    if descriptor_led_generic_umbrella_overlap:
        score -= 18
    industrial_manufacturing_drift_overlap = (
        "industrial_additive_manufacturing" in descriptor_families
        and _theme_looks_industrial_manufacturing_drift(prepared_theme)
        and not role_overlap
        and not specific_overlap
    )
    if industrial_manufacturing_drift_overlap:
        score -= 18
    autonomous_component_drift_overlap = (
        "autonomous_systems" in descriptor_families
        and bool({"component", "module"} & value_chain_layers)
        and _theme_looks_autonomous_component_drift(prepared_theme)
        and not role_overlap
        and not specific_overlap
        and not economic_role_overlap
    )
    if autonomous_component_drift_overlap:
        score -= 18
    autonomous_product_family_overlap = (
        "autonomous_systems" in descriptor_families
        and bool({"component", "module"} & value_chain_layers)
        and _theme_looks_autonomous_product_family_bucket(prepared_theme)
        and not role_overlap
        and not economic_role_overlap
    )
    if autonomous_product_family_overlap:
        score -= 12
    digital_asset_infrastructure_drift_overlap = (
        "digital_asset_infrastructure" in descriptor_families
        and _theme_looks_digital_asset_infrastructure_drift(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(specific_overlap) <= {"payments"}
        and set(economic_role_overlap) <= {"financial_platform", "software_service_provider", "end_platform_operator"}
    )
    if digital_asset_infrastructure_drift_overlap:
        score -= 18
    upstream_oil_gas_drift_overlap = (
        "upstream_oil_gas" in descriptor_families
        and _theme_looks_upstream_oil_gas_drift(prepared_theme)
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= {"software_service_provider", "end_platform_operator", "infrastructure_operator"}
    )
    if upstream_oil_gas_drift_overlap:
        score -= 18
    upstream_oil_gas_geography_overlap = (
        "upstream_oil_gas" in descriptor_families
        and _theme_looks_upstream_oil_gas_geography_bucket(prepared_theme)
        and not token_overlap
        and not specific_overlap
    )
    if upstream_oil_gas_geography_overlap:
        score -= 14
    connected_operations_iot_direct_support = (
        "connected_operations_iot" in descriptor_families
        and _theme_supports_connected_operations_iot(prepared_theme)
    )
    if connected_operations_iot_direct_support:
        score += 12
    connected_operations_iot_drift_overlap = (
        "connected_operations_iot" in descriptor_families
        and (
            _theme_looks_connected_operations_iot_drift(prepared_theme)
            or (
                _theme_looks_generic_umbrella(prepared_theme)
                and not _theme_supports_connected_operations_iot(prepared_theme)
            )
        )
        and set(role_overlap) <= WEAK_ROLE_SIGNALS
        and set(economic_role_overlap) <= {"software_service_provider", "end_platform_operator", "infrastructure_operator"}
        and not specific_overlap
    )
    if connected_operations_iot_drift_overlap:
        score -= 18
    if bool(prepared_theme.get("_is_generic_factor_theme")):
        if strong_role_evidence:
            score -= 16
        elif not role_overlap and not archetype_overlap:
            score -= 8
    if (
        "networking_interconnect" in candidate_archetypes
        and candidate_archetypes - {"networking_interconnect"}
        and not archetype_overlap
        and archetype_relation != "adjacent"
    ):
        score -= 6
    if not role_overlap and not specific_overlap and generic_overlap and len(token_overlap) <= 1:
        score -= 3
    if not role_overlap and not specific_overlap and not market_overlap:
        score = min(score, 1)
    if archetype_relation == "incompatible" and not role_overlap and not specific_overlap:
        score = min(score, 0)
    if bool(prepared_theme.get("_is_generic_factor_theme")) and strong_role_evidence and (role_overlap or archetype_overlap or specific_overlap):
        score = min(score, 6)
    if (
        strong_role_evidence
        and dominant_economic_role in {"component_supplier", "materials_supplier", "defense_systems_manufacturer"}
        and "end_platform_operator" in theme_economic_roles
        and not economic_role_overlap
    ):
        score -= 10
    if (
        strong_role_evidence
        and dominant_economic_role in {"financial_platform", "identity_verification_platform"}
        and {"component_supplier", "materials_supplier"} & theme_economic_roles
        and not economic_role_overlap
    ):
        score -= 10
    if not merchant_input_evidence and _theme_looks_merchant_input(prepared_theme):
        score -= 14
        if not role_overlap and not specific_overlap and not economic_role_overlap:
            score = min(score, 4)
    if clear_business_descriptor and _theme_looks_generic_umbrella(prepared_theme) and not role_overlap and not specific_overlap and not economic_role_overlap:
        score -= 12
    if descriptor_led_generic_umbrella_overlap:
        score = min(score, 8 if market_overlap else 6)
    if industrial_manufacturing_drift_overlap:
        score = min(score, 4)
    if autonomous_component_drift_overlap:
        score = min(score, 4)
    if autonomous_product_family_overlap:
        score = min(score, 9 if specific_overlap else 7)
    if digital_asset_infrastructure_drift_overlap:
        score = min(score, 5)
    if upstream_oil_gas_drift_overlap:
        score = min(score, 5)
    if upstream_oil_gas_geography_overlap:
        score = min(score, 4)
    if connected_operations_iot_drift_overlap:
        score = min(score, 5)
    if clear_business_descriptor and "end_market_only" not in value_chain_layers and market_overlap and not role_overlap and not specific_overlap and not economic_role_overlap:
        score -= 6

    if score <= 0:
        why = ""
    elif industrial_manufacturing_drift_overlap:
        why = "Geography/luxury drift suppressed because the description points to an industrial manufacturing systems business."
    elif autonomous_component_drift_overlap:
        why = "Broad AI/equipment adjacency suppressed because the description points to autonomous-system components rather than a broad platform or equipment bucket."
    elif autonomous_product_family_overlap:
        why = "Broader autonomous product-family themes were de-prioritized because the description points more specifically to components and modules."
    elif digital_asset_infrastructure_drift_overlap:
        why = "Generic payments, banking, or software adjacency was suppressed because the description points to digital-asset infrastructure and settlement rails."
    elif upstream_oil_gas_drift_overlap:
        why = "LNG, energy transition, or adjacent energy drift was suppressed because the description points to upstream oil and gas exploration and production."
    elif upstream_oil_gas_geography_overlap:
        why = "Unsupported regional energy bucket was suppressed because the description does not explicitly anchor the company to that geography."
    elif connected_operations_iot_drift_overlap:
        why = "Generic cloud/device noise and unrelated geography/apparel drift were suppressed because the description points to a connected-operations software platform tied to physical assets."
    elif not merchant_input_evidence and _theme_looks_merchant_input(prepared_theme):
        why = "Merchant-input theme suppressed because the description lacks explicit input/material evidence."
    elif descriptor_led_generic_umbrella_overlap or (
        clear_business_descriptor and _theme_looks_generic_umbrella(prepared_theme) and not role_overlap and not specific_overlap and not economic_role_overlap
    ):
        why = "Generic umbrella overlap only; the description points to a more specific business/product layer."
    elif role_overlap and not weak_generic_business_model_overlap:
        why = "Direct business-role fit on " + _format_signal_names(set(role_overlap), ROLE_DISPLAY_NAMES)
    elif economic_role_overlap and not weak_generic_business_model_overlap:
        why = "Compatible economic-role fit on " + _format_signal_names(set(economic_role_overlap), ECONOMIC_ROLE_DISPLAY_NAMES)
    elif weak_generic_business_model_overlap:
        why = "Generic software/business-model overlap only; treat as tentative."
    elif archetype_overlap:
        why = "Compatible business archetype fit on " + _format_signal_names(set(archetype_overlap), ARCHETYPE_DISPLAY_NAMES)
    elif specific_overlap:
        why = "Conceptual fit on " + ", ".join(specific_overlap[:2])
    elif market_overlap:
        why = "Indirect end-market adjacency through " + _format_signal_names(set(market_overlap), END_MARKET_DISPLAY_NAMES)
    elif generic_overlap:
        why = "Broad fit through " + ", ".join(generic_overlap[:2])
    else:
        why = "Weak text-only fit; treat as tentative."

    return {
        "score": score,
        "why": why,
        "role_overlap": role_overlap,
        "market_overlap": market_overlap,
        "specific_overlap": specific_overlap,
        "generic_overlap": generic_overlap,
        "direct_role_fit": direct_role_fit,
        "indirect_only_fit": indirect_only_fit,
        "archetype_overlap": archetype_overlap,
        "archetype_relation": archetype_relation,
        "candidate_archetypes": sorted(candidate_archetypes),
        "theme_archetypes": sorted(theme_archetypes),
        "economic_role_overlap": economic_role_overlap,
        "candidate_economic_roles": sorted(candidate_economic_roles),
        "theme_economic_roles": sorted(theme_economic_roles),
        "dominant_economic_role": dominant_economic_role,
        "generic_factor_theme": bool(prepared_theme.get("_is_generic_factor_theme")),
        "generic_business_model_only": generic_business_model_only,
        "descriptor_led_generic_umbrella_overlap": descriptor_led_generic_umbrella_overlap,
        "strong_role_evidence": strong_role_evidence,
        "connected_operations_iot_direct_support": connected_operations_iot_direct_support,
        "digital_asset_infrastructure_drift_overlap": digital_asset_infrastructure_drift_overlap,
        "upstream_oil_gas_drift_overlap": upstream_oil_gas_drift_overlap,
        "upstream_oil_gas_geography_overlap": upstream_oil_gas_geography_overlap,
        "connected_operations_iot_drift_overlap": connected_operations_iot_drift_overlap,
    }


def _concept_strength(concepts: set[str]) -> str:
    specific = [concept for concept in concepts if concept not in GENERIC_CONCEPTS]
    if specific:
        return specific[0]
    return next(iter(concepts), "")


def theme_catalog_context(conn, representative_limit: int = 5) -> list[dict[str, object]]:
    from .scanner_research_profiles import theme_catalog_context as load_theme_catalog_context

    return load_theme_catalog_context(conn, representative_limit=representative_limit)


def _theme_preprocess_cache_key(theme_entry: dict[str, object]) -> tuple[object, ...]:
    from .scanner_research_analysis import theme_preprocess_cache_key

    return theme_preprocess_cache_key(theme_entry)


def _build_preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    from .scanner_research_analysis import build_preprocessed_theme_entry

    return build_preprocessed_theme_entry(theme_entry)


def _preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    from .scanner_research_analysis import preprocessed_theme_entry

    return preprocessed_theme_entry(theme_entry)


def _preprocessed_catalog(catalog: list[dict[str, object]]) -> list[dict[str, object]]:
    from .scanner_research_analysis import preprocessed_catalog

    return preprocessed_catalog(catalog)


def _concise_theme_context(theme_entry: dict[str, object], representative_limit: int = 3) -> dict[str, object]:
    representative_tickers = [str(value).strip().upper() for value in list(theme_entry.get("representative_tickers") or []) if str(value).strip()][:representative_limit]
    description = _normalize_text(theme_entry.get("theme_description"))
    if description:
        description = description[:180]
    elif representative_tickers:
        description = f"Representative tickers: {', '.join(representative_tickers)}"
    return {
        "theme_id": int(theme_entry["theme_id"]),
        "theme_name": str(theme_entry["theme_name"]),
        "category": str(theme_entry.get("category") or "Uncategorized"),
        "theme_description": description,
        "representative_tickers": representative_tickers,
    }


def _load_company_profile(ticker: str) -> dict[str, object]:
    from .scanner_research_profiles import load_company_profile

    return load_company_profile(ticker)


def _profile_has_research_value(profile: dict[str, object] | None) -> bool:
    from .scanner_research_profiles import profile_has_research_value

    return profile_has_research_value(profile)


def _load_company_profile_with_cache(ticker: str) -> dict[str, object]:
    from .scanner_research_profiles import load_company_profile_with_cache

    return load_company_profile_with_cache(ticker)


def _candidate_context(conn, ticker: str) -> dict[str, object]:
    from .scanner_research_profiles import candidate_context

    return candidate_context(conn, ticker)


def _description_analysis_cache_key(profile: dict[str, object], candidate: dict[str, object], extra_parts: tuple[object, ...]) -> tuple[object, ...]:
    from .scanner_research_analysis import description_analysis_cache_key

    return description_analysis_cache_key(profile, candidate, extra_parts)


def _build_candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    from .scanner_research_analysis import build_candidate_analysis

    return build_candidate_analysis(profile, candidate, *extra_parts)


def _candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    from .scanner_research_analysis import candidate_analysis

    return candidate_analysis(profile, candidate, *extra_parts)


def _theme_fit_score(theme_entry: dict[str, object], profile: dict[str, object], candidate: dict[str, object]) -> tuple[int, str]:
    from .scanner_research_heuristics import theme_fit_score

    return theme_fit_score(theme_entry, profile, candidate)


def _candidate_new_theme_label(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str | None:
    roles = _candidate_roles(profile, candidate, *extra_parts)
    markets = _candidate_end_markets(profile, candidate, *extra_parts)
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    economic_role = _dominant_economic_role(profile, candidate, *extra_parts)
    description = " ".join(
        [
            _normalize_text(profile.get("company_name")).lower(),
            _normalize_text(profile.get("description")).lower(),
            _normalize_text(profile.get("sic_description")).lower(),
            _normalize_text(candidate.get("recommendation_reason")).lower(),
            *[_normalize_text(part).lower() for part in extra_parts],
        ]
    )
    native_descriptors = _description_native_business_descriptors(description)
    merchant_input_evidence = _merchant_input_evidence(description)
    if native_descriptors:
        descriptor_families = _descriptor_families(native_descriptors)
        if "optical_networking" in descriptor_families:
            if "ai" in markets and "fiber" in description:
                return "AI Fiber Optics"
            if "data_center" in markets or "data center" in description or "data-center" in description:
                return "Data Center Optics"
        return native_descriptors[0]
    if economic_role == "financial_platform" and "fintech_payments_lending" in archetypes:
        return "Digital Payments"
    if economic_role == "identity_verification_platform" and "digital_identity_security" in archetypes:
        return "Identity Verification"
    if "fintech_payments_lending" in archetypes:
        return "Digital Payments"
    if "digital_identity_security" in archetypes:
        return "Identity Verification"
    if "semiconductor_materials_electronics_materials" in archetypes and merchant_input_evidence:
        if "compound semiconductor" in description or "gallium arsenide" in description or "indium phosphide" in description:
            return "Compound Semiconductor Materials"
        if "substrate" in description or "substrates" in description:
            return "Semiconductor Substrates"
        if "electronics materials" in description or "packaging materials" in description:
            return "Electronics Materials"
        if "specialty" in description:
            return "Specialty Semiconductor Materials"
        return "Semiconductor Materials"
    if "aerospace_defense_space_systems" in archetypes and economic_role == "defense_systems_manufacturer":
        return "Defense Systems"
    if "optical_networking" in roles:
        if "ai" in markets and "fiber" in description:
            return "AI Fiber Optics"
        if "data_center" in markets or "data center" in description or "data-center" in description:
            return "Data Center Optics"
        if "interconnect" in description:
            return "Optical Interconnects"
        return "Optical Networking"
    if "networking_interconnect" in archetypes:
        if economic_role == "materials_supplier":
            return None
        if "ai" in markets and "fiber" in description:
            return "AI Fiber Optics"
        if "data_center" in markets or "data center" in description or "data-center" in description:
            return "Data Center Optics"
        if "interconnect" in description:
            return "Optical Interconnects"
        return "Optical Networking"
    if "ai_infrastructure_data_centers" in archetypes:
        return "AI Data Centers"
    if "semiconductor_materials" in roles and merchant_input_evidence:
        if "compound semiconductor" in description or "gallium arsenide" in description or "indium phosphide" in description:
            return "Compound Semiconductor Materials"
        if "substrate" in description or "substrates" in description:
            return "Semiconductor Substrates"
        if "specialty" in description:
            return "Specialty Semiconductor Materials"
        return "Semiconductor Materials"
    direct_phrase_ideas = _direct_phrase_theme_ideas(description)
    if direct_phrase_ideas:
        return direct_phrase_ideas[0]
    dominant_role = _dominant_role(profile, candidate, *extra_parts)
    if dominant_role:
        if dominant_role in {"software_tooling", "power_generation"}:
            return None
        return _normalize_optional_theme_label(ROLE_NEW_LABELS.get(dominant_role))
    concept = _concept_strength(_candidate_concepts(profile, candidate))
    if concept:
        return _normalize_optional_theme_label(THEME_NEW_LABELS.get(concept) or _normalize_text(profile.get("sic_description")).title())
    return None


def _proposed_new_theme_category(
    profile: dict[str, object],
    candidate: dict[str, object],
    possible_new_theme: object,
    *,
    business_descriptors: list[str] | None = None,
    value_chain_layers: list[str] | set[str] | None = None,
) -> str | None:
    theme_label = _normalize_optional_theme_label(possible_new_theme)
    if not theme_label:
        return None
    normalized_theme_label = theme_label.lower()
    descriptor_list = list(business_descriptors or _description_native_business_descriptors(
        " ".join(
            [
                _normalize_text(profile.get("company_name")),
                _normalize_text(profile.get("description")),
                _normalize_text(profile.get("sic_description")),
                _normalize_text(candidate.get("recommendation_reason")),
            ]
        )
    ))
    descriptor_families = _descriptor_families(descriptor_list + [theme_label])
    normalized_layers = set(value_chain_layers or _descriptor_value_chain_layers(descriptor_list + [theme_label]))
    if "extractive_resources" in descriptor_families:
        if any(token in normalized_theme_label for token in {"tungsten", "lithium", "nickel", "uranium", "graphite", "cobalt", "rare earth"}):
            return "Metals & Mining / Critical Minerals"
        return "Metals & Mining"
    for family, category in MISSING_THEME_CATEGORY_BY_FAMILY.items():
        if family in descriptor_families:
            return category
    if "platform" in normalized_layers or "software_application" in normalized_layers:
        return "Software & Platforms"
    if "component" in normalized_layers or "module" in normalized_layers or "device" in normalized_layers:
        return "Components & Devices"
    if "system" in normalized_layers or "network_service" in normalized_layers:
        return "Systems & Infrastructure"
    return None


def _supports_distinct_new_theme_label(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> bool:
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    if archetypes & {
        "fintech_payments_lending",
        "digital_identity_security",
        "semiconductor_materials_electronics_materials",
        "ai_infrastructure_data_centers",
        "networking_interconnect",
    }:
        return True
    role = _dominant_role(profile, candidate, *extra_parts)
    if role in {"", "software_tooling", "power_generation"}:
        return False
    return True


def _should_prioritize_new_theme(
    candidate_roles: set[str],
    suggested_existing: list[dict[str, object]],
    strongest_score: int,
    strongest_direct_role_fit: bool,
) -> bool:
    if not candidate_roles:
        return False
    if not suggested_existing:
        return True
    if not strongest_direct_role_fit:
        return True
    return strongest_score < 12


def _value_chain_summary(profile: dict[str, object], candidate: dict[str, object]) -> str:
    roles = _candidate_roles(profile, candidate)
    markets = _candidate_end_markets(profile, candidate)
    economic_role = _dominant_economic_role(profile, candidate)
    if roles:
        if "server_systems" in roles and ({"ai", "data_center"} & markets):
            return "appears to serve AI compute infrastructure and data-center end markets through server systems"
        role_text = _format_signal_names(roles, ROLE_DISPLAY_NAMES)
        economic_role_text = ECONOMIC_ROLE_DISPLAY_NAMES.get(economic_role, "")
        market_text = _format_signal_names(markets, END_MARKET_DISPLAY_NAMES) if markets else ""
        if market_text and economic_role_text:
            return f"appears to serve {market_text} end markets as a {economic_role_text} focused on {role_text}"
        if market_text:
            return f"appears to serve {market_text} end markets through {role_text}"
        return f"appears to operate primarily in {role_text}"
    concepts = _candidate_concepts(profile, candidate)
    if "cybersecurity" in concepts:
        return "appears to operate in cybersecurity software and security operations tooling"
    if "ai_compute" in concepts and "semiconductor" in concepts:
        return "appears to sit in AI compute infrastructure through chips, servers, or accelerated data-center hardware"
    if "ai_compute" in concepts:
        return "appears to sit in AI compute infrastructure and data-center enablement"
    if "data_analytics" in concepts and "defense" in concepts:
        return "appears to provide data/decision platforms with government or defense adjacency"
    if "data_analytics" in concepts:
        return "appears to provide data integration, analytics, or decision software"
    if "space" in concepts:
        return "appears to operate in the space/satellite infrastructure value chain"
    if "defense" in concepts:
        return "appears to have defense or mission-oriented technology exposure"
    if "payments" in concepts:
        return "appears to operate in digital payments or transaction infrastructure"
    if "biotech" in concepts:
        return "appears to operate in biotech or therapeutics"
    if "energy" in concepts:
        return "appears to operate in energy generation, storage, or grid infrastructure"
    description = _normalize_text(profile.get("description")) or _normalize_text(profile.get("sic_description"))
    return description[:140] if description else "has limited profile context available"


def _prefilter_ai_theme_catalog(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    max_themes: int = 12,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    from .scanner_research_analysis import prefilter_ai_theme_catalog

    return prefilter_ai_theme_catalog(candidate, catalog, profile, max_themes=max_themes)


def _heuristic_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    adjacent_scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    broad_alternatives: list[str] = []
    candidate_analysis = _candidate_analysis(profile, candidate)
    candidate_concepts = set(candidate_analysis.get("candidate_concepts") or set())
    candidate_roles = set(candidate_analysis.get("candidate_roles") or set())
    candidate_markets = set(candidate_analysis.get("candidate_markets") or set())
    strong_role_evidence = bool(candidate_analysis.get("strong_role_evidence"))
    for entry in _preprocessed_catalog(catalog):
        theme_concepts = set(entry.get("_theme_concepts") or set())
        theme_roles = set(entry.get("_theme_roles") or set())
        theme_markets = set(entry.get("_theme_markets") or set())
        if (
            not (candidate_roles & theme_roles)
            and not ((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
            and ((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
            and str(entry.get("theme_name") or "") not in broad_alternatives
        ):
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        elif (_looks_generic_theme(entry) or _is_generic_factor_theme(entry)) and str(entry.get("theme_name") or "") not in broad_alternatives:
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        score = int(fit_details["score"])
        why = str(fit_details["why"])
        if not why and not fit_details.get("direct_role_fit"):
            if fit_details.get("market_overlap"):
                why = "Indirect end-market adjacency through " + _format_signal_names(set(fit_details["market_overlap"]), END_MARKET_DISPLAY_NAMES)
            elif fit_details.get("specific_overlap"):
                why = "Partial conceptual overlap on " + ", ".join(list(fit_details["specific_overlap"])[:2])
        suggestion_payload = {
            "theme_id": int(entry["theme_id"]),
            "theme_name": str(entry["theme_name"]),
            "category": str(entry["category"]),
            "why_it_might_fit": why,
            "representative_tickers": list(entry.get("representative_tickers") or []),
        }
        suggestion_payload = _annotate_suggestion_fit(suggestion_payload, fit_details)
        weak_economic_only_adjacent = bool(
            strong_role_evidence
            and not fit_details.get("direct_role_fit")
            and fit_details.get("indirect_only_fit")
            and not fit_details.get("market_overlap")
            and not fit_details.get("role_overlap")
            and not fit_details.get("archetype_overlap")
            and fit_details.get("economic_role_overlap")
        )
        if (
            not weak_economic_only_adjacent
            and (
            not fit_details.get("direct_role_fit")
            and (
                fit_details.get("market_overlap")
                or ((candidate_markets & theme_markets) and not (candidate_roles & theme_roles))
                or (((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS) and not (candidate_roles & theme_roles))
            )
            )
        ):
            adjacent_scored.append((score, suggestion_payload, fit_details))
        if weak_economic_only_adjacent:
            continue
        if score < 3:
            continue
        scored.append(
            (
                score,
                suggestion_payload,
                fit_details,
            )
        )
    scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    adjacent_scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    strongest_score = scored[0][0] if scored else 0
    score_floor = max(8, strongest_score - 2) if strongest_score else 999
    suggested_existing = [item[1] for item in scored if item[0] >= score_floor][:3]
    if strong_role_evidence:
        suggested_existing = [item for item in suggested_existing if item.get("fit_label") != "broad_fit" or not _is_generic_factor_theme({
            "theme_name": item.get("theme_name"),
            "category": item.get("category"),
            "theme_description": item.get("why_it_might_fit"),
        })][:3]
    strongest_details = scored[0][2] if scored else {}
    strongest_direct_role_fit = bool(strongest_details.get("direct_role_fit"))
    strongest_indirect_only_fit = bool(strongest_details.get("indirect_only_fit"))
    secondary_existing = [item[1] for item in scored if item[0] >= max(5, strongest_score - 5)][:2]
    if strongest_indirect_only_fit and secondary_existing:
        suggested_existing = secondary_existing

    possible_similar: list[str] = []
    similar_seed = [item for item in scored if item[2].get("direct_role_fit")] or scored
    if strongest_score >= 8:
        for _, suggestion, fit_details in similar_seed[:3]:
            if not fit_details.get("direct_role_fit") and candidate_roles:
                continue
            for ticker in suggestion.get("representative_tickers") or []:
                symbol = str(ticker).strip().upper()
                if symbol and symbol != candidate["ticker"] and symbol not in possible_similar:
                    possible_similar.append(symbol)
    possible_similar = possible_similar[:3 if strongest_score >= 12 else 2]

    confidence = "low"
    recommended_action = "watch_only"
    possible_new_theme = None
    possible_new_theme_category = None
    caveats: list[str] = []
    new_theme_label = _candidate_new_theme_label(profile, candidate)
    should_prioritize_new_theme = _should_prioritize_new_theme(
        candidate_roles,
        suggested_existing,
        strongest_score,
        strongest_direct_role_fit,
    )
    if should_prioritize_new_theme and new_theme_label:
        if not suggested_existing and adjacent_scored:
            suggested_existing = [item[1] for item in adjacent_scored[:2]]
        possible_new_theme = new_theme_label
        possible_new_theme_category = _proposed_new_theme_category(profile, candidate, possible_new_theme)
        recommended_action = "consider_new_theme"
        confidence = "medium" if candidate_roles else "low"
        if suggested_existing:
            caveats.append("Existing governed themes look adjacent rather than direct fits for the company's narrow business role.")
        else:
            caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif suggested_existing and strongest_score >= 10:
        confidence = "high" if strongest_score >= 12 and strongest_direct_role_fit else "medium"
        recommended_action = "add_to_existing_theme_review"
        if strong_role_evidence and new_theme_label and suggested_existing:
            top_existing_name = str(suggested_existing[0].get("theme_name") or "")
            if _is_generic_factor_theme(
                {
                    "theme_name": top_existing_name,
                    "category": suggested_existing[0].get("category"),
                    "theme_description": suggested_existing[0].get("why_it_might_fit"),
                }
            ):
                possible_new_theme = new_theme_label
                possible_new_theme_category = _proposed_new_theme_category(profile, candidate, possible_new_theme)
                recommended_action = "consider_new_theme"
                confidence = "medium"
                caveats.append("Generic factor/style themes are less useful than the company's operating-role framing for thematic review.")
    elif new_theme_label:
        confidence = "low"
        possible_new_theme = new_theme_label
        possible_new_theme_category = _proposed_new_theme_category(profile, candidate, possible_new_theme)
        recommended_action = "consider_new_theme"
        caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif candidate["recommendation"] in {"high-persistence uncovered", "review for addition"}:
        caveats.append("No external company profile was available, so the draft is based on internal scanner evidence only.")
    else:
        recommended_action = "reject_for_now"
        caveats.append("Internal evidence is weak and no grounded theme fit was found.")

    if not _normalize_text(profile.get("description")):
        caveats.append("Company description is unavailable or unverified in the current environment.")

    rationale_parts = [
        f"The company { _value_chain_summary(profile, candidate) }.",
        f"Scanner Audit shows {candidate['recommendation']} with persistence_score={candidate['persistence_score']}, observed_days={candidate['observed_days']}, last_10={candidate['observations_last_10d']}, streak={candidate['current_streak']}.",
        "Theme ranking prioritizes the company's actual role in the stack over broad end-market adjacency.",
    ]
    if possible_new_theme:
        role_text = _format_signal_names(candidate_roles, ROLE_DISPLAY_NAMES) if candidate_roles else "its apparent role"
        if suggested_existing:
            rationale_parts.append(
                f"The narrow business-role framing points more precisely to {possible_new_theme} than the best governed-theme matches, which remain useful but adjacent."
            )
            rationale_parts.append(
                "Best governed-theme fit: "
                + "; ".join(f"{item['theme_name']} ({item['why_it_might_fit']})" for item in suggested_existing)
            )
        else:
            rationale_parts.append(f"No strong governed-theme match stood out, so a tentative new-theme direction is {possible_new_theme}, which better reflects {role_text}.")
        weaker_alternatives = [name for name in broad_alternatives if name not in {item["theme_name"] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker because they map more to end-market adjacency than to the company's actual role in the stack.")
        rationale_parts.append(f"A tentative new-theme label is being surfaced because {possible_new_theme} is a more precise description of the company's direct role than the current governed taxonomy.")
    elif suggested_existing and strongest_score >= 10:
        rationale_parts.append(
            "Best governed-theme fit: "
            + "; ".join(f"{item['theme_name']} ({item['why_it_might_fit']})" for item in suggested_existing)
        )
        weaker_alternatives = [item[1]["theme_name"] for item in scored[1:3] if item[0] < strongest_score and item[0] < 10]
        if not weaker_alternatives:
            weaker_alternatives = [name for name in broad_alternatives if name not in {item["theme_name"] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker because they map more to end-market adjacency than to the company's actual role in the stack.")
    else:
        rationale_parts.append("No strong governed-theme fit was identified from the available profile and taxonomy context.")

    return {
        "ticker": candidate["ticker"],
        "company_name": _normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": _normalize_text(profile.get("description")) or _normalize_text(profile.get("sic_description")) or "No verified company description available.",
        "possible_similar_tickers": possible_similar,
        "suggested_existing_themes": _prioritize_operating_role_suggestions(
            _truncate_existing_theme_suggestions(suggested_existing),
            strong_role_evidence=strong_role_evidence,
        ),
        "possible_new_theme": possible_new_theme,
        "possible_new_theme_category": possible_new_theme_category,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
    }


def _description_theme_generation_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    from .scanner_research_analysis import description_theme_generation_draft

    return description_theme_generation_draft(candidate, catalog, profile)


def _call_openai_research(api_key: str, context: dict[str, object], *, max_output_tokens: int = 550) -> dict[str, object]:
    from .scanner_research_merge import call_openai_research

    return call_openai_research(api_key, context, max_output_tokens=max_output_tokens)


def _estimate_context_size_chars(context: dict[str, object]) -> int:
    from .scanner_research_merge import estimate_context_size_chars

    return estimate_context_size_chars(context)


def _normalize_action(value: object, fallback: str = "watch_only") -> str:
    from .scanner_research_merge import normalize_action

    return normalize_action(value, fallback)


def _best_suggested_theme_fit_details(
    suggested_existing: list[dict[str, object]],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    from .scanner_research_merge import best_suggested_theme_fit_details

    return best_suggested_theme_fit_details(suggested_existing, catalog, profile, candidate)


def _annotate_existing_theme_suggestions(
    suggestions: list[dict[str, object]],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> list[dict[str, object]]:
    candidate_analysis = _candidate_analysis(profile, candidate)
    by_id = {int(item["theme_id"]): item for item in _preprocessed_catalog(catalog)}
    annotated: list[dict[str, object]] = []
    for suggestion in list(suggestions or []):
        try:
            theme_id = int(suggestion.get("theme_id"))
        except Exception:
            continue
        entry = by_id.get(theme_id)
        if entry is None:
            continue
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        annotated.append(_annotate_suggestion_fit(suggestion, fit_details))
    return _truncate_existing_theme_suggestions(annotated)


def _prioritize_operating_role_suggestions(
    suggestions: list[dict[str, object]],
    *,
    strong_role_evidence: bool,
) -> list[dict[str, object]]:
    if not strong_role_evidence:
        return _truncate_existing_theme_suggestions(suggestions)
    ranked = sorted(
        list(suggestions or []),
        key=lambda item: (
            _is_generic_factor_theme(
                {
                    "theme_name": item.get("theme_name"),
                    "category": item.get("category"),
                    "theme_description": item.get("why_it_might_fit"),
                }
            ),
            item.get("fit_label") == "broad_fit",
            -int(item.get("_match_score") or 0),
            str(item.get("theme_name") or ""),
        ),
    )
    return _truncate_existing_theme_suggestions(ranked)


def _precision_override_reason(
    possible_new_theme: str,
    suggested_existing: list[dict[str, object]],
) -> str:
    from .scanner_research_merge import precision_override_reason

    return precision_override_reason(possible_new_theme, suggested_existing)


def _rationale_signals_precision_gap(rationale: str) -> bool:
    from .scanner_research_merge import rationale_signals_precision_gap

    return rationale_signals_precision_gap(rationale)


def _existing_theme_fit_is_adjacent_only(best_fit: dict[str, object]) -> bool:
    if not isinstance(best_fit, dict):
        return False
    if bool(best_fit.get("direct_role_fit")):
        return False
    if bool(best_fit.get("indirect_only_fit")):
        return True
    return int(best_fit.get("score") or 0) < 12


def _merge_ai_with_heuristic_draft(
    ai_draft: dict[str, object],
    heuristic_draft: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    from .scanner_research_merge import merge_ai_with_heuristic_draft

    return merge_ai_with_heuristic_draft(ai_draft, heuristic_draft, catalog, profile, candidate)


def _normalize_ai_theme_suggestions(raw_items: object, catalog: list[dict[str, object]]) -> list[dict[str, object]]:
    from .scanner_research_merge import normalize_ai_theme_suggestions

    return normalize_ai_theme_suggestions(raw_items, catalog)


def _ensure_scanner_research_review_table(conn) -> None:
    from .scanner_research_persistence import ensure_scanner_research_review_table

    ensure_scanner_research_review_table(conn)


def _scanner_research_review_context(draft: dict[str, object]) -> dict[str, object]:
    from .scanner_research_persistence import scanner_research_review_context

    return scanner_research_review_context(draft)


def get_scanner_research_review(conn, ticker: str, draft: dict[str, object] | None) -> dict[str, object] | None:
    from .scanner_research_persistence import get_scanner_research_review as load_research_review

    return load_research_review(conn, ticker, draft)


def save_scanner_research_review(
    conn,
    ticker: str,
    draft: dict[str, object] | None,
    *,
    outcome_class: object,
    reviewer_notes: object = "",
) -> dict[str, object]:
    from .scanner_research_persistence import save_scanner_research_review as persist_research_review

    return persist_research_review(
        conn,
        ticker,
        draft,
        outcome_class=outcome_class,
        reviewer_notes=reviewer_notes,
    )


def scanner_research_review_summary(conn, *, limit: int = 8) -> dict[str, object]:
    from .scanner_research_persistence import scanner_research_review_summary as load_review_summary

    return load_review_summary(conn, limit=limit)


def _baseline_research_draft(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
) -> dict[str, object]:
    from .scanner_research_merge import baseline_research_draft

    return baseline_research_draft(candidate, catalog, profile)


def _ai_research_draft_for_strategy(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    strategy: str,
) -> dict[str, object]:
    from .scanner_research_merge import ai_research_draft_for_strategy

    return ai_research_draft_for_strategy(candidate, catalog, profile, strategy=strategy)


def generate_scanner_research_draft(conn, ticker: str, *, strategy: str = "description_theme_generation") -> dict[str, object]:
    from .scanner_research_merge import RecoverableResearchGenerationError

    total_start = _now_perf()
    candidate_start = _now_perf()
    candidate = _candidate_context(conn, ticker)
    candidate_ms = _elapsed_ms(candidate_start)
    catalog_start = _now_perf()
    catalog = theme_catalog_context(conn)
    catalog_ms = _elapsed_ms(catalog_start)
    preprocess_start = _now_perf()
    preprocessed_catalog = _preprocessed_catalog(catalog)
    catalog_preprocess_ms = _elapsed_ms(preprocess_start)
    profile_start = _now_perf()
    profile = _load_company_profile_with_cache(candidate["ticker"])
    profile_ms = _elapsed_ms(profile_start)
    generated_at = datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")
    normalized_strategy = _normalize_research_strategy(strategy)

    research_mode = "heuristic_fallback"
    fallback_reason = None
    research_error = None
    try:
        draft = _ai_research_draft_for_strategy(candidate, preprocessed_catalog, profile, strategy=normalized_strategy)
        research_mode = "openai"
    except RecoverableResearchGenerationError as exc:
        draft = _baseline_research_draft(candidate, preprocessed_catalog, profile)
        research_error = dict(getattr(exc, "details", {}) or {})
        if not research_error:
            research_error = _extract_openai_error_details(exc)
        fallback_reason = _format_openai_error_summary(research_error) if research_error else str(exc)

    draft["ticker"] = candidate["ticker"]
    draft["generated_at"] = generated_at
    draft["source"] = "scanner_audit"
    draft["research_mode"] = research_mode
    draft["theme_generation_strategy"] = normalized_strategy
    timing = dict(draft.get("research_timing_summary") or {})
    timing.update(
        {
            "candidate_context_ms": candidate_ms,
            "catalog_query_ms": catalog_ms,
            "catalog_preprocess_ms": catalog_preprocess_ms,
            "profile_lookup_ms": profile_ms,
            "total_ms": _elapsed_ms(total_start),
        }
    )
    draft["research_timing_summary"] = timing
    if fallback_reason:
        draft["fallback_reason"] = fallback_reason
    if research_error:
        draft["research_error"] = research_error
    return draft


def get_or_create_scanner_research_draft(
    conn,
    ticker: str,
    existing_draft: dict[str, object] | None = None,
    *,
    force_refresh: bool = False,
    strategy: str = "description_theme_generation",
) -> tuple[dict[str, object], bool]:
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_strategy = _normalize_research_strategy(strategy)
    if (
        not force_refresh
        and isinstance(existing_draft, dict)
        and str(existing_draft.get("ticker") or "").strip().upper() == normalized_ticker
        and _normalize_research_strategy(existing_draft.get("theme_generation_strategy")) == normalized_strategy
    ):
        return existing_draft, True
    return generate_scanner_research_draft(conn, normalized_ticker, strategy=normalized_strategy), False
