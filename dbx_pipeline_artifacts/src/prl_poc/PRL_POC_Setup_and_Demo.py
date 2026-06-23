# Databricks notebook source
# MAGIC %md
# MAGIC # PRL Engine — Proof of Concept
# MAGIC **Entity:** Payer Master (data from lakefusion_prod_1)
# MAGIC
# MAGIC This notebook demonstrates the Probabilistic Record Linkage (PRL) engine as a replacement
# MAGIC for deterministic match rules. It follows the real pipeline flow:
# MAGIC
# MAGIC 1. **Setup** — Load prod payer data, reset unified records to ACTIVE
# MAGIC 2. **Vector Search** — Create index on master, run search to populate search_results
# MAGIC 3. **PRL Core** — Pure Python Fellegi-Sunter scorer
# MAGIC 4. **PRL Scoring** — Score all candidate pairs from vector search
# MAGIC 5. **Results** — Waterfall evidence breakdown, routing decisions
# MAGIC 6. **Stewardship Feedback** — Simulate steward decisions
# MAGIC 7. **EM Re-estimation** — Refine parameters from feedback and re-score

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "lakefusion_dev"
ENTITY = "payer"
VOLUME_PATH = f"/Volumes/{CATALOG}/metadata/metadata_files"

# Table names (aligned with current LakeFusion conventions)
UNIFIED_TABLE = f"{CATALOG}.silver.{ENTITY}_unified_prl_poc"
MASTER_TABLE = f"{CATALOG}.gold.{ENTITY}_master_prl_poc"
PRL_SCORED_TABLE = f"{CATALOG}.gold.{ENTITY}_unified_prl_scored"
PRL_PARAMS_TABLE = f"{CATALOG}.metadata.prl_parameters"
STEWARDSHIP_FEEDBACK_TABLE = f"{CATALOG}.gold.{ENTITY}_stewardship_feedback"

# Vector search config
VS_ENDPOINT = "lakefusion_vs_endpoint"
EMBEDDING_ENDPOINT = "databricks-gte-large-en"
VS_INDEX_NAME = f"{CATALOG}.gold.{ENTITY}_master_prl_poc_index"
MAX_CANDIDATES = 5

# PRL thresholds
AUTO_MATCH_THRESHOLD = 15.0
AUTO_REJECT_THRESHOLD = 5.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data from Volume

# COMMAND ----------

import json
from pyspark.sql import functions as F

def load_json_to_table(json_filename, table_name, timestamp_cols=None):
    """Load a JSON data file from the volume and save as a Delta table."""
    timestamp_cols = timestamp_cols or []
    with open(f"{VOLUME_PATH}/{json_filename}", "r") as f:
        data = json.load(f)
    columns = data["columns"]
    rows = data["data"]
    row_dicts = [{col: val for col, val in zip(columns, row)} for row in rows]
    df = spark.createDataFrame(row_dicts)
    for tc in timestamp_cols:
        if tc in df.columns:
            df = df.withColumn(tc, F.to_timestamp(F.col(tc)))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"✓ {json_filename} → {table_name} ({df.count()} rows)")
    return df

# Load master (golden records)
master_df = load_json_to_table("prl_poc_master.json", MASTER_TABLE, ["src_update_timestamp"])

# Load unified and reset to ACTIVE with empty search_results (simulating fresh incoming records)
unified_df = load_json_to_table("prl_poc_unified.json", UNIFIED_TABLE, ["src_update_timestamp"])

# COMMAND ----------

# Reset unified records to ACTIVE with empty search_results for the POC
# This simulates fresh incoming records that need matching
spark.sql(f"""
    CREATE OR REPLACE TABLE {UNIFIED_TABLE} AS
    SELECT
        surrogate_key,
        source_path,
        source_id,
        CAST(NULL AS STRING) as master_lakefusion_id,
        'ACTIVE' as record_status,
        attributes_combined,
        '' as search_results,
        '' as scoring_results,
        Legal_Name, NPI, State_License_ID, Official_Mailing_Address,
        Payer_Type_Code, Primary_Contact_Phone, Source_System_ID,
        Cleaned_Parent_Name, Cleaned_Parent_HQ_Address, src_update_timestamp
    FROM {UNIFIED_TABLE}
""")

active_count = spark.read.table(UNIFIED_TABLE).filter(F.col("record_status") == "ACTIVE").count()
print(f"✓ Reset {active_count} unified records to ACTIVE with empty search_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Vector Search Index on Master

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Check if index already exists
try:
    existing_index = vsc.get_index(VS_ENDPOINT, VS_INDEX_NAME)
    print(f"Index {VS_INDEX_NAME} already exists — dropping and recreating")
    existing_index.delete()
    import time
    time.sleep(5)
except Exception:
    print(f"No existing index — creating fresh")

# Create delta sync index on master table
index = vsc.create_delta_sync_index(
    endpoint_name=VS_ENDPOINT,
    source_table_name=MASTER_TABLE,
    index_name=VS_INDEX_NAME,
    pipeline_type="TRIGGERED",
    primary_key="lakefusion_id",
    embedding_source_column="attributes_combined",
    embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
)

print(f"✓ Created index {VS_INDEX_NAME}")
print(f"  Endpoint: {VS_ENDPOINT}")
print(f"  Embedding: {EMBEDDING_ENDPOINT}")
print(f"  Source: {MASTER_TABLE}")

# COMMAND ----------

# Wait for index to be ready
import time

def wait_for_index(vsc, endpoint, index_name, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        try:
            idx = vsc.get_index(endpoint, index_name)
            status = idx.describe().get("status", {})
            ready = status.get("ready", False)
            detailed_state = status.get("detailed_state", "")
            indexed_count = status.get("index_status", {}).get("indexed_row_count", 0)
            print(f"  Status: ready={ready}  state={detailed_state}  indexed_rows={indexed_count}")
            if ready:
                return idx
        except Exception as e:
            print(f"  Waiting... ({e})")
        time.sleep(15)
    raise TimeoutError(f"Index not ready after {timeout}s")

print("Waiting for index to be ready (this may take 2-5 minutes)...")
index = wait_for_index(vsc, VS_ENDPOINT, VS_INDEX_NAME)
print(f"✓ Index is READY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Vector Search — Populate search_results

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

# Get ACTIVE records needing search
active_df = spark.read.table(UNIFIED_TABLE).filter(
    (F.col("record_status") == "ACTIVE") &
    ((F.col("search_results") == "") | F.col("search_results").isNull())
)

print(f"Records needing vector search: {active_df.count()}")

# Run vector search for each record
def search_one(text, index_ref, max_results):
    """Query vector search for one record."""
    try:
        results = index_ref.similarity_search(
            query_text=text,
            columns=["attributes_combined", "lakefusion_id"],
            num_results=max_results,
        )
        formatted = []
        for r in results.get("result", {}).get("data_array", []):
            formatted.append({
                "text": r[0],
                "lakefusion_id": r[1],
                "score": float(r[2]) if len(r) > 2 else 0.0,
            })
        return formatted
    except Exception as e:
        print(f"  VS error: {e}")
        return []

# Collect active records and run search
active_records = active_df.select("surrogate_key", "attributes_combined").collect()
search_results_map = {}

print(f"Running vector search for {len(active_records)} records...")

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {}
    for row in active_records:
        sk = row["surrogate_key"]
        text = row["attributes_combined"]
        if text:
            futures[executor.submit(search_one, text, index, MAX_CANDIDATES)] = sk

    done = 0
    for future in as_completed(futures):
        sk = futures[future]
        results = future.result()
        # Format as the standard search_results string
        if results:
            parts = [f"[{r['text']}, {r['lakefusion_id']}, {r['score']}]" for r in results]
            search_results_map[sk] = ", ".join(parts)
        done += 1
        if done % 50 == 0:
            print(f"  Searched {done}/{len(active_records)} records")

print(f"✓ Vector search complete — {len(search_results_map)} records got candidates")

# COMMAND ----------

# Update unified table with search results
from pyspark.sql.types import StringType

# Create a lookup DataFrame
sr_rows = [{"surrogate_key": sk, "new_search_results": sr} for sk, sr in search_results_map.items()]
if sr_rows:
    sr_df = spark.createDataFrame(sr_rows)

    # Join and update
    updated_df = (spark.read.table(UNIFIED_TABLE)
        .alias("u")
        .join(sr_df.alias("s"), F.col("u.surrogate_key") == F.col("s.surrogate_key"), "left")
        .select(
            "u.*",
        )
    )

    # Use merge or overwrite
    spark.sql(f"""
        MERGE INTO {UNIFIED_TABLE} AS target
        USING (SELECT surrogate_key, new_search_results FROM {{sr_table}}) AS source
        ON target.surrogate_key = source.surrogate_key
        WHEN MATCHED THEN UPDATE SET target.search_results = source.new_search_results
    """.replace("{sr_table}", "sr_lookup"))

# Actually, simpler approach — just do it with a temp view
    sr_df.createOrReplaceTempView("sr_lookup")
    spark.sql(f"""
        MERGE INTO {UNIFIED_TABLE} AS target
        USING sr_lookup AS source
        ON target.surrogate_key = source.surrogate_key
        WHEN MATCHED THEN UPDATE SET target.search_results = source.new_search_results
    """)

    # Verify
    filled = spark.read.table(UNIFIED_TABLE).filter(
        (F.col("search_results").isNotNull()) & (F.col("search_results") != "")
    ).count()
    print(f"✓ Updated search_results — {filled} records now have candidates")
else:
    print("✗ No search results returned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: PRL Core Engine (Pure Python)
# MAGIC
# MAGIC ~120 lines. No Spark. Portable to API layer or unit tests.

# COMMAND ----------

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import Counter


@dataclass
class ComparisonLevel:
    level: int
    description: str
    comparator: str
    threshold: Optional[float]
    m_probability: float = 0.9
    u_probability: float = 0.01

    @property
    def match_weight(self) -> float:
        if self.u_probability <= 0 or self.m_probability <= 0:
            return 0.0
        return math.log2(self.m_probability / max(self.u_probability, 1e-10))


@dataclass
class AttributeConfig:
    attribute_name: str
    evidence_type: str
    comparison_levels: List[ComparisonLevel] = field(default_factory=list)
    missing_weight: float = 0.0
    is_frequency_weighted: bool = False
    frequency_table: Dict[str, float] = field(default_factory=dict)


@dataclass
class ScoringResult:
    total_match_weight: float
    match_probability: float
    routing_decision: str
    attribute_weights: Dict[str, float] = field(default_factory=dict)
    attribute_levels: Dict[str, int] = field(default_factory=dict)
    attribute_descriptions: Dict[str, str] = field(default_factory=dict)


def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """Jaro-Winkler similarity (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    s1u, s2u = s1.upper(), s2.upper()
    if s1u == s2u:
        return 1.0
    len1, len2 = len(s1u), len(s2u)
    match_dist = max(len1, len2) // 2 - 1
    if match_dist < 0:
        match_dist = 0
    s1m = [False] * len1
    s2m = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len2)
        for j in range(start, end):
            if s2m[j] or s1u[i] != s2u[j]:
                continue
            s1m[i] = True
            s2m[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1m[i]:
            continue
        while not s2m[k]:
            k += 1
        if s1u[i] != s2u[k]:
            transpositions += 1
        k += 1
    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1u[i] == s2u[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    return ''.join(c for c in str(phone) if c.isdigit())


def compare_values(left: str, right: str, comparator: str, threshold: Optional[float]) -> bool:
    if comparator == "exact":
        return bool(left and right and left.strip().upper() == right.strip().upper())
    elif comparator == "exact_digits":
        l, r = normalize_phone(left), normalize_phone(right)
        return l == r and l != ""
    elif comparator == "jaro_winkler":
        if not left or not right:
            return False
        return jaro_winkler_similarity(str(left), str(right)) >= (threshold or 0.0)
    return False


class FellegiSunterScorer:
    def __init__(self, attribute_configs: List[AttributeConfig],
                 auto_match_threshold: float = 15.0,
                 auto_reject_threshold: float = 5.0):
        self.configs = {ac.attribute_name: ac for ac in attribute_configs}
        self.auto_match_threshold = auto_match_threshold
        self.auto_reject_threshold = auto_reject_threshold

    def compare_attribute(self, attr_name: str, left_val: Any, right_val: Any) -> Tuple[int, float, str]:
        config = self.configs[attr_name]
        if left_val is None or right_val is None or str(left_val).strip() == "" or str(right_val).strip() == "":
            return (-1, config.missing_weight, "Null/Empty")
        left_str, right_str = str(left_val).strip(), str(right_val).strip()
        for cl in config.comparison_levels:
            if cl.level == len(config.comparison_levels) - 1:
                break
            if compare_values(left_str, right_str, cl.comparator, cl.threshold):
                weight = cl.match_weight
                if config.is_frequency_weighted and config.frequency_table:
                    freq = config.frequency_table.get(left_str.upper(), cl.u_probability)
                    if freq > 0:
                        weight = math.log2(cl.m_probability / max(freq, 1e-10))
                return (cl.level, weight, cl.description)
        else_level = config.comparison_levels[-1]
        return (else_level.level, else_level.match_weight, else_level.description)

    def score_pair(self, left_record: dict, right_record: dict) -> ScoringResult:
        total_weight = 0.0
        attr_weights, attr_levels, attr_descs = {}, {}, {}
        for attr_name in self.configs:
            level, weight, desc = self.compare_attribute(
                attr_name, left_record.get(attr_name), right_record.get(attr_name))
            total_weight += weight
            attr_weights[attr_name] = round(weight, 2)
            attr_levels[attr_name] = level
            attr_descs[attr_name] = desc
        try:
            probability = 2**total_weight / (1 + 2**total_weight)
        except OverflowError:
            probability = 1.0
        if total_weight >= self.auto_match_threshold:
            routing = "AUTO_MATCH"
        elif total_weight < self.auto_reject_threshold:
            routing = "REJECT"
        else:
            routing = "REVIEW"
        return ScoringResult(round(total_weight, 2), probability, routing, attr_weights, attr_levels, attr_descs)


print("✓ PRL Core Engine loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Configure Payer Comparison Levels
# MAGIC
# MAGIC This is what the admin selects in Match Maven — evidence type per attribute.

# COMMAND ----------

def build_payer_config() -> List[AttributeConfig]:
    configs = []

    # Legal_Name — Name / Free Text (4 levels)
    configs.append(AttributeConfig(
        attribute_name="Legal_Name", evidence_type="name", is_frequency_weighted=True,
        comparison_levels=[
            ComparisonLevel(0, "Exact",          "exact",        None, 0.62, 0.00003),
            ComparisonLevel(1, "Very Close",     "jaro_winkler", 0.92, 0.25, 0.0004),
            ComparisonLevel(2, "Somewhat Close", "jaro_winkler", 0.80, 0.10, 0.003),
            ComparisonLevel(3, "Different",      "else",         None, 0.03, 0.9966),
        ],
    ))

    # NPI — Identifier (2 levels)
    configs.append(AttributeConfig(
        attribute_name="NPI", evidence_type="identifier",
        comparison_levels=[
            ComparisonLevel(0, "Exact",     "exact", None, 0.98, 0.00001),
            ComparisonLevel(1, "Different", "else",  None, 0.02, 0.9999),
        ],
    ))

    # Official_Mailing_Address — Name / Free Text (4 levels)
    configs.append(AttributeConfig(
        attribute_name="Official_Mailing_Address", evidence_type="name",
        comparison_levels=[
            ComparisonLevel(0, "Exact",          "exact",        None, 0.55, 0.0001),
            ComparisonLevel(1, "Very Close",     "jaro_winkler", 0.90, 0.25, 0.001),
            ComparisonLevel(2, "Somewhat Close", "jaro_winkler", 0.75, 0.10, 0.01),
            ComparisonLevel(3, "Different",      "else",         None, 0.10, 0.9889),
        ],
    ))

    # Primary_Contact_Phone — Identifier (2 levels)
    configs.append(AttributeConfig(
        attribute_name="Primary_Contact_Phone", evidence_type="identifier",
        comparison_levels=[
            ComparisonLevel(0, "Exact",     "exact_digits", None, 0.85, 0.00008),
            ComparisonLevel(1, "Different", "else",         None, 0.15, 0.99992),
        ],
    ))

    # Cleaned_Parent_Name — Name / Free Text (4 levels)
    configs.append(AttributeConfig(
        attribute_name="Cleaned_Parent_Name", evidence_type="name", is_frequency_weighted=True,
        comparison_levels=[
            ComparisonLevel(0, "Exact",          "exact",        None, 0.80, 0.005),
            ComparisonLevel(1, "Very Close",     "jaro_winkler", 0.90, 0.12, 0.008),
            ComparisonLevel(2, "Somewhat Close", "jaro_winkler", 0.75, 0.05, 0.02),
            ComparisonLevel(3, "Different",      "else",         None, 0.03, 0.967),
        ],
    ))

    # Payer_Type_Code — Structured (2 levels)
    configs.append(AttributeConfig(
        attribute_name="Payer_Type_Code", evidence_type="structured",
        comparison_levels=[
            ComparisonLevel(0, "Exact",     "exact", None, 0.95, 0.20),
            ComparisonLevel(1, "Different", "else",  None, 0.05, 0.80),
        ],
    ))

    return configs


payer_configs = build_payer_config()

print("=" * 80)
print("PAYER MASTER — PRL COMPARISON LEVELS (seed defaults)")
print("=" * 80)
for cfg in payer_configs:
    freq_tag = " [freq-weighted]" if cfg.is_frequency_weighted else ""
    print(f"\n{cfg.attribute_name} ({cfg.evidence_type}){freq_tag}")
    for cl in cfg.comparison_levels:
        print(f"  Level {cl.level}: {cl.description:16s}  m={cl.m_probability:.4f}  "
              f"u={cl.u_probability:.6f}  weight={cl.match_weight:+.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Build Frequency Tables

# COMMAND ----------

master_df = spark.read.table(MASTER_TABLE)
freq_weighted_attrs = [c for c in payer_configs if c.is_frequency_weighted]

for cfg in freq_weighted_attrs:
    col_name = cfg.attribute_name
    total = master_df.count()
    freq_df = (master_df
               .filter(F.col(col_name).isNotNull())
               .groupBy(F.upper(F.trim(F.col(col_name))).alias("value"))
               .count()
               .withColumn("frequency", F.col("count") / F.lit(total)))
    freq_map = {row["value"]: row["frequency"] for row in freq_df.collect()}
    cfg.frequency_table = freq_map

    print(f"\n{col_name} — {len(freq_map)} unique values:")
    for val, freq in sorted(freq_map.items(), key=lambda x: -x[1])[:8]:
        adj_weight = math.log2(cfg.comparison_levels[0].m_probability / max(freq, 1e-10))
        print(f"  {val:50s}  freq={freq:.4f}  exact_weight={adj_weight:+.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Score All Candidate Pairs with PRL

# COMMAND ----------

# Load unified records that now have search_results
unified_with_sr = (spark.read.table(UNIFIED_TABLE)
    .filter(F.col("search_results").isNotNull())
    .filter(F.col("search_results") != "")
)

print(f"Records with search results: {unified_with_sr.count()}")

# Parse search_results and build candidate pairs
def parse_search_results(sr_str):
    if not sr_str:
        return []
    results = []
    parts = sr_str.replace("[", "").split("],")
    for part in parts:
        part = part.replace("]", "").strip()
        if not part:
            continue
        # Format: "text, lakefusion_id, score"
        # lakefusion_id is a 32-char hex, score is a float at the end
        # Split from the right to handle commas in the text
        pieces = part.rsplit(",", 2)
        if len(pieces) >= 2:
            try:
                score = float(pieces[-1].strip())
                lf_id = pieces[-2].strip()
                results.append((lf_id, score))
            except (ValueError, IndexError):
                continue
    return results

# Build pairs
master_dict = {row["lakefusion_id"]: row.asDict() for row in master_df.collect()}
pair_rows = []

for urow in unified_with_sr.collect():
    u_dict = urow.asDict()
    candidates = parse_search_results(u_dict.get("search_results", ""))
    for lf_id, vs_score in candidates:
        if lf_id in master_dict:
            pair_rows.append({
                "source_surrogate_key": u_dict["surrogate_key"],
                "candidate_lakefusion_id": lf_id,
                "vs_score": vs_score,
                "source_record": u_dict,
                "candidate_record": master_dict[lf_id],
            })

print(f"Total candidate pairs to score: {len(pair_rows)}")

# COMMAND ----------

# Score all pairs
scorer = FellegiSunterScorer(payer_configs, AUTO_MATCH_THRESHOLD, AUTO_REJECT_THRESHOLD)

scored_results = []
for pair in pair_rows:
    result = scorer.score_pair(pair["source_record"], pair["candidate_record"])
    scored_results.append({
        "source_surrogate_key": pair["source_surrogate_key"],
        "candidate_lakefusion_id": pair["candidate_lakefusion_id"],
        "source_legal_name": pair["source_record"].get("Legal_Name", ""),
        "candidate_legal_name": pair["candidate_record"].get("Legal_Name", ""),
        "vs_score": pair["vs_score"],
        "total_match_weight": result.total_match_weight,
        "match_probability": result.match_probability,
        "routing_decision": result.routing_decision,
        "attribute_weights": json.dumps(result.attribute_weights),
        "attribute_levels": json.dumps(result.attribute_levels),
        "attribute_descriptions": json.dumps(result.attribute_descriptions),
    })

# Routing summary
routing_counts = Counter(r["routing_decision"] for r in scored_results)
print(f"\nPRL Routing decisions ({len(scored_results)} pairs):")
for decision, count in sorted(routing_counts.items()):
    pct = count / len(scored_results) * 100
    print(f"  {decision:12s}: {count:4d} ({pct:.1f}%)")

# COMMAND ----------

# Save scored results to Delta
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

scored_schema = StructType([
    StructField("source_surrogate_key", StringType()),
    StructField("candidate_lakefusion_id", StringType()),
    StructField("source_legal_name", StringType()),
    StructField("candidate_legal_name", StringType()),
    StructField("vs_score", DoubleType()),
    StructField("total_match_weight", DoubleType()),
    StructField("match_probability", DoubleType()),
    StructField("routing_decision", StringType()),
    StructField("attribute_weights", StringType()),
    StructField("attribute_levels", StringType()),
    StructField("attribute_descriptions", StringType()),
])

scored_df = spark.createDataFrame(scored_results, schema=scored_schema)
scored_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PRL_SCORED_TABLE)
print(f"✓ Saved to {PRL_SCORED_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Results — Evidence Waterfall

# COMMAND ----------

sorted_results = sorted(scored_results, key=lambda x: x["total_match_weight"], reverse=True)

print("=" * 110)
print("PRL EVIDENCE WATERFALL — TOP MATCHES")
print("=" * 110)

for i, r in enumerate(sorted_results[:20]):
    weights = json.loads(r["attribute_weights"])
    descs = json.loads(r["attribute_descriptions"])
    emoji = {"AUTO_MATCH": "✅", "REVIEW": "🔶", "REJECT": "❌"}[r["routing_decision"]]

    print(f"\n{'─' * 110}")
    print(f"{emoji}  {r['source_legal_name']}  ↔  {r['candidate_legal_name']}")
    print(f"   Weight: {r['total_match_weight']:+.2f}  |  P(match): {r['match_probability']:.8f}  "
          f"|  VS: {r['vs_score']:.4f}  |  Routing: {r['routing_decision']}")
    print()

    max_w = max(abs(w) for w in weights.values()) if weights else 1
    for attr, weight in sorted(weights.items(), key=lambda x: -x[1]):
        desc = descs.get(attr, "")
        bar_len = int(abs(weight) / max_w * 30) if max_w > 0 else 0
        if weight > 0:
            bar = "█" * bar_len
            print(f"   {attr:30s} {desc:16s} {bar:30s} {weight:+.2f}")
        elif weight < 0:
            bar = "░" * bar_len
            print(f"   {attr:30s} {desc:16s} {bar:>30s} {weight:+.2f}")
        else:
            print(f"   {attr:30s} {desc:16s} {'·':>30s}  0.00")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Stewardship Feedback Simulation

# COMMAND ----------

from pyspark.sql.types import TimestampType
from datetime import datetime

now = datetime.now()
simulated_feedback = []

for r in scored_results:
    if r["routing_decision"] != "REVIEW":
        continue
    weights = json.loads(r["attribute_weights"])
    npi_w = weights.get("NPI", 0)
    addr_w = weights.get("Official_Mailing_Address", 0)

    # Steward logic: same NPI + same/close address → merge; different address → reject
    if npi_w > 10 and addr_w > 0:
        decision = "MERGE"
    elif npi_w > 10 and addr_w < -3:
        decision = "NOT_A_MATCH"
    else:
        decision = "MERGE"

    simulated_feedback.append({
        "source_record_id": r["source_surrogate_key"],
        "candidate_record_id": r["candidate_lakefusion_id"],
        "decision": decision,
        "scored_by": "PRL",
        "score_at_decision": r["total_match_weight"],
        "attribute_levels_snapshot": r["attribute_levels"],
        "decided_by": "steward_demo",
        "decided_at": now,
    })

if simulated_feedback:
    fb_schema = StructType([
        StructField("source_record_id", StringType()),
        StructField("candidate_record_id", StringType()),
        StructField("decision", StringType()),
        StructField("scored_by", StringType()),
        StructField("score_at_decision", DoubleType()),
        StructField("attribute_levels_snapshot", StringType()),
        StructField("decided_by", StringType()),
        StructField("decided_at", TimestampType()),
    ])
    fb_df = spark.createDataFrame(simulated_feedback, schema=fb_schema)
    fb_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STEWARDSHIP_FEEDBACK_TABLE)
    print(f"✓ {len(simulated_feedback)} feedback records → {STEWARDSHIP_FEEDBACK_TABLE}")
    merge_count = sum(1 for f in simulated_feedback if f["decision"] == "MERGE")
    reject_count = sum(1 for f in simulated_feedback if f["decision"] == "NOT_A_MATCH")
    print(f"  MERGE: {merge_count}  |  NOT_A_MATCH: {reject_count}")
else:
    print("No REVIEW pairs to provide feedback on")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: EM Re-estimation with Feedback

# COMMAND ----------

import copy

def em_estimate_with_feedback(scored_pairs, feedback_pairs, attribute_configs, max_iterations=20):
    configs = copy.deepcopy(attribute_configs)
    feedback_lookup = {}
    for fb in feedback_pairs:
        key = (fb["source_record_id"], fb["candidate_record_id"])
        feedback_lookup[key] = 1.0 if fb["decision"] == "MERGE" else 0.0

    pair_data = []
    for sp in scored_pairs:
        key = (sp["source_surrogate_key"], sp["candidate_lakefusion_id"])
        pair_data.append({
            "key": key,
            "levels": json.loads(sp["attribute_levels"]),
            "pinned": feedback_lookup.get(key),
        })

    print(f"EM: {len(pair_data)} pairs, {len(feedback_lookup)} pinned from stewardship")

    for iteration in range(max_iterations):
        # E-step
        for pd_item in pair_data:
            if pd_item["pinned"] is not None:
                pd_item["p_match"] = pd_item["pinned"]
            else:
                weight = 0.0
                cfg_map = {c.attribute_name: c for c in configs}
                for attr_name, cfg in cfg_map.items():
                    level = pd_item["levels"].get(attr_name, -1)
                    for cl in cfg.comparison_levels:
                        if cl.level == level:
                            weight += cl.match_weight
                            break
                try:
                    pd_item["p_match"] = 2**weight / (1 + 2**weight)
                except OverflowError:
                    pd_item["p_match"] = 1.0

        # M-step
        max_change = 0.0
        for cfg in configs:
            for cl in cfg.comparison_levels:
                m_num = sum(p["p_match"] for p in pair_data if p["levels"].get(cfg.attribute_name) == cl.level)
                u_num = sum(1 - p["p_match"] for p in pair_data if p["levels"].get(cfg.attribute_name) == cl.level)
                m_den = sum(p["p_match"] for p in pair_data) + 0.01
                u_den = sum(1 - p["p_match"] for p in pair_data) + 0.01

                new_m = max(0.001, min(0.999, (m_num + 0.01) / m_den))
                new_u = max(0.000001, min(0.999, (u_num + 0.01) / u_den))

                max_change = max(max_change, abs(new_m - cl.m_probability), abs(new_u - cl.u_probability))
                cl.m_probability = new_m
                cl.u_probability = new_u

        if max_change < 0.001:
            print(f"  Converged at iteration {iteration + 1}")
            break
    else:
        print(f"  Reached max iterations, change={max_change:.6f}")

    return configs


updated_configs = em_estimate_with_feedback(scored_results, simulated_feedback, payer_configs)

print("\n" + "=" * 100)
print("PARAMETERS: SEED → POST-FEEDBACK")
print("=" * 100)
for orig, updated in zip(payer_configs, updated_configs):
    print(f"\n{orig.attribute_name}")
    for ocl, ucl in zip(orig.comparison_levels, updated.comparison_levels):
        w_delta = ucl.match_weight - ocl.match_weight
        print(f"  {ocl.description:16s}  weight: {ocl.match_weight:+.2f} → {ucl.match_weight:+.2f} ({w_delta:+.2f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Re-Score with Updated Parameters

# COMMAND ----------

updated_scorer = FellegiSunterScorer(updated_configs, AUTO_MATCH_THRESHOLD, AUTO_REJECT_THRESHOLD)

rescored = []
for pair in pair_rows:
    result = updated_scorer.score_pair(pair["source_record"], pair["candidate_record"])
    rescored.append({
        "source_legal_name": pair["source_record"].get("Legal_Name", ""),
        "candidate_legal_name": pair["candidate_record"].get("Legal_Name", ""),
        "total_match_weight": result.total_match_weight,
        "routing_decision": result.routing_decision,
    })

before = Counter(r["routing_decision"] for r in scored_results)
after = Counter(r["routing_decision"] for r in rescored)

print("=" * 60)
print("ROUTING: BEFORE vs AFTER EM RE-ESTIMATION")
print("=" * 60)
print(f"\n{'Decision':14s}  {'Before':>8s}  {'After':>8s}  {'Change':>8s}")
print("─" * 50)
for d in ["AUTO_MATCH", "REVIEW", "REJECT"]:
    b, a = before.get(d, 0), after.get(d, 0)
    print(f"{d:14s}  {b:8d}  {a:8d}  {a-b:+8d}")

print(f"\nPairs that changed routing:")
for orig, rescore in zip(scored_results, rescored):
    if orig["routing_decision"] != rescore["routing_decision"]:
        print(f"  {rescore['source_legal_name']} ↔ {rescore['candidate_legal_name']}")
        print(f"    {orig['routing_decision']} ({orig['total_match_weight']:+.2f}) → "
              f"{rescore['routing_decision']} ({rescore['total_match_weight']:+.2f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Save Updated Parameters

# COMMAND ----------

import uuid

version_id = f"v2_{str(uuid.uuid4())[:8]}"
param_rows = []

for cfg in updated_configs:
    for cl in cfg.comparison_levels:
        param_rows.append({
            "entity": ENTITY,
            "version_id": version_id,
            "attribute_name": cfg.attribute_name,
            "evidence_type": cfg.evidence_type,
            "comparison_level": cl.level,
            "comparison_description": cl.description,
            "comparator": cl.comparator,
            "threshold": cl.threshold,
            "m_probability": round(cl.m_probability, 6),
            "u_probability": round(cl.u_probability, 8),
            "match_weight": round(cl.match_weight, 2),
            "is_frequency_weighted": cfg.is_frequency_weighted,
            "is_active": True,
            "created_at": datetime.now(),
        })

params_df = spark.createDataFrame(param_rows)
params_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PRL_PARAMS_TABLE)
print(f"✓ {len(param_rows)} params → {PRL_PARAMS_TABLE} (version: {version_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What happened |
# MAGIC |------|---------------|
# MAGIC | 1-2 | Loaded prod payer data, reset to ACTIVE |
# MAGIC | 3 | Created vector search index on master |
# MAGIC | 4 | Ran vector search → populated search_results |
# MAGIC | 5-6 | Configured comparison levels + frequency tables |
# MAGIC | 7-8 | Scored pairs with PRL, showed waterfall breakdowns |
# MAGIC | 9 | Simulated steward feedback (merge/reject decisions) |
# MAGIC | 10-11 | EM re-estimated params from feedback, re-scored |
# MAGIC | 12 | Saved versioned parameters |
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `lakefusion_dev.gold.payer_unified_prl_scored` — PRL scoring with weight decomposition
# MAGIC - `lakefusion_dev.gold.payer_stewardship_feedback` — shared feedback (PRL + LLM)
# MAGIC - `lakefusion_dev.metadata.prl_parameters` — versioned m/u parameters
