# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""Read existing FIAR documents and accounting data and build a review snapshot."""
import hashlib
import json
import re
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from fiar_candidate_coverage_summary

# COMMAND ----------

# Edit this cell if your source names or columns differ.
NAMESPACE = "workspace.default"
VOLUME_ROOT = "/Volumes/workspace/default/fiar-docs"
REPOSITORY = f"{NAMESPACE}.discovery_document_repository"   # None to infer from folders
UPLOADS = f"{NAMESPACE}.fiar_evidence_uploads"             # optional existing app registry
INVOICE_TABLE = f"{NAMESPACE}.payables_vendor_invoices"
DISBURSEMENT_TABLE = f"{NAMESPACE}.genledger_disbursements"
INVOICE_COLUMNS = dict(id="invoice_id", invoice="invoice_number", amount="total_amount")
DISBURSEMENT_COLUMNS = dict(id="disbursement_id", dssn="dssn", voucher="voucher_number",
                            schedule="treasury_schedule_no", amount="amount")
CACHE = f"{NAMESPACE}.fiar_extract_cache_v2"
PREVIEW = f"{NAMESPACE}.fiar_ai_link_review_preview"
SMOKE_PREVIEW = f"{NAMESPACE}.fiar_ai_link_review_smoke"
REVERSE = f"{NAMESPACE}.fiar_candidate_coverage_preview"
COVERAGE_SUMMARY = f"{NAMESPACE}.fiar_candidate_coverage_summary"  # SQL view, no extra Delta table
COVERAGE_SINGLE_CURRENCY = None  # Set only for a verified single-currency population.
# Only set if your workspace's reviewer links should use this host.
WORKSPACE_HOST = ""  # e.g. https://adb-<workspace>.<number>.azuredatabricks.net

# Map optional keys only to VERIFIED existing accounting columns.
INVOICE_COLUMNS.update(contract=None, vendor_code=None, currency=None, date=None)
DISBURSEMENT_COLUMNS.update(contract=None, vendor_code=None, currency=None, date=None, alc=None)
NORMALIZE_SEPARATORS = dict(invoice=True, schedule=True, voucher=False, dssn=False)
MATCH_RULES_VERSION = "fiar-exact-normalized-fuzzy-v1"
FUZZY_POLICY = dict(enabled=True, min_similarity=0.85, max_edits=1,
                    min_identifier_length=6, amount_tolerance="0.01",
                    date_window_days=45, max_block_pairs=100,
                    single_currency=None)  # Only set an approved population currency.

# COMMAND ----------

def field(description, kind="string"):
    return {"type": kind, "description": description}


DETAIL_TYPES = ["sot_detail", "gwa_detail", "contract_pay_detail"]
DOC_TYPES = ["invoice", "voucher"] + DETAIL_TYPES
ALIASES = {
    "invoice": "invoice", "invoices": "invoice",
    "voucher": "voucher", "vouchers": "voucher",
    "sot": "sot_detail", "sot_detail": "sot_detail",
    "gwa": "gwa_detail", "gwa_detail": "gwa_detail",
    "dcas": "contract_pay_detail", "contract_pay_detail": "contract_pay_detail",
}
AMOUNT_TYPE = "decimal(38, 4)"
TEXT_TYPES = ["md", "txt"]
BINARY_TYPES = ["pdf", "png", "jpg", "jpeg", "tif", "tiff", "doc", "docx", "ppt", "pptx"]

# Extract identifiers as strings to preserve their original formatting.
VOUCHER_KEYS = {
    "dssn": field("Disbursing Station Symbol Number as printed, including leading zeros"),
    "voucher_number": field("Disbursement voucher identifier as printed, including leading zeros"),
}
COMMON = {
    "vendor_code": field("Vendor code only if explicitly printed; never infer it from a vendor name"),
    "contract_number": field("Contract or order identifier printed on this record"),
    "vendor_name": field("Vendor name printed on this record"),
    "document_date": field("Record date, ISO YYYY-MM-DD if unambiguous"),
    "currency": field("ISO currency code only if explicitly indicated"),
}
SCHEMAS = {
    "invoice": {
        **COMMON,
        "invoice_number": field("Invoice identifier exactly as printed"),
        "doc_amount": field("Final invoice total payable; not subtotal or line amount", "number"),
    },
    "voucher": {
        **COMMON, **VOUCHER_KEYS,
        "state_voucher": field("State voucher identifier, null if absent"),
        "doc_amount": field("Final voucher payment amount", "number"),
    },
}
for name in DETAIL_TYPES:
    keys = ({"reference_number": field("Treasury schedule reference printed on this row"),
             "alc": field("Agency Location Code on this row")}
            if name == "gwa_detail" else VOUCHER_KEYS)
    SCHEMAS[name] = {"transaction_rows": {
        "type": "array",
        "description": "Every transaction row in document order. Exclude subtotals and headers.",
        "items": {"type": "object", "properties": {
            **COMMON, **keys,
            "doc_amount": field("Signed transaction amount on this row", "number"),
        }},
    }}

OPTIONS = {
    "version": "2.1", "enableCitations": "true", "enableConfidenceScores": "true",
    "instructions": "Extract only facts explicitly supported by the input. Return null for "
                    "missing values. Preserve identifiers and amount signs. For reports "
                    "keep each transaction row separate; never borrow neighboring values.",
}
PARSER_OPTIONS = {"version": "2.0", "descriptionElementTypes": ""}
EXTRACTOR_REVISION = hashlib.sha256(
    json.dumps([SCHEMAS, OPTIONS, PARSER_OPTIONS, "pipeline-v1"], sort_keys=True).encode()
).hexdigest()[:16]

# COMMAND ----------

def quote(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Use a plain catalog, schema or column identifier: {name!r}")
    return f"`{name}`"


def table_name(name):
    parts = name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table: {name!r}")
    return ".".join(quote(p) for p in parts)


def normalize_uri(c):
    return F.regexp_replace(c, r"^(?:dbfs:|file:)(?=/Volumes/)", "")


def required(df, columns, source):
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"{source} is missing {sorted(missing)}. Check its schema and update the configuration.")
    return df


def assert_unique(df, cols, context):
    if df.groupBy(*cols).count().where(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{context} has duplicate keys for {cols}. Resolve duplicates before continuing.")


def alias_type(c):
    aliases = F.create_map(*[F.lit(x) for pair in ALIASES.items() for x in pair])
    return F.element_at(aliases, F.lower(F.trim(c)))


def inventory(spark, root, repository, uploads, sample_per_type):
    if not root.startswith("/Volumes/"):
        raise ValueError("Set VOLUME_ROOT to a Databricks Unity Catalog volume path.")
    # Listing requests metadata only. The selected paths are read as bytes later.
    suffixes = ",".join(f"*.[{e[0].lower()}{e[0].upper()}]" + "".join(
        f"[{ch.lower()}{ch.upper()}]" for ch in e[1:]) for e in TEXT_TYPES + BINARY_TYPES)
    files = (spark.read.format("binaryFile").option("recursiveFileLookup", "true")
             .option("pathGlobFilter", "{" + suffixes + "}").load(root)
             .select(normalize_uri(F.col("path")).alias("storage_uri"), "length", "modificationTime")
             .withColumn("extension", F.lower(F.regexp_extract("storage_uri", r"\.([^.\/]+)$", 1))))
    folder = F.element_at(F.split(F.col("storage_uri").substr(len(root.rstrip("/")) + 2, 100000), "/"), 1)
    files = files.withColumn("_folder_type", alias_type(folder))
    refs = []
    for source, priority in [(repository, 1), (uploads, 2)]:
        if not source:
            continue
        exists = spark.catalog.tableExists(source)
        if not exists and source == repository:
            raise ValueError(f"Repository {source} does not exist. Set REPOSITORY=None to use folder names.")
        if exists:
            df = required(spark.table(table_name(source)), ["storage_uri", "document_id", "document_type"], source)
            refs.append(df.select(normalize_uri(F.col("storage_uri")).alias("storage_uri"),
                                  F.col("document_id").cast("string").alias("_id"),
                                  alias_type(F.col("document_type")).alias("_kind"),
                                  F.lit(priority).alias("_priority"))
                        .filter(F.col("_id").isNotNull()))
    if refs:
        ref = reduce(lambda a, b: a.unionByName(b), refs).distinct()
        assert_unique(ref, ["storage_uri", "_priority"], "Document index")
        w = Window.partitionBy("storage_uri").orderBy(F.desc("_priority"))
        ref = ref.withColumn("_n", F.row_number().over(w)).where("_n = 1").drop("_n", "_priority")
        files = files.join(ref, "storage_uri", "left")
    else:
        files = files.withColumn("_id", F.lit(None).cast("string")).withColumn("_kind", F.lit(None).cast("string"))
    files = (files.withColumn("document_id", F.coalesce("_id", F.sha2("storage_uri", 256)))
             .withColumn("document_type", F.coalesce("_kind", "_folder_type", F.lit("unknown")))
             .withColumn("file_name", F.regexp_extract("storage_uri", r"([^/]+)$", 1))
             .drop("_kind", "_folder_type", "_id"))
    if sample_per_type > 0:
        w = Window.partitionBy("document_type").orderBy("storage_uri")
        files = files.withColumn("_n", F.row_number().over(w)).where(F.col("_n") <= sample_per_type).drop("_n")
    return files


def add_content(spark, docs, batch_paths):
    valid = F.col("document_type").isin(DOC_TYPES) & F.col("length").between(1, 20 * 1024 * 1024)
    eligible = docs.where(valid)
    rejected = (docs.where(~valid).withColumn("content", F.lit(None).cast("binary"))
                .withColumn("document_content_hash", F.lit(None).cast("string")))
    # Only path strings reach the driver; avoid opening oversized or unmapped files.
    paths = [r.storage_uri for r in eligible.select("storage_uri").orderBy("storage_uri").collect()]
    if not paths:
        return rejected.withColumn("extractor_revision", F.lit(EXTRACTOR_REVISION))
    parts = []
    for i in range(0, len(paths), batch_paths):
        part = (spark.read.format("binaryFile").load(paths[i:i + batch_paths])
                .select(normalize_uri(F.col("path")).alias("storage_uri"),
                        "modificationTime", "length", "content"))
        parts.append(part)
    raw = reduce(lambda a, b: a.unionByName(b), parts)
    meta = eligible.drop("length", "modificationTime")
    accepted = (raw.join(meta, "storage_uri", "inner")
                .withColumn("document_content_hash", F.sha2("content", 256)))
    return accepted.unionByName(rejected).withColumn("extractor_revision", F.lit(EXTRACTOR_REVISION))


def extraction_calls(df):
    # Use SQL expressions inside PySpark so the notebook needs no extra package.
    def args(options):
        return F.create_map(*[F.lit(x) for pair in options.items() for x in pair])
    meta = ["document_id", "document_type", "storage_uri", "file_name",
            "document_content_hash", "extractor_revision"]
    parts = []
    # Do not send unsupported, empty or oversized inputs to either AI function.
    valid = F.col("document_type").isin(DOC_TYPES) & F.col("length").between(1, 20 * 1024 * 1024)
    invalid = df.where(~valid).select(
        *meta, F.lit(None).cast("string").alias("extraction_json"),
        F.when(~F.col("document_type").isin(DOC_TYPES), "Document type not mapped")
         .otherwise("File is empty or exceeds the pipeline's 20 MiB limit").alias("input_error"),
        F.lit(None).cast("string").alias("source_excerpt"))
    for kind, schema in SCHEMAS.items():
        # The JSON schema must be a literal, not a dynamic column.
        schema_literal = F.lit(json.dumps(schema))
        typed = df.where(valid & (F.col("document_type") == kind))
        for plain in (True, False):
            route = typed.where(F.col("extension").isin(TEXT_TYPES) if plain else ~F.col("extension").isin(TEXT_TYPES))
            if plain:
                route = route.withColumn("_input", F.try_validate_utf8(F.col("content").cast("string")))
                err = (F.when(F.col("_input").isNull(), "Document text is not valid UTF-8")
                       .when(F.length(F.trim("_input")) == 0, "Document text is empty"))
                excerpt = F.substring("_input", 1, 1500)
            else:
                route = route.withColumn("_input", F.call_function("ai_parse_document", F.col("content"), args(PARSER_OPTIONS)))
                route = route.withColumn("_parse_errors", F.expr("to_json(_input:error_status)"))
                err = F.when(F.col("_input").isNull(), "Document parser returned no result").when(
                    F.col("_parse_errors").isNotNull() & ~F.col("_parse_errors").isin("null", "[]"),
                    "Document parser reported errors")
                excerpt = F.lit(None).cast("string")
            route = (route.withColumn("input_error", err)
                     .withColumn("source_excerpt", excerpt)
                     .withColumn("extraction_json", F.when(F.col("input_error").isNull(), F.to_json(F.call_function(
                         "ai_extract", F.col("_input"), schema_literal, args(OPTIONS))))))
            parts.append(route.select(*meta, "extraction_json", "input_error", "source_excerpt"))
    return reduce(lambda a, b: a.unionByName(b), [invalid] + parts)


CACHE_KEYS = ["storage_uri", "document_type", "document_content_hash", "extractor_revision"]


def extract_once(spark, content, cache, retry_errors=False):
    qualified_cache = table_name(cache)
    ready = content.where(F.col("document_content_hash").isNotNull())
    rejected = (content.where(F.col("document_content_hash").isNull())
                .select(*CACHE_KEYS, "document_id", "file_name",
                    F.lit(None).cast("string").alias("extraction_json"),
                    F.when(~F.col("document_type").isin(DOC_TYPES), "Document type not mapped")
                     .otherwise("File is empty or exceeds the pipeline's 20 MiB limit").alias("input_error"),
                    F.lit(None).cast("string").alias("source_excerpt"),
                    F.lit(None).cast("timestamp").alias("extracted_at")))
    if not ready.limit(1).count():
        return rejected
    keys = ready.select(*CACHE_KEYS).distinct()
    exists = spark.catalog.tableExists(cache)
    if exists:
        prior = spark.table(qualified_cache).join(keys, CACHE_KEYS, "inner")
        if retry_errors:
            prior = prior.where(F.col("input_error").isNull() & F.col("extraction_json").isNotNull() &
                                F.get_json_object("extraction_json", "$.error_message").isNull() &
                                F.get_json_object("extraction_json", "$.response").isNotNull() &
                                (~F.col("document_type").isin(DETAIL_TYPES) |
                                 (F.coalesce(F.json_array_length(F.get_json_object("extraction_json",
                                    "$.response.transaction_rows")), F.lit(0)) > 0)))
        missing = ready.join(prior.select(*CACHE_KEYS).distinct(), CACHE_KEYS, "left_anti")
    else:
        missing = ready
    # Use one job writer. Successful cached responses are reused after a committed run.
    if missing.limit(1).count():
        (extraction_calls(missing).withColumn("extracted_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(qualified_cache))
    latest = Window.partitionBy(*CACHE_KEYS).orderBy(F.desc("extracted_at"))
    payload = (spark.table(qualified_cache).join(keys, CACHE_KEYS, "inner")
               .withColumn("_n", F.row_number().over(latest)).where("_n = 1")
               .select(*CACHE_KEYS, "extraction_json", "input_error", "source_excerpt", "extracted_at"))
    processed = ready.select(*CACHE_KEYS, "document_id", "file_name").join(payload, CACHE_KEYS, "inner")
    return processed.unionByName(rejected)


# COMMAND ----------

RAW_FIELDS = ["invoice_number", "dssn", "voucher_number", "reference_number",
              "contract_number", "state_voucher", "vendor_name", "document_date",
              "currency", "alc", "doc_amount", "vendor_code"]
VALUE_SCHEMA = T.StructType([
    T.StructField("value", T.StringType()), T.StructField("confidence_score", T.DoubleType()),
    T.StructField("citation_ids", T.ArrayType(T.IntegerType())),
])
RECORD_SCHEMA = T.StructType([T.StructField(c, VALUE_SCHEMA) for c in RAW_FIELDS])
RESPONSE_SCHEMA = T.StructType(list(RECORD_SCHEMA.fields) +
    [T.StructField("transaction_rows", T.ArrayType(RECORD_SCHEMA))])
RESULT_SCHEMA = T.StructType([T.StructField("response", RESPONSE_SCHEMA),
                             T.StructField("error_message", T.StringType())])


def flatten(extracted):
    result = extracted.withColumn("_parsed", F.from_json("extraction_json", RESULT_SCHEMA))
    result = result.withColumn("_error", F.coalesce(
        "input_error", F.col("_parsed.error_message"),
        F.when(F.col("_parsed.response").isNull(), "No structured extraction response")))
    detail = result.where(F.col("document_type").isin(DETAIL_TYPES)).select(
        "*", F.posexplode_outer("_parsed.response.transaction_rows").alias("_position", "_record"))
    single = result.where(~F.col("document_type").isin(DETAIL_TYPES)).select(
        "*", F.lit(None).cast("int").alias("_position"),
        F.struct(*[F.col(f"_parsed.response.{k}").alias(k) for k in RAW_FIELDS]).alias("_record"))
    rows = detail.unionByName(single)
    rows = (rows.withColumn("document_row_id", F.when(F.col("_position").isNull() &
                                                     F.col("document_type").isin(DETAIL_TYPES), "missing")
                            .otherwise(F.coalesce(F.col("_position").cast("string"), F.lit("0"))))
            .withColumn("extraction_error", F.coalesce(
                "_error", F.when(F.col("_position").isNull() & F.col("document_type").isin(DETAIL_TYPES),
                                 "No transaction rows extracted")))
            .withColumn("source_row_key", F.concat_ws(":", "document_id", "document_type", "document_row_id")))
    for k in RAW_FIELDS:
        rows = rows.withColumn(k, F.col(f"_record.{k}.value"))
    rows = rows.withColumn("doc_amount", F.expr(f"try_cast(doc_amount AS {AMOUNT_TYPE})"))
    return (rows.withColumn("field_evidence_json", F.to_json("_record"))
            .withColumn("model_version", F.concat(F.lit("ai_extract:2.1/schema:"), F.col("extractor_revision")))
            .drop("_parsed", "_record", "_position", "_error", "input_error", "extraction_json")
            .withColumn("source_excerpt", F.coalesce("source_excerpt", F.lit(""))))


def key(c):
    trimmed = F.upper(F.trim(c.cast("string")))
    return F.when(F.length(trimmed) > 0, trimmed)


def numeric_identifier(c):
    # Existing DSSN/voucher accounting fields may be numeric. Keep the raw value
    # in evidence, but compare digit-only IDs without redundant leading zeros.
    x = key(c)
    return F.when(x.rlike(r"^[0-9]+$"), F.regexp_replace(x, r"^0+(?=[0-9])", "")).otherwise(x)



def raw_identifier(c):
    value = c.cast("string")
    return F.when(F.length(F.trim(value)) > 0, value)


def normalized_identifier(c, name):
    value = key(c)
    if NORMALIZE_SEPARATORS.get(name, False):
        value = F.regexp_replace(value, r"[\s-]+", "")
        value = F.when(F.length(value) > 0, value)
    if name in {"dssn", "voucher"}:
        value = F.when(value.rlike(r"^[0-9]+$"), F.regexp_replace(value, r"^0+(?=[0-9])", "")).otherwise(value)
    return value


def accounting(df, mapping, schema, table):
    required(df, [v for v in mapping.values() if v], f"{schema}.{table}")
    def get(k):
        return F.col(quote(mapping[k])) if mapping.get(k) else F.lit(None).cast("string")
    return df.select(
        F.lit(schema).alias("target_schema"), F.lit(table).alias("target_table"),
        get("id").cast("string").alias("target_record_id"),
        F.lit(INVOICE_TABLE if table == "vendor_invoices" else DISBURSEMENT_TABLE).alias("target_storage_table"),
        F.expr(f"try_cast({quote(mapping['amount'])} AS {AMOUNT_TYPE})").alias("system_amount"),
        *[raw_identifier(get(k)).alias("_raw_"+k) for k in ["invoice","dssn","voucher","schedule"]],
        *[normalized_identifier(get(k),k).alias("_norm_"+k) for k in ["invoice","dssn","voucher","schedule"]],
        *[key(get(k)).alias("_"+k) for k in ["contract","vendor_code","alc","currency"]],
        F.expr(f"try_cast({quote(mapping['date'])} AS DATE)" if mapping.get("date") else "cast(NULL AS DATE)").alias("_date"),
        F.to_json(F.struct(*[F.col("\x60"+c.replace("\x60","\x60\x60")+"\x60") for c in df.columns])).alias("actual_record_json")
    ).where(F.length(F.trim("target_record_id")) > 0)


def validate_matching_policy(policy):
    from decimal import Decimal, InvalidOperation
    try:
        tolerance = Decimal(str(policy["amount_tolerance"]))
    except (InvalidOperation, KeyError, TypeError) as error:
        raise ValueError("Set a finite non-negative amount tolerance.") from error
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("Set a finite non-negative amount tolerance.")
    if not 0 < policy["min_similarity"] <= 1 or policy["min_identifier_length"] < 2:
        raise ValueError("Invalid fuzzy similarity or minimum identifier length.")
    if policy["max_edits"] < 0 or policy["date_window_days"] < 0 or policy["max_block_pairs"] < 1:
        raise ValueError("Invalid fuzzy edit, date or block size limit.")
    if policy.get("single_currency") is not None and not re.fullmatch("[A-Z]{3}",policy["single_currency"]):
        raise ValueError("single_currency must be an approved uppercase ISO currency code or None.")


def match_rows(rows, tx, policy=None, rejected_pairs=None):
    """The first non-empty tier supplies candidates. No tier automatically approves links.

    Optional rejected_pairs must be filtered to this exact source/accounting
    generation by the caller before passing it. No unversioned rejection list
    is read automatically.
    """
    policy = dict(FUZZY_POLICY if policy is None else policy)
    validate_matching_policy(policy)
    for name in ["vendor_code","document_date","currency","alc","contract_number"]:
        if name not in rows.columns:
            rows = rows.withColumn(name,F.lit(None).cast("string"))
    d = rows
    names = dict(invoice="invoice_number",dssn="dssn",voucher="voucher_number",schedule="reference_number")
    for name,source in names.items():
        d = d.withColumn("_raw_"+name,raw_identifier(F.col(source))).withColumn("_norm_"+name,normalized_identifier(F.col(source),name))
    for name,source in dict(contract="contract_number",vendor_code="vendor_code",alc="alc",currency="currency").items():
        d = d.withColumn("_"+name,key(F.col(source)))
    d = d.withColumn("_date",F.expr("try_cast(document_date AS DATE)"))
    eligible = d.where(F.col("extraction_error").isNull() & F.col("document_type").isin(DOC_TYPES) &
                       ((F.col("document_type") != "voucher") | key(F.col("state_voucher")).isNull()))
    identities = ["source_row_key","target_schema","target_table","target_record_id"]
    if rejected_pairs is not None:
        required(rejected_pairs,identities,"Current-generation rejected pairs")
    specs = [
        (["invoice"],["invoice"],"vendor_invoices","INVOICE_NUMBER"),
        (["voucher","sot_detail","contract_pay_detail"],["dssn","voucher"],"disbursements","DSSN_VOUCHER"),
        (["gwa_detail"],["schedule"],"disbursements","TREASURY_SCHEDULE"),
    ]
    def c(side,name):
        return F.col(side+"."+name)
    def conflict(name):
        return F.coalesce(c("d",name).isNotNull() & c("t",name).isNotNull() &
                          (c("d",name)!=c("t",name)),F.lit(False))
    def project(joined,keys,tier,rule,block):
        return joined.select(c("d","source_row_key"),
            *[c("t",k) for k in ["target_schema","target_table","target_record_id","target_storage_table","system_amount","actual_record_json"]],
            F.lit(tier).alias("match_tier"),F.lit(rule).alias("match_rule"),F.lit(block).alias("block_rule"),
            *[F.to_json(F.struct(*[c(side,prefix+k).alias(k) for k in keys])).alias(out)
              for side,prefix,out in [
                ("d","_raw_","document_raw_keys"),("t","_raw_","accounting_raw_keys"),
                ("d","_norm_","document_normalized_keys"),("t","_norm_","accounting_normalized_keys")]],
            (conflict("_currency")|conflict("_vendor_code")|conflict("_contract")).alias("supporting_attributes_conflict"),
            c("d","_norm_"+keys[-1]).alias("_doc_key"),c("t","_norm_"+keys[-1]).alias("_tx_key"))
    def remove_rejected(frame):
        if rejected_pairs is None:
            return frame
        return frame.join(rejected_pairs.select(*identities).distinct(),identities,"left_anti")
    def remaining(documents,candidates):
        return documents.join(candidates.select("source_row_key").distinct(),"source_row_key","left_anti")
    def deterministic(documents,tier):
        prefix = "_raw_" if tier=="EXACT" else "_norm_"
        results=[]
        for types,keys,table,rule in specs:
            left=documents.where(F.col("document_type").isin(types)).alias("d")
            right=tx.where(F.col("target_table")==table).alias("t")
            condition=reduce(lambda a,b:a&b,[c("d",prefix+k)==c("t",prefix+k) for k in keys])
            result=remove_rejected(project(left.join(right,condition,"inner"),keys,tier,tier+"_"+rule,"IDENTIFIER_EQUALITY"))
            results.append(result.withColumn("edit_distance",F.lit(0))
                .withColumn("identifier_similarity",F.lit(1.0))
                .withColumn("fuzzy_block_size",F.lit(None).cast("long")))
        return reduce(lambda a,b:a.unionByName(b),results)
    exact=deterministic(eligible,"EXACT")
    normalized=deterministic(remaining(eligible,exact),"NORMALIZED")
    leftovers=remaining(remaining(eligible,exact),normalized)
    blocks=[]
    for types,keys,table,rule in specs:
        left=leftovers.where(F.col("document_type").isin(types)).alias("d")
        right=tx.where(F.col("target_table")==table).alias("t")
        if table=="vendor_invoices":
            anchor=(c("d","_contract")==c("t","_contract"))|(c("d","_vendor_code")==c("t","_vendor_code"))
            block_name="SAME_CONTRACT_OR_VENDOR_CODE_AMOUNT_CURRENCY"
        elif keys==["schedule"]:
            anchor=c("d","_alc")==c("t","_alc")
            block_name="SAME_ALC_AMOUNT_CURRENCY"
        else:
            anchor=c("d","_norm_dssn")==c("t","_norm_dssn")
            block_name="SAME_DSSN_AMOUNT_CURRENCY"
        dc=F.coalesce(c("d","_currency"),F.lit(policy.get("single_currency")))
        tc=F.coalesce(c("t","_currency"),F.lit(policy.get("single_currency")))
        date_ok=c("d","_date").isNull()|c("t","_date").isNull()|(
            F.abs(F.datediff(c("d","_date"),c("t","_date")))<=policy["date_window_days"])
        condition=(anchor & (dc==tc) &
            (F.abs(c("d","doc_amount")-c("t","system_amount"))<=F.lit(policy["amount_tolerance"]).cast(AMOUNT_TYPE)) &
            date_ok & ~conflict("_contract") & ~conflict("_vendor_code") & ~conflict("_currency") &
            (F.length(c("d","_norm_"+keys[-1]))>=policy["min_identifier_length"]) &
            (F.length(c("t","_norm_"+keys[-1]))>=policy["min_identifier_length"]) & F.lit(bool(policy["enabled"])))
        blocks.append(remove_rejected(project(left.join(right,condition,"inner"),keys,"FUZZY","FUZZY_"+rule,block_name)))
    block_pairs=reduce(lambda a,b:a.unionByName(b),blocks)
    block_counts=block_pairs.groupBy("source_row_key").agg(F.count("*").alias("fuzzy_block_size"))
    fuzzy=(block_pairs.join(block_counts,"source_row_key").where(F.col("fuzzy_block_size")<=policy["max_block_pairs"])
        .withColumn("edit_distance",F.levenshtein("_doc_key","_tx_key"))
        .withColumn("identifier_similarity",F.lit(1.0)-F.col("edit_distance")/F.greatest(F.length("_doc_key"),F.length("_tx_key")))
        .where((F.col("edit_distance")<=policy["max_edits"]) & (F.col("identifier_similarity")>=policy["min_similarity"])))
    candidates=(exact.unionByName(normalized).unionByName(fuzzy).drop("_doc_key","_tx_key")
                .withColumn("candidate_count",F.count("*").over(Window.partitionBy("source_row_key"))))
    return (rows.join(candidates,"source_row_key","left")
        .join(block_counts.withColumnRenamed("fuzzy_block_size","_block_count"),"source_row_key","left")
        .withColumn("candidate_count",F.coalesce("candidate_count",F.lit(0)))
        .withColumn("match_rules_version",F.lit(MATCH_RULES_VERSION))
        .withColumn("match_policy_json",F.lit(json.dumps({"fuzzy":policy,"normalization":NORMALIZE_SEPARATORS},sort_keys=True)))
        .withColumn("amount_match",F.when(F.col("target_record_id").isNull()|F.col("doc_amount").isNull()|
            F.col("system_amount").isNull(),"UNKNOWN").when(F.col("doc_amount")==F.col("system_amount"),"MATCH").otherwise("MISMATCH"))
        .withColumn("matching_note",F.when(F.col("extraction_error").isNotNull(),"EXTRACTION_FAILED")
            .when((F.col("document_type")=="voucher") & key(F.col("state_voucher")).isNotNull(),"STATE_VOUCHER_RULE_EXCLUDED")
            .when(F.col("match_tier").isNotNull(),F.concat("match_tier",F.lit("_CANDIDATES_FOR_REVIEW")))
            .when(F.lit(not policy["enabled"]),"FUZZY_DISABLED")
            .when(F.col("_block_count")>policy["max_block_pairs"],"FUZZY_BLOCK_TOO_LARGE")
            .when(F.col("_block_count").isNull(),"NO_ELIGIBLE_FUZZY_BLOCK").otherwise("NO_FUZZY_CANDIDATE_ABOVE_THRESHOLD"))
        .withColumn("attempted_tiers",F.when(F.col("extraction_error").isNotNull()|
            ((F.col("document_type")=="voucher") & key(F.col("state_voucher")).isNotNull()),F.array().cast("array<string>"))
            .when(F.col("match_tier")=="EXACT",F.array(F.lit("EXACT")))
            .when((F.col("match_tier")=="NORMALIZED")|F.lit(not policy["enabled"]),F.array(F.lit("EXACT"),F.lit("NORMALIZED")))
            .otherwise(F.array(F.lit("EXACT"),F.lit("NORMALIZED"),F.lit("FUZZY"))))
        .withColumn("review_status",F.when(F.col("extraction_error").isNotNull(),"EXTRACTION_ERROR")
            .when(F.col("target_record_id").isNull(),"UNMATCHED").when(F.col("candidate_count")>1,"AMBIGUOUS")
            .when(F.col("match_tier")=="FUZZY","FUZZY_REVIEW").when(F.col("supporting_attributes_conflict"),"ATTRIBUTE_CONFLICT")
            .when(F.col("amount_match")=="MISMATCH","AMOUNT_MISMATCH").when(F.col("amount_match")=="UNKNOWN","CHECK_AMOUNT")
            .otherwise("PROPOSED"))
        .withColumn("rule_trace_json",F.to_json(F.struct("match_rules_version","match_policy_json","attempted_tiers",
            "match_tier","match_rule","block_rule","document_raw_keys","accounting_raw_keys",
            "document_normalized_keys","accounting_normalized_keys","identifier_similarity",
            "edit_distance","fuzzy_block_size","matching_note","supporting_attributes_conflict")))
        .withColumn("data_path_json",F.to_json(F.struct("storage_uri","document_id","document_row_id",
            "target_storage_table","target_record_id"))).drop("_block_count"))


def reverse_coverage(tx, review, single_currency=None):
    """One row per accounting transaction, regardless of its document count.

    Coverage means a current-snapshot candidate exists, not accepted evidence
    or a complete lifecycle. Amounts come exclusively from accounting.
    """
    if single_currency is not None and not re.fullmatch(r"[A-Z]{3}", single_currency):
        raise ValueError("Coverage single_currency must be an approved uppercase currency code or None.")
    ids = ["target_schema", "target_table", "target_record_id"]
    required(tx, ids + ["system_amount", "actual_record_json", "_currency"], "Coverage accounting")
    required(review, ids + ["source_row_key", "actual_record_json"], "Coverage review")
    # A changed accounting record must be rematched; old candidates cannot
    # imply coverage of the new record merely because its ID is unchanged.
    snapshot = ids + ["actual_record_json"]
    counts = (review.where(F.col("target_record_id").isNotNull()).groupBy(*snapshot)
              .agg(F.countDistinct("source_row_key").alias("candidate_document_rows")))
    currency = (F.when(F.col("_currency").rlike(r"^[A-Z]{3}$"), F.col("_currency"))
                .when(F.col("_currency").isNull(), F.lit(single_currency)))
    coverage = (tx.select(*ids, "system_amount", "actual_record_json", "_currency")
        .join(counts, snapshot, "left").fillna(0, ["candidate_document_rows"])
        .withColumn("transaction_currency", currency)
        .withColumn("currency_source", F.when(F.col("transaction_currency").isNull(), "UNKNOWN")
                    .when(F.col("_currency").isNull(), "APPROVED_POPULATION_CURRENCY").otherwise("ACCOUNTING"))
        .withColumn("has_candidate", F.col("candidate_document_rows") > 0)
        .withColumn("candidate_status", F.when(F.col("has_candidate"), "HAS_CANDIDATES").otherwise("NO_CANDIDATES"))
        .withColumn("amount_known", F.col("system_amount").isNotNull())
        .withColumn("transaction_amount_abs", F.abs("system_amount"))
        .withColumn("candidate_covered_amount", F.when(F.col("has_candidate"), F.col("system_amount")))
        .withColumn("candidate_uncovered_amount", F.when(~F.col("has_candidate"), F.col("system_amount")))
        .withColumn("candidate_covered_amount_abs", F.when(F.col("has_candidate"), F.abs("system_amount")))
        .withColumn("candidate_uncovered_amount_abs", F.when(~F.col("has_candidate"), F.abs("system_amount")))
        .withColumn("coverage_basis", F.lit("CANDIDATE_ONLY"))
        .withColumn("coverage_as_of", F.current_timestamp()).drop("_currency"))
    return coverage


def coverage_summary_sql(coverage_relation):
    """SQL shared by the durable view and local tests; relation must be trusted/quoted."""
    return f"""
    WITH totals AS (
      SELECT target_schema, target_table, transaction_currency, coverage_basis,
        MAX(coverage_as_of) AS coverage_as_of,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN has_candidate THEN 1 ELSE 0 END) AS candidate_covered_count,
        SUM(CASE WHEN NOT has_candidate THEN 1 ELSE 0 END) AS candidate_uncovered_count,
        SUM(CASE WHEN NOT amount_known THEN 1 ELSE 0 END) AS missing_amount_count,
        SUM(CASE WHEN has_candidate AND NOT amount_known THEN 1 ELSE 0 END) AS covered_missing_amount_count,
        SUM(CASE WHEN NOT has_candidate AND NOT amount_known THEN 1 ELSE 0 END) AS uncovered_missing_amount_count,
        SUM(CASE WHEN system_amount < 0 THEN 1 ELSE 0 END) AS negative_amount_count,
        SUM(CASE WHEN system_amount = 0 THEN 1 ELSE 0 END) AS zero_amount_count,
        SUM(system_amount) AS _net_total,
        SUM(COALESCE(candidate_covered_amount, 0)) AS _net_covered,
        SUM(COALESCE(candidate_uncovered_amount, 0)) AS _net_uncovered,
        SUM(transaction_amount_abs) AS _abs_total,
        SUM(COALESCE(candidate_covered_amount_abs, 0)) AS _abs_covered,
        SUM(COALESCE(candidate_uncovered_amount_abs, 0)) AS _abs_uncovered
      FROM {coverage_relation}
      GROUP BY target_schema, target_table, transaction_currency, coverage_basis
    )
    SELECT target_schema, target_table, transaction_currency, coverage_basis, coverage_as_of,
      transaction_count, candidate_covered_count, candidate_uncovered_count,
      missing_amount_count, covered_missing_amount_count, uncovered_missing_amount_count,
      negative_amount_count, zero_amount_count,
      ROUND(100.0 * candidate_covered_count / NULLIF(transaction_count, 0), 2) AS count_coverage_pct,
      CASE WHEN transaction_currency IS NOT NULL THEN _net_total END AS total_amount_net,
      CASE WHEN transaction_currency IS NOT NULL THEN _net_covered END AS candidate_covered_amount_net,
      CASE WHEN transaction_currency IS NOT NULL THEN _net_uncovered END AS candidate_uncovered_amount_net,
      CASE WHEN transaction_currency IS NOT NULL THEN _abs_total END AS total_amount_abs,
      CASE WHEN transaction_currency IS NOT NULL THEN _abs_covered END AS candidate_covered_amount_abs,
      CASE WHEN transaction_currency IS NOT NULL THEN _abs_uncovered END AS candidate_uncovered_amount_abs,
      CASE WHEN transaction_currency IS NOT NULL THEN
        ROUND(100.0 * _abs_covered / NULLIF(_abs_total, 0), 2) END AS dollar_coverage_pct,
      CASE WHEN transaction_currency IS NULL THEN 'UNKNOWN_CURRENCY'
           WHEN missing_amount_count > 0 THEN 'INCOMPLETE_AMOUNTS'
           WHEN _abs_total = 0 THEN 'ZERO_DOLLAR_POPULATION'
           ELSE 'COMPLETE_KNOWN_AMOUNTS' END AS dollar_coverage_quality
    FROM totals
    """


def save_coverage(spark, tx, review):
    (reverse_coverage(tx, review, COVERAGE_SINGLE_CURRENCY).write.format("delta")
     .mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name(REVERSE)))
    spark.sql(f"CREATE OR REPLACE VIEW {table_name(COVERAGE_SUMMARY)} AS " +
              coverage_summary_sql(table_name(REVERSE)))


def refresh_coverage(spark):
    """Refresh coverage from existing full preview; no file reads or AI calls.

    Current accounting records that differ from the candidate snapshot are
    uncovered until rematched. This never reads the five-document smoke table.
    """
    inv = accounting(spark.table(table_name(INVOICE_TABLE)), INVOICE_COLUMNS, "payables", "vendor_invoices")
    dis = accounting(spark.table(table_name(DISBURSEMENT_TABLE)), DISBURSEMENT_COLUMNS, "genledger", "disbursements")
    tx = inv.unionByName(dis)
    assert_unique(tx, ["target_schema", "target_table", "target_record_id"], "Accounting sources")
    save_coverage(spark, tx, spark.table(table_name(PREVIEW)))
    return spark.table(table_name(REVERSE)), spark.table(table_name(COVERAGE_SUMMARY))

# COMMAND ----------

def run_pipeline(spark, sample_per_type=5, retry_errors=False, batch_paths=200):
    if sample_per_type < 0 or batch_paths < 1:
        raise ValueError("Use a non-negative sample size and a positive file batch size.")
    review_table = SMOKE_PREVIEW if sample_per_type else PREVIEW
    # Validate mappings before paying for extraction.
    inv = accounting(spark.table(table_name(INVOICE_TABLE)), INVOICE_COLUMNS, "payables", "vendor_invoices")
    dis = accounting(spark.table(table_name(DISBURSEMENT_TABLE)), DISBURSEMENT_COLUMNS, "genledger", "disbursements")
    tx = inv.unionByName(dis)
    assert_unique(tx, ["target_schema", "target_table", "target_record_id"], "Accounting sources")
    docs = inventory(spark, VOLUME_ROOT, REPOSITORY, UPLOADS, sample_per_type)
    assert_unique(docs, ["document_id"], "Discovered documents")
    expected = docs.count()
    if expected == 0:
        raise ValueError("No supported document files found under VOLUME_ROOT.")
    data = add_content(spark, docs, batch_paths)
    if data.count() != expected:
        raise ValueError("File paths changed or could not be joined back to the inventory. Check paths and retry.")
    extracted = extract_once(spark, data, CACHE, retry_errors)
    if extracted.count() != expected:
        raise ValueError("Some files were not extracted or changed mid-run. Retry with a stable input folder.")
    rows = flatten(extracted)
    review = match_rows(rows, tx)
    if WORKSPACE_HOST:
        host = WORKSPACE_HOST.rstrip("/")
        if not re.fullmatch(r"https://[A-Za-z0-9.-]+", host):
            raise ValueError("WORKSPACE_HOST must be an HTTPS workspace origin without a path.")
        encoded = F.concat_ws("/", F.transform(F.split("storage_uri", "/"),
                                               lambda x: F.regexp_replace(F.url_encode(x), r"\+", "%20")))
        review = review.withColumn("document_url", F.concat(F.lit(host + "/ajax-api/2.0/fs/files"), encoded))
    else:
        review = review.withColumn("document_url", F.lit(None).cast("string"))
    # Fully materialize the candidate snapshot. The app's live source is never overwritten.
    review.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name(review_table))
    saved = spark.table(table_name(review_table))
    if not sample_per_type:
        save_coverage(spark, tx, saved)
    return saved, review_table

# COMMAND ----------

# Import as a Databricks source notebook. Start with five files per document type.
# Set sample_per_type=0 only after verifying the smoke output and AI usage.
if "dbutils" in globals() and "spark" in globals():
    dbutils.widgets.text("sample_per_type", "0")
    dbutils.widgets.dropdown("retry_extraction_errors", "false", ["false", "true"])
    review, output = run_pipeline(spark, int(dbutils.widgets.get("sample_per_type")),
                                 dbutils.widgets.get("retry_extraction_errors") == "true")
    print(f"Saved {output}")
    display(review.groupBy("document_type", "review_status").count().orderBy("document_type", "review_status"))
    if int(dbutils.widgets.get("sample_per_type")) == 0:
        display(spark.table(table_name(COVERAGE_SUMMARY)).orderBy("target_table", "transaction_currency"))
