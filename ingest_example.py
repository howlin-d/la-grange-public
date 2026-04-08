"""
distributor_digital_ingest.py  [REDACTED]
────────────────────────────────────────
Loads Bronze.distributor_digital from monthly CSV royalty statements.

Mixed encodings per file (UTF-16, UTF-8 BOM, Latin-1). Auto-detected.
Digital streaming and download royalties — one row per line item per
invoice period.

row_key = SHA-256 hash of composite business key
          (invoice | catalog | upc | product_id | country | txn_type | serial).

Pattern: detect encoding → parse to DataFrame → rename columns →
         normalize nulls → generate deterministic row_key →
         type coercions → TRUNCATE + reload.
"""
import sys, os, glob, hashlib
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text

# ── CONFIG ────────────────────────────────────────────────────────────────
TARGET_TABLE  = "distributor_digital"
TARGET_SCHEMA = "Bronze"
SOURCE_DIR    = "/path/to/Royalty Sources/Distributor/Digital"   # [REDACTED]

# Map raw CSV headers (lowercased) → standardized column names.
# Distributor CSVs use spaces in headers; we normalize to underscores.
COLUMN_MAP = {
    "internal invoice number":                      "Internal_Invoice_Number",
    "internal serial number":                       "Internal_Serial_Number",
    "invoice_description":                          "Invoice_Description",
    "catalog number":                               "Catalog_Number",
    "upc":                                          "UPC",
    "product id":                                   "Product_ID",
    "artist":                                       "Artist",
    "product title":                                "Product_Title",
    "title_description":                            "Title_Description",
    "format":                                       "Format",
    "customer":                                     "Customer",
    "digital service provider_subdescription":      "DSP_Subdescription",
    "country code":                                 "Country_Code",
    "transaction type":                             "Transaction_Type",
    "quantity":                                     "Quantity",
    "gross unit price":                             "Gross_Unit_Price",
    "total gross income":                           "Total_Gross_Income",
    "distribution fee total":                       "Distribution_Fee_Total",
    "label net unit price":                         "Label_Net_Unit_Price",
    "label net total":                              "Label_Net_Total",
    "datepaidlast":                                 "datePaidlast",
}


# ── Encoding Detection ───────────────────────────────────────────────────

def _detect_encoding(path: str) -> str:
    """Sniff BOM to choose encoding: UTF-16, UTF-8-sig, or Latin-1 fallback."""
    with open(path, "rb") as f:
        raw = f.read(4)
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    if raw[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    return "latin-1"


def _detect_sep(path: str, enc: str) -> str:
    """Sniff first line to determine delimiter (tab vs comma)."""
    with open(path, encoding=enc) as f:
        first_line = f.readline()
    return "\t" if "\t" in first_line else ","


# ── Main ─────────────────────────────────────────────────────────────────

def run():
    csv_files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {SOURCE_DIR}")

    print(f"[{datetime.now():%H:%M:%S}] Found {len(csv_files)} file(s).")

    frames = []
    for path in csv_files:
        enc = _detect_encoding(path)
        sep = _detect_sep(path, enc)
        print(f"  → {os.path.basename(path)}  [{enc}, {'tab' if sep == chr(9) else 'comma'}]")
        df = pd.read_csv(path, encoding=enc, sep=sep, dtype=str)
        df.columns = [col.lstrip("\ufeff").strip() for col in df.columns]
        df["_source_file"] = os.path.basename(path)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    print(f"[{datetime.now():%H:%M:%S}] Raw rows loaded: {len(combined):,}")

    # ── Rename columns ───────────────────────────────────────────────────
    combined.columns = [col.lower() for col in combined.columns]
    combined = combined.rename(columns={k: v for k, v in COLUMN_MAP.items()
                                        if k in combined.columns})
    combined["Source_File"] = combined.pop("_source_file")

    # ── Normalize nulls ──────────────────────────────────────────────────
    combined = combined.replace(["", "None", "nan", "NaT", "NaN"], np.nan)

    # ── Generate deterministic row_key (vectorized SHA-256) ──────────────
    combined["Load_Timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None)

    composite = (
        combined["Internal_Invoice_Number"].fillna("").astype(str) + "|"
        + combined["Catalog_Number"].fillna("").astype(str)         + "|"
        + combined["UPC"].fillna("").astype(str)                    + "|"
        + combined["Product_ID"].fillna("").astype(str)             + "|"
        + combined["Country_Code"].fillna("").astype(str)           + "|"
        + combined["Transaction_Type"].fillna("").astype(str)       + "|"
        + combined["Internal_Serial_Number"].fillna("").astype(str)
    )
    combined["row_key"] = composite.apply(
        lambda s: hashlib.sha256(s.encode()).hexdigest()[:85]
    )
    combined = combined.drop_duplicates(subset=["row_key"], keep="last")

    # ── Type coercions ───────────────────────────────────────────────────
    if "Quantity" in combined.columns:
        combined["Quantity"] = pd.to_numeric(combined["Quantity"], errors="coerce") \
                                  .round(0).astype("Int64")

    decimal_cols = [
        "Gross_Unit_Price", "Total_Gross_Income", "Distribution_Fee_Total",
        "Label_Net_Unit_Price", "Label_Net_Total",
    ]
    for col in decimal_cols:
        if col in combined.columns:
            combined[col] = (
                combined[col].astype(str).str.strip()
                .str.replace(",", "", regex=False)
                .replace(r"^-\s*$", "0", regex=True)
            )
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # ── Column ordering ──────────────────────────────────────────────────
    keep_cols = ["Source_File", "Load_Timestamp", "row_key"] + list(COLUMN_MAP.values())
    combined = combined[[c for c in keep_cols if c in combined.columns]]

    # ── TRUNCATE + reload ────────────────────────────────────────────────
    print(f"[{datetime.now():%H:%M:%S}] Upserting {len(combined):,} rows → "
          f"{TARGET_SCHEMA}.{TARGET_TABLE} ...")

    engine = get_engine()   # [REDACTED] — connection helper

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{TARGET_SCHEMA}].[{TARGET_TABLE}]"))

    combined.to_sql(
        TARGET_TABLE, engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=2000,
    )

    print(f"[{datetime.now():%H:%M:%S}] Done — {len(combined):,} rows loaded.")


if __name__ == "__main__":
    run()
