# Distributor Reporting Pipeline — Reference Implementation

Redacted examples from a production pipeline that ingests label-wide distributor reporting
into a reconciled, accounting-ready dataset.

**Architecture**: Bronze (raw ingest) → Silver (accounting logic) → Gold (unified facts + reporting)

| File | Description |
|------|-------------|
| `ingest_example.py` | Python ingest script — encoding detection, SHA-256 row keys, full-refresh load |
| `silver_view_example.sql` | dbt Silver view — multi-pass SKU resolution, GL routing, sign conventions |
| `pipeline_lineage.mermaid` | Data lineage diagram — 6 sources through Bronze/Silver/Gold to GL push |

All identifying details (database names, server addresses, label-specific identifiers) have been redacted.
The structural patterns are production code.
