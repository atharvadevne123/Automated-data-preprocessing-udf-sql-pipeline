# Architecture

## Overview

This project implements an end-to-end data analytics pipeline for the Yelp Open Dataset. It combines file splitting, validation, sentiment analysis, and statistical aggregation into a modular, testable Python package.

## Component Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     Data Sources                        │
│   yelp_academic_dataset_review.json (9 GB JSONL)       │
└─────────────────────┬───────────────────────────────────┘
                      │
            ┌─────────▼──────────┐
            │   split_files.py   │  CLI: split large JSONL
            │  split_file()      │  into N smaller chunks
            │  split_file_stream()│
            └─────────┬──────────┘
                      │  N × chunk_i.jsonl
         ┌────────────▼────────────────────────┐
         │           pipeline/                  │
         │  ┌────────────────────────────────┐ │
         │  │  processor.py (RecordProcessor)│ │  Filter + sample
         │  └──────────────┬─────────────────┘ │
         │  ┌──────────────▼─────────────────┐ │
         │  │  cleaner.py  (TextCleaner)     │ │  Normalise text
         │  └──────────────┬─────────────────┘ │
         │  ┌──────────────▼─────────────────┐ │
         │  │  sentiment.py (SentimentAnalyzer│ │  TextBlob polarity
         │  └──────────────┬─────────────────┘ │
         │  ┌──────────────▼─────────────────┐ │
         │  │  aggregator.py (StatsAggregator│ │  Per-business stats
         │  └──────────────┬─────────────────┘ │
         │  ┌──────────────▼─────────────────┐ │
         │  │  exporter.py (DataExporter)    │ │  JSONL / CSV / JSON
         │  └────────────────────────────────┘ │
         └────────────────────────────────────┘
                      │
         ┌────────────▼─────────────────────┐
         │         Snowflake DWH            │
         │  snowflake_connector.py          │
         │  • get_connection()              │
         │  • execute_query()               │
         │  • execute_many()  (bulk insert) │
         │  • managed_connection() ctx mgr  │
         └───────────────────────────────────┘
```

## Module Reference

| Module | Responsibility |
|--------|---------------|
| `split_files.py` | Split large JSONL into N equal chunks |
| `snowflake_connector.py` | Snowflake connection and query helpers |
| `pipeline/processor.py` | Filter, transform, and sample records |
| `pipeline/cleaner.py` | Normalise review text (HTML, URLs, whitespace) |
| `pipeline/sentiment.py` | TextBlob sentiment scoring |
| `pipeline/aggregator.py` | Per-business and global statistics |
| `pipeline/exporter.py` | Write records to JSONL / JSON / CSV |
| `models/yelp.py` | Pydantic models for Yelp record types |
| `utils/validators.py` | Input validation helpers |
| `utils/metrics.py` | SplitMetrics, ValidationMetrics, PipelineRunMetrics |
| `utils/logger.py` | Structured logging (text + JSON) |
| `utils/retry.py` | Exponential-backoff retry decorator |
| `utils/cache.py` | LRU cache wrapper with hit/miss stats |
| `scripts/validate_jsonl.py` | CLI: validate JSONL files |
| `scripts/analyze_sentiment.py` | CLI: batch sentiment enrichment |
| `scripts/generate_report.py` | CLI: JSON summary report |
| `scripts/benchmark.py` | Performance benchmark tool |

## Data Flow

```
Input JSONL
    → split_files (create chunks)
    → RecordProcessor (filter / sample)
    → TextCleaner (normalise text)
    → SentimentAnalyzer (polarity / label)
    → StatsAggregator (accumulate stats)
    → DataExporter (write JSONL / CSV / JSON)
    → snowflake_connector (bulk INSERT via execute_many)
```

## Design Principles

- **Single responsibility**: each module does one thing.
- **Testability**: all modules accept dependency-injected callables; no hidden global state.
- **Graceful degradation**: optional libraries (textblob, pydantic, snowflake-connector) are guarded with `try/except ImportError`.
- **Type safety**: all public functions carry type annotations compatible with Python 3.9+.
