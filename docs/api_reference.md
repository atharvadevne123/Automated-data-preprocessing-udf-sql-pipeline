# API Reference

Quick reference for modules added in v1.4.0.

## pipeline.deduplicator

```python
from pipeline.deduplicator import RecordDeduplicator, DeduplicationStats

dedup = RecordDeduplicator(key_fields=["review_id"])
unique = dedup.deduplicate(records)
print(dedup.stats.duplicates_dropped)
```

| Class / Function | Description |
|-----------------|-------------|
| `RecordDeduplicator(key_fields, track_stats)` | Remove duplicate records by hashing selected fields |
| `DeduplicationStats` | Dataclass: `total_seen`, `duplicates_dropped`, `unique_count` |

## pipeline.sampler

```python
from pipeline.sampler import ReservoirSampler, StratifiedSampler

rs = ReservoirSampler(size=100, seed=42)
rs.add_batch(records)
sample = rs.get_sample()

ss = StratifiedSampler(key_field="stars", sample_rate=0.1, seed=0)
sampled = ss.sample(records)
```

| Class | Description |
|-------|-------------|
| `ReservoirSampler(size, seed)` | Vitter's Algorithm R reservoir sampling |
| `StratifiedSampler(key_field, sample_rate, seed)` | Proportional stratified sampling by field |

## pipeline.partitioner

```python
from pipeline.partitioner import RecordPartitioner, FilePartitioner

fp = FilePartitioner(
    key_func=lambda r: str(r["stars"]),
    output_dir=Path("output/partitions"),
)
fp.add_batch(records)
fp.flush()
```

## pipeline.normalizer

```python
from pipeline.normalizer import FieldNormalizer

norm = FieldNormalizer()
norm.register("stars", FieldNormalizer.normalise_stars)
normalised = norm.normalise_batch(records)
```

## utils.hash_utils

```python
from utils.hash_utils import sha256_hex, record_fingerprint, short_id

fp = record_fingerprint(record, key_fields=["review_id", "business_id"])
sid = short_id(record, length=8)
```

## utils.text_stats

```python
from utils.text_stats import compute_text_stats, lexical_diversity, top_n_words

stats = compute_text_stats("Great place! Really enjoyed the food.")
print(stats.word_count, stats.avg_word_length)
```

## utils.schema_validator

```python
from utils.schema_validator import RecordSchemaValidator, FieldSpec

validator = RecordSchemaValidator([
    FieldSpec("stars", "float", required=True, min_value=1.0, max_value=5.0),
    FieldSpec("text", "str", required=True, min_length=1),
])
valid = validator.filter_valid(records)
```

## utils.file_utils

```python
from utils.file_utils import iter_jsonl, write_jsonl, list_jsonl_files

for record in iter_jsonl(Path("data.jsonl")):
    process(record)
```

## utils.profiler.AsyncTimer

```python
from utils.profiler import AsyncTimer

async with AsyncTimer("fetch_phase") as t:
    await fetch_data()
print(t.elapsed_sec, t.to_dict())
```

## pipeline.exporter.DataExporter.to_parquet_compatible

```python
from pipeline.exporter import DataExporter

exporter = DataExporter()
flat = exporter.to_parquet_compatible(records)
# nested dicts/lists are JSON-serialised to strings
```

## snowflake_connector (stage helpers)

```python
from snowflake_connector import copy_into_stage, list_stage_files

copy_into_stage(conn, "@my_stage", "/tmp/data.json")
files = list_stage_files(conn, "@my_stage")
```
