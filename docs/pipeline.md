# Pipeline Module Reference

The `pipeline/` package provides composable stages for processing Yelp JSONL records.

## RecordProcessor

```python
from pipeline.processor import RecordProcessor

proc = RecordProcessor(
    filters=[lambda r: r["stars"] >= 4],
    transforms=[lambda r: {**r, "source": "yelp"}],
    sample_rate=0.1,
    seed=42,
)
for record in proc.process_file(Path("reviews.jsonl")):
    print(record)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filters` | `list[Callable]` | `[]` | All must return True to keep record |
| `transforms` | `list[Callable]` | `[]` | Applied in sequence to each record |
| `sample_rate` | `float` | `1.0` | Fraction of passing records to keep |
| `seed` | `int \| None` | `None` | Random seed for sampling |

## TextCleaner

```python
from pipeline.cleaner import TextCleaner

cleaner = TextCleaner(lowercase=True, strip_urls=True, strip_html=True, max_length=500)
clean_text = cleaner.clean("<b>Great</b> food! http://yelp.com")
# → "great food!"
```

## SentimentAnalyzer

```python
from pipeline.sentiment import SentimentAnalyzer

analyzer = SentimentAnalyzer(positive_threshold=0.1, negative_threshold=-0.1)
result = analyzer.analyze("This was absolutely fantastic!")
print(result.label)      # "positive"
print(result.polarity)   # e.g. 0.75
```

Uses TextBlob under the hood. Falls back to `neutral` if TextBlob is not installed.

## StatsAggregator

```python
from pipeline.aggregator import StatsAggregator

agg = StatsAggregator()
for record in records:
    agg.add(record)

print(agg.global_stats().to_dict())
print(agg.top_businesses(n=5))
```

## DataExporter

```python
from pipeline.exporter import DataExporter

exporter = DataExporter()
exporter.to_jsonl(records, Path("output/enriched.jsonl"))
exporter.to_csv(records, Path("output/enriched.csv"), fields=["business_id", "stars"])
exporter.export(records, Path("output/data.json"), fmt="json")
```

Supported formats: `jsonl`, `json`, `csv`.

## RecordDeduplicator

```python
from pipeline.deduplicator import RecordDeduplicator

dedup = RecordDeduplicator(key_fields=["review_id"], track_stats=True)
unique = list(dedup.deduplicate(records))
print(dedup.stats.duplicates_dropped, dedup.stats.unique_count)
```

Hashes the concatenated values of `key_fields` with SHA-256 to detect duplicates in a single pass.

## ReservoirSampler / StratifiedSampler

```python
from pipeline.sampler import ReservoirSampler, StratifiedSampler

rs = ReservoirSampler(size=1000, seed=42)
rs.add_batch(records)
sample = rs.get_sample()

ss = StratifiedSampler(key_field="stars", sample_rate=0.1)
stratified = ss.sample(records)
```

## RecordPartitioner / FilePartitioner

```python
from pipeline.partitioner import FilePartitioner
from pathlib import Path

fp = FilePartitioner(
    key_func=lambda r: str(int(r["stars"])),
    output_dir=Path("output/partitions"),
    prefix="stars",
)
fp.add_batch(records)
fp.flush()  # writes stars_1.jsonl … stars_5.jsonl
```

## FieldNormalizer

```python
from pipeline.normalizer import FieldNormalizer

norm = FieldNormalizer()
norm.register("stars", FieldNormalizer.normalise_stars)
norm.register("text", FieldNormalizer.lowercase_strip)
normalised = norm.normalise_batch(records)
```
