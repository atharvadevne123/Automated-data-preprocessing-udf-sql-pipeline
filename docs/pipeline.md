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
