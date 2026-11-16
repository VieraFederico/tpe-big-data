# Bronze streaming

Step `bronze_stream` ([`ingest.py`](ingest.py)). Lee `landing/usage_events_stream/*.jsonl` con Structured Streaming y trigger `availableNow`: procesa los archivos nuevos en micro-lotes de hasta 10 archivos y termina. Parsea el timestamp, deduplica por `event_id` (o por un hash del record si es null) con un watermark de 61 días y, en cada micro-lote, reparte entre `bronze/usage_events` (por `event_date`) y `quarantine/bronze/usage_events`.

El checkpoint está en `checkpoints/bronze_usage_events/`: volver a correr no relee lo ya procesado. Por qué 61 días: [`DECISIONS.md`](../../../DECISIONS.md), D-11.
