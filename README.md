# Load Doubleclick impressions data into google big query using cloud dataflow

How to use
```terminal
python -m load_with_dataflow.py --project dbck --runner DataflowRunner --num_workers 10  --staging_location gs://data/staging --temp_location gs://data/temp --output dwh_prod.raw_impressions_daily --input gs://data/input/*.csv
```

This module does 2 things

a) Load Raw data into given big query table

b) Generate daily aggregates for each dimension


Schema of dimension table

-   event_date:DATE
-   dimension_type:STRING
-   dimension:STRING
-   aggr_value:INTEGER










