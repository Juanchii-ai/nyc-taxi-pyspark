# NYC Taxi Analytics with PySpark

Distributed analytics project for New York City taxi trips, designed for Spark and Databricks.

## What it demonstrates

- Schema enforcement and data-quality filters.
- Feature engineering for duration, speed, hourly patterns and weekday behaviour.
- Reusable grouped statistics for boxplots and heatmaps.
- Geospatial enrichment with GeoPandas and Apache Sedona.
- Broadcast spatial joins to attach pickup and drop-off community districts.

## Repository structure

```text
├── src/nyc_taxi_analysis.py
├── src/geo_helpers.py
├── data/README.md
├── requirements.txt
└── README.md
```

## Notes

The portfolio edition removes private Azure paths and credentials. Input paths are supplied at runtime so the same transformations can run in Databricks or another Spark environment.

## Technologies

PySpark · Spark SQL · Databricks · GeoPandas · Folium · Apache Sedona

