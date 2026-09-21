from __future__ import annotations

from functools import reduce
from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


WEEKDAYS = {
    1: "domingo",
    2: "lunes",
    3: "martes",
    4: "miercoles",
    5: "jueves",
    6: "viernes",
    7: "sabado",
}


def clean_and_enrich(df: DataFrame) -> DataFrame:
    result = (
        df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumn(
            "trip_duration_s",
            F.col("tpep_dropoff_datetime").cast("long")
            - F.col("tpep_pickup_datetime").cast("long"),
        )
        .filter(F.col("trip_duration_s").between(60, 86_400))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("total_amount") >= 0)
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("dropoff_hour", F.hour("tpep_dropoff_datetime"))
        .withColumn("day_number", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("speed", F.col("trip_distance") / (F.col("trip_duration_s") / 3600.0))
        .withColumn(
            "total_amount_per_mile",
            F.col("total_amount") / F.col("trip_distance"),
        )
        .withColumn(
            "tip_percentage",
            F.when(F.col("fare_amount") > 0, 100 * F.col("tip_amount") / F.col("fare_amount")),
        )
    )
    day_expr = F.create_map(*[F.lit(v) for item in WEEKDAYS.items() for v in item])
    return result.withColumn("day_of_week", day_expr[F.col("day_number")]).drop("day_number")


def boxplot_stats(df: DataFrame, column: str) -> DataFrame:
    days = list(WEEKDAYS.values())
    aggregates = []
    for day in days:
        values = F.when(F.col("day_of_week") == day, F.col(column))
        aggregates.extend(
            [
                F.percentile_approx(values, 0.25).alias(f"{day}_q1"),
                F.percentile_approx(values, 0.50).alias(f"{day}_median"),
                F.percentile_approx(values, 0.75).alias(f"{day}_q3"),
                F.min(values).alias(f"{day}_min"),
                F.max(values).alias(f"{day}_max"),
            ]
        )
    return df.groupBy("pickup_hour").agg(*aggregates)


def weekday_hour_summary(df: DataFrame, median_columns: Iterable[str]) -> DataFrame:
    median_columns = list(median_columns)
    grouped = df.groupBy("day_of_week", "pickup_hour").agg(
        F.count(F.lit(1)).alias("n_trips"),
        *[F.expr(f"percentile_approx({column}, 0.5)").alias(column) for column in median_columns],
    )
    measures = ["n_trips", *median_columns]
    pivoted = []
    for measure in measures:
        item = grouped.groupBy("day_of_week").pivot("pickup_hour", range(24)).agg(F.first(measure))
        item = item.select(
            "day_of_week",
            *[F.col(str(hour)).alias(f"{measure}_{hour}") for hour in range(24)],
        )
        pivoted.append(item)
    return reduce(lambda left, right: left.join(right, "day_of_week", "outer"), pivoted)


def attach_districts(trips: DataFrame, districts: DataFrame) -> DataFrame:
    points = trips.withColumn(
        "pickup_point", F.expr("ST_Point(pickup_longitude, pickup_latitude)")
    ).withColumn(
        "dropoff_point", F.expr("ST_Point(dropoff_longitude, dropoff_latitude)")
    )
    pickup = districts.select(
        F.col("district").alias("pickup_district"), "district_geom"
    )
    dropoff = districts.select(
        F.col("district").alias("dropoff_district"),
        F.col("district_geom").alias("dropoff_geom"),
    )
    joined = points.join(
        F.broadcast(pickup),
        F.expr("ST_Contains(district_geom, pickup_point)"),
        "left",
    ).drop("district_geom", "pickup_point")
    return joined.join(
        F.broadcast(dropoff),
        F.expr("ST_Contains(dropoff_geom, dropoff_point)"),
        "left",
    ).drop("dropoff_geom", "dropoff_point")

