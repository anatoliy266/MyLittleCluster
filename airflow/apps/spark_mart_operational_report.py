
import argparse
import math
import sys
import traceback
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("orchestrator")

SCALAR_PARAMS = [
    "temperature_2m",
    "cloud_cover",
    "rain",
    "showers",
    "precipitation",
    "precipitation_probability",
    "apparent_temperature",
    "wind_speed_10m",
    "wind_gusts_10m",
    "soil_temperature_0cm",
    "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm",
    "soil_moisture_3_to_9cm",
    "relative_humidity_2m",
    "wind_speed_80m",
    "dew_point_2m",
    "vapour_pressure_deficit",
    "evapotranspiration",
    "et0_fao_evapotranspiration",
    "cape",
    "boundary_layer_height",
]

REQUIRED_FIRMS_COLUMNS = [
    "latitude",
    "longitude",
    "acq_date",
    "acq_time",
]

REQUIRED_WEATHER_COLUMNS = (
    SCALAR_PARAMS
    + [
        "time",
        "latitude",
        "longitude",
        "wind_direction_10m",
        "wind_direction_80m",
        "weather_code",
    ]
)


def parse_and_validate_config():
    parser = argparse.ArgumentParser(
        description="Triangulate Open-Meteo weather to NASA FIRMS hotspots"
    )

    parser.add_argument("--firms_path", required=True, type=str)
    parser.add_argument("--weather_path", required=True, type=str)
    parser.add_argument("--mart_path", required=True, type=str)

    parser.add_argument("--run_id", required=True, type=str)
    parser.add_argument("--execution_date", required=True, type=str)

    parser.add_argument(
        "--search_radius_km",
        default=50.0,
        type=float,
    )

    parser.add_argument(
        "--fallback_hours",
        default=12,
        type=int,
    )

    parser.add_argument(
        "--epsilon",
        default=1e-6,
        type=float,
    )

    args = parser.parse_args()

    try:
        datetime.strptime(args.execution_date, "%Y-%m-%d")
    except ValueError:
        parser.error("--execution_date must have format YYYY-MM-DD")

    if args.search_radius_km <= 0:
        parser.error("--search_radius_km must be greater than zero")

    if args.fallback_hours <= 0:
        parser.error("--fallback_hours must be greater than zero")

    if args.epsilon <= 0:
        parser.error("--epsilon must be greater than zero")

    return args


def create_spark_session(args):
    return (
        SparkSession.builder
        .appName(
            f"mart_fire_weather_triangulated_{args.execution_date}"
        )
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .getOrCreate()
    )


def validate_columns(df, required_columns, source_name):
    missing = sorted(set(required_columns) - set(df.columns))

    if missing:
        raise ValueError(
            f"{source_name}: missing required columns: {missing}"
        )


def prepare_firms(df_firms, fallback_hours):
    """
    Подготовка спутниковых наблюдений.

    Каждая исходная строка остается самостоятельным наблюдением.

    Временная последовательность строится по географической ячейке
    с точностью 0.01 градуса.

    Одинаковые координаты и время получают общий следующий timestamp,
    но остаются отдельными наблюдениями.
    """

    df = (
        df_firms
        .withColumn(
            "acq_time_str",
            F.lpad(
                F.col("acq_time").cast("int").cast("string"),
                4,
                "0",
            ),
        )
        .withColumn(
            "f_start_ts",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("acq_date").cast("string"),
                    F.col("acq_time_str"),
                ),
                "yyyy-MM-dd HHmm",
            ),
        )
        .withColumn("f_lat", F.col("latitude").cast("double"))
        .withColumn("f_lon", F.col("longitude").cast("double"))
        .filter(
            F.col("f_start_ts").isNotNull()
            & F.col("f_lat").isNotNull()
            & F.col("f_lon").isNotNull()
        )
        .withColumn(
            "cell_lat",
            F.round(F.col("f_lat"), 2),
        )
        .withColumn(
            "cell_lon",
            F.round(F.col("f_lon"), 2),
        )
        .withColumn(
            "fire_id",
            F.monotonically_increasing_id(),
        )
    )

    # Строим временную последовательность отдельно от исходных строк.
    # Это исключает нулевые интервалы между несколькими наблюдениями
    # с одинаковыми координатами и временем.

    timeline = (
        df
        .select(
            "cell_lat",
            "cell_lon",
            "f_start_ts",
        )
        .distinct()
    )

    w_next = (
        Window
        .partitionBy("cell_lat", "cell_lon")
        .orderBy(F.col("f_start_ts").asc())
    )

    timeline = (
        timeline
        .withColumn(
            "f_next_ts_raw",
            F.lead("f_start_ts").over(w_next),
        )
        .withColumn(
            "f_next_ts",
            F.coalesce(
                F.col("f_next_ts_raw"),
                F.col("f_start_ts")
                + F.expr(f"INTERVAL {fallback_hours} HOURS"),
            ),
        )
        .drop("f_next_ts_raw")
    )

    df = (
        df
        .join(
            timeline,
            on=["cell_lat", "cell_lon", "f_start_ts"],
            how="inner",
        )
    )

    return df


def prepare_weather(df_weather):
    """
    Подготовка почасовых погодных наблюдений.
    Никакого усреднения по времени не выполняется.
    """

    return (
        df_weather
        .withColumn(
            "w_ts",
            F.col("time").cast("timestamp"),
        )
        .withColumn(
            "w_lat",
            F.col("latitude").cast("double"),
        )
        .withColumn(
            "w_lon",
            F.col("longitude").cast("double"),
        )
        .filter(
            F.col("w_ts").isNotNull()
            & F.col("w_lat").isNotNull()
            & F.col("w_lon").isNotNull()
        )
    )


def calculate_idw_aggregations():
    """
    Формирует выражения IDW.

    Знаменатель для каждого параметра учитывает только
    погодные точки с непустым значением этого параметра.
    """

    agg_exprs = []

    for col_name in SCALAR_PARAMS:

        value = F.col(f"w.{col_name}")

        valid_weight = F.when(
            value.isNotNull(),
            F.col("weight"),
        )

        numerator = F.sum(
            value * F.col("weight")
        )

        denominator = F.sum(valid_weight)

        agg_exprs.append(
            F.try_divide(
                numerator,
                denominator,
            ).alias(col_name)
        )

    # Векторные компоненты направления ветра.
    for direction in ("10m", "80m"):

        u_col = f"u{direction}"
        v_col = f"v{direction}"

        valid_weight = F.when(
            F.col(u_col).isNotNull()
            & F.col(v_col).isNotNull(),
            F.col("weight"),
        )

        denominator = F.sum(valid_weight)

        agg_exprs.extend([
            F.try_divide(
                F.sum(F.col(u_col) * F.col("weight")),
                denominator,
            ).alias(f"{u_col}_interp"),

            F.try_divide(
                F.sum(F.col(v_col) * F.col("weight")),
                denominator,
            ).alias(f"{v_col}_interp"),
        ])

    agg_exprs.extend([
        F.min_by(
            F.col("w.weather_code"),
            F.col("dist_km"),
        ).alias("weather_code"),

        F.count("*").alias("triangulation_points_used"),

        F.min("dist_km").alias("nearest_weather_pt_km"),

        F.max("dist_km").alias("farthest_weather_pt_km"),
    ])

    return agg_exprs


def build_triangulated_mart(
    df_firms,
    df_openmeteo,
    search_radius,
    fallback_hours,
    epsilon,
):
    """
    Формирует почасовую погодную историю для каждого
    спутникового наблюдения FIRMS.

    Для каждого часа:
      1. Находим погодные точки в пространственном радиусе.
      2. Рассчитываем расстояние Haversine.
      3. Выбираем три ближайшие точки.
      4. Выполняем IDW-интерполяцию.
    """

    df_firms_prep = prepare_firms(
        df_firms,
        fallback_hours,
    )

    df_weather_prep = prepare_weather(
        df_openmeteo,
    )

    # Предварительное ограничение пространственного поиска.
    lat_margin = (search_radius / 111.0) + 0.02

    cos_lat = F.greatest(
        F.abs(F.cos(F.radians(F.col("f.f_lat")))),
        F.lit(0.01),
    )

    lon_margin = (
        F.lit(search_radius)
        / (F.lit(111.0) * cos_lat)
        + F.lit(0.02)
    )

    candidates = (
        df_firms_prep.alias("f")
        .join(
            df_weather_prep.alias("w"),
            on=(
                (F.col("w.w_ts") >= F.col("f.f_start_ts"))
                & (F.col("w.w_ts") < F.col("f.f_next_ts"))
                & (
                    F.abs(
                        F.col("w.w_lat") - F.col("f.f_lat")
                    ) <= lat_margin
                )
                & (
                    F.abs(
                        F.col("w.w_lon") - F.col("f.f_lon")
                    ) <= lon_margin
                )
            ),
            how="inner",
        )
    )

    # Расстояние Haversine.

    candidates = (
        candidates
        .withColumn(
            "dlat",
            F.radians(
                F.col("w.w_lat") - F.col("f.f_lat")
            ),
        )
        .withColumn(
            "dlon",
            F.radians(
                F.col("w.w_lon") - F.col("f.f_lon")
            ),
        )
        .withColumn(
            "a",
            F.pow(
                F.sin(F.col("dlat") / 2.0),
                2,
            )
            + F.cos(F.radians(F.col("f.f_lat")))
            * F.cos(F.radians(F.col("w.w_lat")))
            * F.pow(
                F.sin(F.col("dlon") / 2.0),
                2,
            ),
        )
        .withColumn(
            "dist_km",
            F.lit(2.0 * 6371.0088)
            * F.asin(
                F.sqrt(
                    F.least(
                        F.lit(1.0),
                        F.col("a"),
                    )
                )
            ),
        )
        .filter(
            F.col("dist_km") <= search_radius
        )
    )

    # Три ближайшие погодные точки для каждого FIRMS-наблюдения
    # и каждого часа.

    w_3_nearest = (
        Window
        .partitionBy(
            F.col("f.fire_id"),
            F.col("w.w_ts"),
        )
        .orderBy(
            F.col("dist_km").asc(),
        )
    )

    triangle_points = (
        candidates
        .withColumn(
            "pt_rank",
            F.row_number().over(w_3_nearest),
        )
        .filter(
            F.col("pt_rank") <= 3
        )
        .withColumn(
            "weight",
            F.lit(1.0)
            / (
                F.pow(F.col("dist_km"), 2)
                + F.lit(epsilon)
            ),
        )
    )

    # Направление ветра переводим в векторные компоненты.
    # NULL не участвуют ни в числителе, ни в знаменателе.

    for height in ("10m", "80m"):

        direction_col = F.col(
            f"w.wind_direction_{height}"
        )

        triangle_points = (
            triangle_points
            .withColumn(
                f"u{height}",
                F.when(
                    direction_col.isNotNull(),
                    F.sin(F.radians(direction_col)),
                ),
            )
            .withColumn(
                f"v{height}",
                F.when(
                    direction_col.isNotNull(),
                    F.cos(F.radians(direction_col)),
                ),
            )
        )

    agg_exprs = calculate_idw_aggregations()

    # Группировка по исходному спутниковому наблюдению
    # и конкретному часу погодных данных.

    firms_cols = [
        "fire_id",
        "f_start_ts",
        "f_next_ts",
    ] + [
        c for c in df_firms_prep.columns
        if c not in {
            "fire_id",
            "f_start_ts",
            "f_next_ts",
            "f_next_ts_raw",
            "f_lat",
            "f_lon",
            "cell_lat",
            "cell_lon",
            "acq_time_str",
        }
    ]

    df_triangulated = (
        triangle_points
        .groupBy(
            *[
                F.col(f"f.{c}")
                for c in firms_cols
            ],
            F.col("w.w_ts").alias("weather_ts"),
        )
        .agg(*agg_exprs)
    )

    # Восстанавливаем направление ветра из интерполированных
    # векторных компонент.

    for height in ("10m", "80m"):

        u = F.col(f"u{height}_interp")
        v = F.col(f"v{height}_interp")

        magnitude = F.sqrt(
            F.pow(u, 2) + F.pow(v, 2)
        )

        direction = (
            F.degrees(F.atan2(u, v))
            + F.lit(360.0)
        ) % F.lit(360.0)

        df_triangulated = (
            df_triangulated
            .withColumn(
                f"wind_direction_{height}",
                F.when(
                    magnitude > F.lit(epsilon),
                    direction,
                ),
            )
            .drop(
                f"u{height}_interp",
                f"v{height}_interp",
            )
        )

    return df_triangulated.drop("fire_id")


def delete_hdfs_partition(spark, mart_path, execution_date):
    """
    Удаляет только целевую партицию.

    Не затрагивает остальные даты в MART.
    """

    partition_path = (
        f"{mart_path.rstrip('/')}/dt={execution_date}"
    )

    path = spark._jvm.org.apache.hadoop.fs.Path(
        partition_path
    )

    fs = path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    if fs.exists(path):
        if not fs.delete(path, True):
            raise RuntimeError(
                f"Cannot delete HDFS partition: {partition_path}"
            )


def write_to_hdfs_mart(
    spark,
    df,
    mart_path,
    execution_date,
):
    """
    Перезаписывает только целевую дату.

    В случае пустого результата удаляет старую партицию,
    чтобы не оставлять устаревшие данные.
    """

    has_data = not df.limit(1).rdd.isEmpty()

    if not has_data:
        delete_hdfs_partition(
            spark,
            mart_path,
            execution_date,
        )

        print(
            f"No data for {execution_date}. "
            "Target partition removed."
        )

        return

    (
        df.write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(mart_path)
    )


def main():
    args = parse_and_validate_config()

    spark = create_spark_session(args)

    try:
        target_dt = datetime.strptime(
            args.execution_date,
            "%Y-%m-%d",
        ).date()

        next_day = (
            target_dt + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # FIRMS: целевой день + следующий день.
        # Следующий день необходим для определения границ интервалов
        # наблюдений целевого дня.

        df_firms_raw = (
            spark.read
            .parquet(args.firms_path)
            .filter(
                (
                    F.to_date(F.col("acq_date"))
                    >= F.lit(args.execution_date)
                )
                & (
                    F.to_date(F.col("acq_date"))
                    <= F.lit(next_day)
                )
            )
        )

        validate_columns(
            df_firms_raw,
            REQUIRED_FIRMS_COLUMNS,
            "FIRMS",
        )

        # Погодные данные: целевой день + следующий день.
        # Нужны все почасовые наблюдения внутри интервалов.

        df_weather_raw = (
            spark.read
            .parquet(args.weather_path)
            .filter(
                (
                    F.to_date(F.col("time"))
                    >= F.lit(args.execution_date)
                )
                & (
                    F.to_date(F.col("time"))
                    <= F.lit(next_day)
                )
            )
        )

        validate_columns(
            df_weather_raw,
            REQUIRED_WEATHER_COLUMNS,
            "Open-Meteo",
        )

        df_mart = build_triangulated_mart(
            df_firms=df_firms_raw,
            df_openmeteo=df_weather_raw,
            search_radius=args.search_radius_km,
            fallback_hours=args.fallback_hours,
            epsilon=args.epsilon,
        )

        # Оставляем только спутниковые наблюдения целевого дня.
        # Каждое наблюдение сохраняет все связанные с ним часы погоды.

        df_mart_final = (
            df_mart
            .filter(
                F.to_date(F.col("f_start_ts"))
                == F.lit(args.execution_date)
            )
            .withColumn(
                "dt",
                F.lit(args.execution_date),
            )
            .withColumn(
                "run_id",
                F.lit(args.run_id),
            )
            .withColumn(
                "processed_at",
                F.current_timestamp(),
            )
        )

        write_to_hdfs_mart(
            spark=spark,
            df=df_mart_final,
            mart_path=args.mart_path,
            execution_date=args.execution_date,
        )

        logger.info(
            f"Successfully processed FIRMS weather mart "
            f"for {args.execution_date}"
        )

    except Exception as e:
        logger.error(
            f"CRITICAL ERROR in Spark job: {e}",
            file=sys.stderr,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
