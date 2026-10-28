# NASA FIRMS RAW -> STG + КАРАНТИН
#
# НАЗНАЧЕНИЕ
# - Реализовать детерминированные проверки файла и отдельных записей,
#   расчёт метрик, дедупликацию и публикацию результатов за одну логическую дату.
# - Записать полный изолированный набор результатов и сделать его доступным
#   только после фиксации через специальный указатель.

import argparse
import posixpath
from datetime import date
import re
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
)

# ВХОДНАЯ СХЕМА — ТОЧНЫЙ ОЖИДАЕМЫЙ ЗАГОЛОВОК CSV, 14 ПОЛЕЙ
# latitude, longitude, bright_ti4, scan, track, acq_date, acq_time,
# satellite, instrument, confidence, version, bright_ti5, frp, daynight
#
schema = StructType(
    [
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("bright_ti4", StringType()),
        StructField("scan", StringType()),
        StructField("track", StringType()),
        StructField("acq_date", StringType()),
        StructField("acq_time", StringType()),
        StructField("satellite", StringType()),
        StructField("instrument", StringType()),
        StructField("confidence", StringType()),
        StructField("version", StringType()),
        StructField("bright_ti5", StringType()),
        StructField("frp", StringType()),
        StructField("daynight", StringType()),
    ]
)


# НАБОР ВЫХОДНЫХ ДАННЫХ
# - stg/: корректные записи после дедупликации, в согласованной схеме и формате STG.
# - quarantine/: отклонённые исходные записи со всеми 14 полями исходного CSV,
#   а также source_file, processed_at, failed_rules.
# - metrics.json: машинно-читаемые метрики входных данных, качества, дедупликации
#   и выходных данных.
# - manifest.json: идентификатор запуска, сведения о входных данных, версии схемы
#   и правил, пути результатов, счётчики, контрольные данные для проверки,
#   если они применимы, и статус набора.
# ПЕРЕДАЮТСЯ ЧЕРЕЗ АРГУМЕНТЫ

#
# МОДЕЛЬ ДАННЫХ И ПУБЛИКАЦИИ
# - Использовать уникальный рабочий каталог каждого запуска.
#   Никогда не записывать напрямую поверх текущего STG.
# - Полностью подготовить и проверить все результаты до того, как сделать их видимыми.
# - Опубликовать полный набор результатов по неизменяемому пути конкретного запуска.
# - Атомарно заменить небольшой манифест/указатель CURRENT только после подготовки набора.
# - Потребители должны определять текущую версию через CURRENT.
#   Они не должны читать незавершённые рабочие каталоги или выбирать текущую версию
#   путём перечисления каталогов.
# - Переименование в HDFS атомарно только в пределах соответствующей файловой системы
#   и пространства имён; оно не создаёт транзакцию для нескольких выходных каталогов.
#   Единой точкой фиксации служит указатель CURRENT.
#
# ОТВЕТСТВЕННОСТЬ ФУНКЦИЙ И ПОРЯДОК ВЫПОЛНЕНИЯ


# 1. parse_and_validate_config
# - Разобрать execution_date, run_id, входные пути, корень рабочего каталога
#   и корень публикации.
# - Проверить обязательные аргументы и отклонить конфликтующие или неоднозначные пути.
# - Не записывать учётные данные и секретную конфигурацию в логи.
def parse_and_validate_config():
    parser = argparse.ArgumentParser(description="Spark NASA FIRMS transformation")
    parser.add_argument("--raw_path", required=True, type=str)
    parser.add_argument("--stg_path", required=True, type=str)
    parser.add_argument("--run_id", required=True, type=str)
    parser.add_argument("--execution_date", required=True, type=str)
    return parser.parse_args()


# 2. create_spark_session
# - Создать одну SparkSession с настройками Spark, предоставленными окружением запуска.
# - Не задавать противоречащие друг другу значения master одновременно
#   в параметрах запуска и конфигурации приложения.
# - Обеспечить завершение приложения с ненулевым кодом при необработанной ошибке
#   обработки или публикации.
def create_spark_session(args):
    return SparkSession.builder.appName(
        f"firms-raw-to-stg-{args.execution_date}"
    ).getOrCreate()


#
# 3. resolve_run_paths
def resolve_run_paths(session, args, logger):
    try:
        execution_date = date.fromisoformat(args.execution_date)
    except ValueError as exc:
        raise ValueError(
            "execution_date должен быть реальной датой в формате YYYY-MM-DD"
        ) from exc

    # - Определить точный раздел RAW и ожидаемые исходные файлы.
    raw_path = posixpath.join(
        args.raw_path,
        f"year={execution_date:%Y}",
        f"month={execution_date:%m}",
        f"day={execution_date:%d}",
    )

    raw = session._jvm.org.apache.hadoop.fs.Path(raw_path)
    fs = raw.getFileSystem(session._jsc.hadoopConfiguration())

    if not fs.exists(raw):
        raise FileNotFoundError(f"RAW-каталог не существует: {raw_path}")

    hdfs_run_id = args.run_id.replace(":", "-")

    # Корень публикации.
    publication_root = args.stg_path

    # Изолированная рабочая область этого запуска.
    staging_run_path = posixpath.join(publication_root, "staging", hdfs_run_id)

    # - Определить изолированные пути рабочего каталога для STG, карантина, метрик и манифеста.
    stg_path = posixpath.join(staging_run_path, "stg")
    quarantine_path = posixpath.join(staging_run_path, "quarantine")
    metadata_path = posixpath.join(staging_run_path, "metadata")

    # - Определить неизменяемый путь опубликованного набора и указатель CURRENT для даты.
    published_run_path = posixpath.join(publication_root, "runs", hdfs_run_id)

    current_path = posixpath.join(
        publication_root,
        "CURRENT",
        f"year={execution_date:%Y}",
        f"month={execution_date:%m}",
        f"day={execution_date:%d}",
    )

    paths = {
        "raw": raw_path,
        "publication_root": publication_root,
        "staging_run": staging_run_path,
        "staging_stg": stg_path,
        "staging_quarantine": quarantine_path,
        "staging_metadata": metadata_path,
        "published_run": published_run_path,
        "current": current_path,
    }

    logger.info(f"Пути запуска: {paths}")
    return paths


# 4. identify_and_validate_input_files
# - Проверить наличие входных файлов и их стабильность на время обработки.
# - Зафиксировать имена исходных файлов и доступные стабильные метаданные происхождения.
# - Проверить точные имена и порядок полей заголовка CSV, а также наличие 14 колонок.
# - Отсутствующий вход, нечитаемые файлы, неверный заголовок и повреждение структуры CSV
#   считать ошибками всего запуска, а не обычными отклонениями отдельных строк.
# - Явно настроить чтение CSV и проверить тестами поведение для повреждённых записей.
# - Не допускать незаметного пропуска некорректных строк и не полагаться на настройки
#   парсера по умолчанию как на политику качества данных.
def identify_and_validate_input_files(session, raw_path):
    expected_columns = [
        "latitude",
        "longitude",
        "bright_ti4",
        "scan",
        "track",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument",
        "confidence",
        "version",
        "bright_ti5",
        "frp",
        "daynight",
    ]

    raw = session._jvm.org.apache.hadoop.fs.Path(raw_path)
    fs = raw.getFileSystem(session._jsc.hadoopConfiguration())

    if not fs.exists(raw):
        raise FileNotFoundError(f"RAW-каталог не существует: {raw_path}")

    if not fs.getFileStatus(raw).isDirectory():
        raise ValueError(f"Ожидался RAW-каталог, получен файл: {raw_path}")

    statuses = fs.listStatus(raw)
    input_files = sorted(
        (
            status.getPath()
            for status in statuses
            if status.isFile() and status.getPath().getName().lower().endswith(".csv")
        ),
        key=lambda path: path.toString(),
    )

    if not input_files:
        raise FileNotFoundError(f"В RAW-каталоге отсутствуют CSV-файлы: {raw_path}")

    result = []

    for file_path in input_files:
        file_name = file_path.getName()
        df = (
            session.read.option("header", True)
            .option("inferSchema", False)
            .schema(schema)
            .csv(file_path.toString())
        )

        # ПРОВЕРКА ПОД ВОПРОСОМ, ЭКСТРАКТ ПРЕДПОЛАГАЕТ ПУСТОЙ СЕТ, ЕСЛИ НЕ БЫЛО ПОЖАРОВ
        # if len(df.columns) == 0:
        #     raise ValueError(f"Файл пуст: {file_path}")

        if expected_columns != df.columns:
            raise ValueError(
                f"Неверный заголовок CSV: {file_path}\n"
                f"Ожидалось: {expected_columns}\n"
                f"Получено:  {df.columns}"
            )

        result.append(
            {
                "path": file_path.toString(),
                "file_name": file_name,
            }
        )

    return result


# 5. read_source_records
# - Изначально читать исходные поля как строки, чтобы сохранять исходные значения
#   некорректных полей в карантине и выполнять преобразование типов явно.
# - Сохранять source_file для каждой записи.
# - Определить и протестировать обработку CSV с кавычками, пустыми значениями,
#   пробелами и повреждёнными строками в рамках выбранных настроек Spark CSV reader.
# - Рассчитывать input_count как количество успешно разобранных записей данных,
#   не включая заголовок.


# 6. validate_record_fields
# - Не вводить неутверждённые физические пороги для температуры яркости или FRP. (отменено, строго больше 0)
# - Хранить исходные строковые значения отдельно от преобразованных типизированных колонок.
def build_record_rules(execution_date):
    def as_double(field):
        return F.expr(f"try_cast(trim(`{field}`) AS DOUBLE)")

    def as_integer(field):
        return F.expr(f"try_cast(trim(`{field}`) AS INT)")

    # - latitude: обязательное числовое значение в диапазоне [-90, 90].

    latitude = as_double("latitude")
    lat_err = latitude.isNull() | (~latitude.between(-90, 90))

    # - longitude: обязательное числовое значение в диапазоне [-180, 180].
    longitude = as_double("longitude")
    lon_err = longitude.isNull() | (~longitude.between(-180, 180))

    # - acq_date: обязательная корректная календарная дата.
    # TODO: - acq_date должна совпадать с execution_date. (пока отменено)
    #   Несовпадение проверять только после успешного разбора даты.(пока отменено)
    parsed_timestamp = F.try_to_timestamp(
        F.trim(F.col("acq_date")),
        F.lit("yyyy-MM-dd"),
    )
    parsed_date = F.to_date(parsed_timestamp)
    acq_date_err = parsed_date.isNull()

    # - acq_time: обязательное время в формате HHMM; часы 0–23, минуты 0–59.
    #   Допускать значения без ведущего нуля, например 357.
    time_int = as_integer("acq_time")
    hours = F.floor(time_int / 100)
    minutes = time_int % 100

    acq_time_err = (
        time_int.isNull()
        | (~hours.between(0, 23))
        | (~minutes.between(0, 59))
        | (time_int < 0)
    )

    # - confidence: допустимы только l, n, h; значение l считается корректным.
    # - daynight: допустимы только D или N.
    confidence = F.trim(F.col("confidence"))
    confidence_err = confidence.isNull() | (~confidence.isin(["l", "n", "h"]))

    daynight = F.trim(F.col("daynight"))
    daynight_err = daynight.isNull() | (~daynight.isin(["D", "N"]))

    # - bright_ti4, bright_ti5, frp: обязательные значения, преобразуемые в числа больше нуля.
    # - scan, track: обязательные числовые значения больше нуля.
    required_positive_fields = ["bright_ti4", "bright_ti5", "frp", "scan", "track"]
    numeric_err = F.lit(False)
    for field in required_positive_fields:
        value = as_double(field)
        numeric_err |= value.isNull() | (value <= 0)

    # - version: обязательное непустое значение.
    # - satellite: обязательный текст; не использовать фиксированный список допустимых спутников.
    # - instrument: проверять только согласованный контракт обязательности/непустого значения,
    required_string_fields = ["version", "satellite", "instrument"]
    missing_err = F.lit(False)
    for field in required_string_fields:
        value = F.trim(F.col(field))
        missing_err |= value.isNull() | (value == "")

    return {
        "lat_err": lat_err,
        "lon_err": lon_err,
        "acq_date_err": acq_date_err,
        "acq_time_err": acq_time_err,
        "confidence_err": confidence_err,
        "daynight_err": daynight_err,
        "numeric_err": numeric_err,
        "missing_err": missing_err,
    }


# 7. classify_records
# - Классифицировать каждую разобранную запись ровно один раз: корректная или некорректная.
# - invalid_count — количество уникальных записей с одним или несколькими нарушениями.
# - Счётчик каждого правила — число записей, нарушающих именно это правило.
#   Сумма счётчиков правил может превышать invalid_count, так как одна запись
#   может нарушать несколько правил.
# - Запись с несколькими нарушениями должна попасть в карантин один раз,
#   со всеми нарушенными правилами.
def classify_records(df, execution_date):
    rules = build_record_rules(execution_date)

    return df.withColumn(
        "error_codes",
        F.array_compact(
            F.array(
                F.when(rules["lat_err"], F.lit("ERR_LATITUDE")),
                F.when(rules["lon_err"], F.lit("ERR_LONGITUDE")),
                F.when(rules["acq_date_err"], F.lit("ERR_ACQ_DATE")),
                F.when(rules["acq_time_err"], F.lit("ERR_ACQ_TIME")),
                F.when(rules["confidence_err"], F.lit("ERR_CONFIDENCE")),
                F.when(rules["daynight_err"], F.lit("ERR_DAYNIGHT")),
                F.when(rules["numeric_err"], F.lit("ERR_NUMERIC_VALUES")),
                F.when(rules["missing_err"], F.lit("ERR_MISSING_MANDATORY")),
            )
        ),
    )


# 8. build_quarantine_dataset
# - Сохранять все 14 исходных полей без замены исходных значений результатами преобразований.
# - Добавлять source_file, processed_at и failed_rules.
# - Пока не добавлять номер исходной строки, checksum или идентификатор загрузки,
#   если это отдельно не согласовано.
# - Записывать Parquet в изолированный рабочий каталог запуска.
# - Если отклонённых записей нет, представлять карантин как явно пустой набор данных
#   с ожидаемой схемой и количеством записей 0.
def build_quarantine_dataset(df):
    return (
        df.filter(F.size(F.col("error_codes")) > 0)
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("failed_rules", F.col("error_codes"))
        .drop("error_codes")
    )


#
# 9. deduplicate_valid_records
# - Выполнять дедупликацию только корректных записей и только после проверок качества.
# - Ключ точного совпадения:
#   latitude, longitude, acq_date, acq_time, bright_ti4, bright_ti5,
#   frp, satellite, instrument.
# - Не округлять координаты и числовые измерения перед сравнением.
# - confidence, scan, track, version и daynight исключены из ключа согласно текущей политике.
# - Документировать, что записи, отличающиеся только исключёнными полями,
#   будут схлопываться в одну запись.
# - Определить детерминированный выбор сохраняемой записи среди дубликатов,
#   чтобы повторные запуски давали стабильный результат.
#   Например, задать документированный порядок сравнения исходных значений.
#   Не полагаться на неопределённый порядок строк в Spark.(сохранять самую раннюю)
# - duplicate_count — количество корректных записей, удалённых при дедупликации.
def deduplicate_valid_records(df):
    dedup_key = [
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "bright_ti4",
        "bright_ti5",
        "frp",
        "satellite",
        "instrument",
    ]
    valid_records = df.filter(F.size(F.col("error_codes")) == 0).drop("error_codes")

    window_spec = Window.partitionBy(*dedup_key).orderBy(
        F.col("source_file").asc(),
        F.col("version").asc(),
        F.col("confidence").asc(),
        F.col("scan").asc(),
        F.col("track").asc(),
        F.col("daynight").asc(),
    )

    window_count_spec = Window.partitionBy(*dedup_key)

    df_deduplicated = (
        valid_records.withColumn("row_num", F.row_number().over(window_spec))
        .withColumn(
            "duplicate_count",
            F.count("*").over(window_count_spec) - 1,
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return df_deduplicated


# 10. calculate_metrics
# - input_count: количество разобранных входных записей.
# - invalid_count: количество записей с одним или несколькими нарушениями качества.
# - Счётчики правил: количество записей, нарушающих каждое отдельное правило.
# - duplicate_count: количество корректных записей, удалённых при дедупликации.
# - output_count: количество уникальных корректных записей, записанных в STG.
# - Для структурно корректного входа проверить равенство:
#   input_count = invalid_count + duplicate_count + output_count.
# - Если равенство не выполняется, завершить запуск ошибкой,
#   а не публиковать несогласованные метрики.
# - Хранить метрики в структурированном машинно-читаемом формате и в логах.
def calculate_metrics(session, source_path, dt, logger):
    df = (
        session.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .schema(schema)
        .load(source_path)
        .withColumn("source_file", F.input_file_name())
    )

    classified_df = classify_records(df, dt).cache()
    stg_df = None

    files = identify_and_validate_input_files(session, source_path)

    try:
        quarantine_df = build_quarantine_dataset(classified_df)

        metrics_row = classified_df.agg(
            F.count("*").alias("input_count"),
            F.sum(F.when(F.size("error_codes") > 0, 1).otherwise(0)).alias(
                "invalid_count"
            ),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_LATITUDE"), 1).otherwise(0)
            ).alias("cnt_lat"),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_LONGITUDE"), 1).otherwise(0)
            ).alias("cnt_lon"),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_ACQ_DATE"), 1).otherwise(0)
            ).alias("cnt_date"),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_ACQ_TIME"), 1).otherwise(0)
            ).alias("cnt_time"),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_CONFIDENCE"), 1).otherwise(
                    0
                )
            ).alias("cnt_conf"),
            F.sum(
                F.when(F.array_contains("error_codes", "ERR_DAYNIGHT"), 1).otherwise(0)
            ).alias("cnt_daynight"),
            F.sum(
                F.when(
                    F.array_contains("error_codes", "ERR_NUMERIC_VALUES"), 1
                ).otherwise(0)
            ).alias("cnt_numeric"),
            F.sum(
                F.when(
                    F.array_contains("error_codes", "ERR_MISSING_MANDATORY"), 1
                ).otherwise(0)
            ).alias("cnt_missing"),
        ).first()

        input_count = metrics_row["input_count"] or 0
        invalid_count = metrics_row["invalid_count"] or 0

        stg_df = deduplicate_valid_records(classified_df).cache()

        counts = stg_df.agg(
            F.coalesce(F.sum("duplicate_count"), F.lit(0)).alias("duplicate_count"),
            F.count("*").alias("output_count"),
        ).first()

        duplicate_count = int(counts["duplicate_count"])
        output_count = int(counts["output_count"])

        if input_count != invalid_count + duplicate_count + output_count:
            raise ValueError(
                "Нарушен баланс записей: "
                f"input={input_count}, "
                f"invalid={invalid_count}, "
                f"duplicates={duplicate_count}, "
                f"output={output_count}"
            )

        metrics_report = {
            "source_path": source_path,
            "files": [{"path": f["path"], "file_name": f["file_name"]} for f in files],
            "execution_date": dt,
            "input_count": input_count,
            "invalid_count": invalid_count,
            "duplicate_count": duplicate_count,
            "output_count": output_count,
            "rules_breakdown": {
                "ERR_LATITUDE": metrics_row["cnt_lat"] or 0,
                "ERR_LONGITUDE": metrics_row["cnt_lon"] or 0,
                "ERR_ACQ_DATE": metrics_row["cnt_date"] or 0,
                "ERR_ACQ_TIME": metrics_row["cnt_time"] or 0,
                "ERR_CONFIDENCE": metrics_row["cnt_conf"] or 0,
                "ERR_DAYNIGHT": metrics_row["cnt_daynight"] or 0,
                "ERR_NUMERIC_VALUES": metrics_row["cnt_numeric"] or 0,
                "ERR_MISSING_MANDATORY": metrics_row["cnt_missing"] or 0,
            },
        }

        return metrics_report, quarantine_df, stg_df

    except Exception:
        classified_df.unpersist()
        if stg_df is not None:
            stg_df.unpersist()
        raise


#
# 11. enforce_publication_policy
# - Структурно корректный CSV, содержащий только заголовок и не содержащий записей,
#   считается допустимым пустым входом.
#   Для него публикуются пустые STG и карантин.
# - Не обновлять CURRENT при неуспешном запуске.
def publish_staging_run(
    session,
    metadata_path,
    quarantine_path,
    stg_path,
    manifest,
    metrics_report,
    quarantine_df,
    stg_df,
    logger,
):
    # - Непустой вход, в котором все записи отклонены, не равнозначен пустому входу.
    # - По умолчанию для полностью невалидного входа завершать запуск ошибкой,
    #   пока отдельная эксплуатационная политика явно не разрешит публикацию пустого STG.
    if (
        metrics_report["input_count"] > 0
        and metrics_report["input_count"] == metrics_report["invalid_count"]
    ):
        try:
            # - Сохранять метрики в рабочем каталоге неуспешного запуска для диагностики.
            if not write_metrics_and_manifest(
                session, metadata_path, manifest, metrics_report, logger
            ):
                raise ValueError(f"Ошибка записи манифеста по пути {metadata_path}")
        except Exception as e:
            logger.error(
                f"ФУЛ ФАКАП!!!! ошибка при ошибке, не удалось сохранить манифест и репорт: {e}"
            )
            raise ValueError(f"{e}")
        raise ValueError(
            f'{metrics_report["invalid_count"]} из {metrics_report["input_count"]} некорректны, проверьте данные в {metrics_report["source_path"]}!'
        )

    try:
        write_and_verify_stg(
            stg_path, stg_df, metrics_report["output_count"], logger
        )
        write_and_verify_quarantine(
            quarantine_path, quarantine_df, metrics_report["invalid_count"], logger
        )
        write_metrics_and_manifest(
            session, metadata_path, manifest, metrics_report, logger
        )
    except Exception as e:
        logger.error(f"Сбой публикации набора данных: {e}")
        raise ValueError(f"Пайплайн остановлен из-за ошибки записи/верификации: {e}")


# 12. write_and_verify_stg
# - Записывать в рабочий каталог только корректные записи после дедупликации.
# - Использовать согласованные формат и схему STG.
#   Не выводить схему только на основе одной порции данных.
# - Повторно прочитать результат или другим способом проверить его,
#   сравнив фактическое количество записей с output_count.
# - Пустой корректный вход должен приводить к валидному пустому набору STG
#   со стабильной схемой.
def write_and_verify_stg(stg_path, df, expected_count, logger):
    try:
        logger.info("Запись stg в рабочий каталог")
        df.write.mode("overwrite").format("parquet").option(
            "compression", "snappy"
        ).save(stg_path)

        actual_count = df.sparkSession.read.parquet(stg_path).count()

        if actual_count != expected_count:
            raise ValueError(
                f"Верификация STG провалена! Ожидалось записей: {expected_count}, "
                f"фактически записано на HDFS: {actual_count}"
            )
        logger.info("STG слой успешно записан и верифицирован.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи/верификации STG: {e}")
        raise


#
# 13. write_and_verify_quarantine
# - Записывать отклонённые записи в Parquet в рабочем каталоге запуска.
def write_and_verify_quarantine(quarantine_path, df, expected_count, logger):
    try:
        logger.info(f"Запись карантина в рабочий каталог: {quarantine_path}")
        df.write.mode("overwrite").format("parquet").option(
            "compression", "snappy"
        ).save(quarantine_path)

        # - Проверить схему карантина и сравнить количество записей с invalid_count.
        quarantine_check = df.sparkSession.read.parquet(quarantine_path)
        actual_count = quarantine_check.count()

        if actual_count != expected_count:
            raise ValueError(
                f"Верификация карантина провалена! Ожидалось записей: {expected_count}, "
                f"фактически записано: {actual_count}"
            )

        # - Убедиться, что у каждой записи карантина заполнено поле failed_rules.
        empty_rules_count = quarantine_check.filter(
            F.col("failed_rules").isNull() | (F.size(F.col("failed_rules")) == 0)
        ).count()

        if empty_rules_count > 0:
            raise ValueError(
                f"Критическое нарушение контракта карантина! Найдено {empty_rules_count} "
                f"записей без указания кодов ошибок (failed_rules)."
            )

        logger.info("Карантин успешно записан и верифицирован.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи/верификации карантина: {e}")
        raise


# 14. write_metrics_and_manifest
# - Записывать metrics.json и manifest.json только после проверки выходных данных.
# - Включать run_id, execution_date, идентификаторы исходных файлов,
#   версии схемы и правил, счётчики и относительные пути результатов.
# - Указывать статус набора, означающий готовность к публикации.
# - На этом этапе ещё не помечать набор как зафиксированный.
def write_metrics_and_manifest(session, metadata_path, manifest, metrics, logger):
    fs = None

    try:
        logger.info(f"Запись метрик и манифеста: {metadata_path}")

        jvm = session._jvm
        conf = session.sparkContext._jsc.hadoopConfiguration()
        metadata = jvm.org.apache.hadoop.fs.Path(metadata_path)
        fs = metadata.getFileSystem(conf)

        if not fs.exists(metadata) and not fs.mkdirs(metadata):
            raise OSError(f"Не удалось создать каталог метаданных: {metadata_path}")

        full_manifest = {
            "run_id": manifest["run_id"],
            "execution_date": manifest["execution_date"],
            "source_files_path": manifest["source_files_path"],
            "source_files": metrics["files"],
            "schema_version": manifest["schema_version"],
            "rules_version": manifest["rules_version"],
            "status": "READY_FOR_PUBLICATION",
            "is_committed": False,
            "metrics_summary": {
                key: value for key, value in metrics.items() if key != "files"
            },
        }

        artifacts = {
            "metrics.json": metrics,
            "manifest.json": full_manifest,
        }

        for filename, payload in artifacts.items():
            target = jvm.org.apache.hadoop.fs.Path(metadata, filename)

            content = json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            )

            # Не перезаписываем существующий артефакт вслепую.
            if fs.exists(target):
                raise FileExistsError(f"Файл метаданных уже существует: {target}")

            stream = fs.create(target, False)

            try:
                stream.write(bytearray(content.encode("utf-8")))
                stream.hflush()
            finally:
                stream.close()

            logger.info(f"Записан файл: {target}")

        return True

    except Exception as e:
        logger.error(f"Ошибка сохранения метрик и манифеста: {metadata_path}: {e}")
        raise


def read_json(session, path):
    jvm = session._jvm
    conf = session.sparkContext._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    hdfs_path = Path(str(path))
    fs = hdfs_path.getFileSystem(conf)

    stream = fs.open(hdfs_path)
    try:
        data = jvm.org.apache.commons.io.IOUtils.toByteArray(stream)
        content = bytes(data).decode("utf-8")
        return json.loads(content)
    finally:
        stream.close()

# 15. publish_immutable_bundle
# - Переместить полностью подготовленный набор из рабочего каталога в неизменяемый
#   путь опубликованного запуска, используя переименование HDFS в пределах одной
#   файловой системы, если оно поддерживается.
# - Если целевой путь уже существует, проверить, является ли он тем же завершённым запуском.
#   Не перезаписывать его вслепую.
# - Не удалять автоматически рабочие или опубликованные каталоги при неоднозначном состоянии.
#   Оставлять такие случаи для контролируемого восстановления.
def publish_immutable_run(
    session, metadata_path, staging_path, published_path, run_id, logger
):
    jvm = session._jvm
    conf = session.sparkContext._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    src = Path(staging_path)
    dst = Path(published_path)
    fs = dst.getFileSystem(conf)

    logger.info(f"Публикация набора: {src} -> {dst}")

    def verify_bundle(root):
        manifest_path = Path(root, "metadata/manifest.json")
        metrics_path = Path(root, "metadata/metrics.json")
        stg_path = Path(root, "stg")
        quarantine_path = Path(root, "quarantine")

        required_paths = [
            manifest_path,
            metrics_path,
            stg_path,
            quarantine_path,
        ]

        missing = [str(path) for path in required_paths if not fs.exists(path)]
        if missing:
            raise RuntimeError(f"Неполный набор результатов: {missing}")

        manifest = read_json(session, manifest_path)
        metrics = read_json(session, metrics_path)

        if manifest.get("status") != "READY_FOR_PUBLICATION":
            raise RuntimeError("Манифест не подтверждает готовность набора.")

        if manifest.get("is_committed") is not False:
            raise RuntimeError("Перед фиксацией ожидался is_committed=false.")

        if manifest.get("run_id") is None:
            raise RuntimeError("В манифесте отсутствует run_id.")

        if manifest.get("metrics_summary", {}).get("output_count") != metrics.get(
            "output_count"
        ):
            raise RuntimeError("Метрики манифеста не совпадают с metrics.json.")

        return manifest

    if fs.exists(dst):
        manifest_path = Path(dst, "metadata/manifest.json")

        if not fs.exists(manifest_path):
            raise RuntimeError(
                f"Критическая неоднозначность данных! Каталог {dst} существует, "
                f"но не содержит metadata/manifest.json. Автоматическое удаление запрещено во избежание потери данных. "
                f"Требуется ручное вмешательство администратора."
            )

        existing = read_json(session, manifest_path)

        if existing.get("run_id") != run_id:
            raise RuntimeError(
                f"В {dst} находится другой запуск: "
                f"{existing.get('run_id')!r}, "
                f"ожидался {run_id!r}"
            )

        verify_bundle(dst)

        logger.info(
            f"Неизменяемый запуск уже опубликован, "
            f"повторное перемещение пропускается: {dst}"
        )
        return True

    # Проверяем, что исходный рабочий каталог вообще существует и готов к публикации
    if not fs.exists(src) or not fs.getFileStatus(src).isDirectory():
        raise FileNotFoundError(f"Рабочий каталог запуска не найден: {src}")

    staging_manifest_path = Path(metadata_path, "manifest.json")
    if not fs.exists(staging_manifest_path):
        raise ValueError(
            f"Ошибка публикации: в рабочем каталоге {metadata_path} отсутствует manifest.json. "
            f"Набор данных не прошел верификацию качества и не может быть опубликован."
        )

    verify_bundle(src)

    # HDFS rename атомарен в пределах одной файловой системы.
    # Родительские каталоги создаём до перемещения.
    parent = dst.getParent()
    if not fs.exists(parent) and not fs.mkdirs(parent):
        raise OSError(f"Не удалось создать каталог публикации: {parent}")

    if not fs.rename(src, dst):
        raise RuntimeError(f"Не удалось переместить набор: {src} -> {dst}")

    # Проверяем результат после перемещения.
    verify_bundle(dst)

    logger.info(f"Набор опубликован: {dst}")
    return True


# 16. commit_current_run
# - Проверить, не обновил ли другой запуск CURRENT после начала текущего запуска.
# - Записать новый манифест CURRENT во временный путь.
# - Атомарно переименовать временный манифест в CURRENT в том же пространстве имён HDFS.
# - Замена этого указателя является единственной точкой фиксации и видимости
#   одновременно для STG и карантина.
# - Потребители должны получать оба набора данных через один манифест запуска.
# - Если конкурентные публикации нельзя безопасно сериализовать или обнаружить,
#   завершать запуск ошибкой, а не позволять более старому запуску незаметно
#   заменить более новую зафиксированную версию.


def commit_current_run(session, published_run_path, current_path, logger):
    jvm = session._jvm
    gateway = session.sparkContext._gateway
    conf = session.sparkContext._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileUtil = jvm.org.apache.hadoop.fs.FileUtil

    run_path = Path(published_run_path)
    source = Path(run_path, "stg")
    current = Path(current_path)
    backup = Path(current_path + ".backup")
    temp = Path(current_path + f".tmp-{run_path.getName()}")

    fs = current.getFileSystem(conf)

    # Восстановление после предыдущего прерванного запуска.
    if fs.exists(backup):
        if fs.exists(current):
            # Новый CURRENT уже появился: осталась старая резервная копия.
            if not fs.delete(backup, True):
                raise RuntimeError(f"Не удалось удалить резервную копию: {backup}")
        else:
            # CURRENT отсутствует: возвращаем предыдущие данные.
            if not fs.rename(backup, current):
                raise RuntimeError(f"Не удалось восстановить CURRENT из {backup}")

    if not fs.exists(source):
        raise FileNotFoundError(f"STG запуска не найден: {source}")

    # Проверяем, что STG запуска читается и содержит ожидаемое число строк.
    manifest_path = Path(run_path, "metadata/manifest.json")
    metrics_path = Path(run_path, "metadata/metrics.json")

    for path in (manifest_path, metrics_path):
        if not fs.exists(path):
            raise FileNotFoundError(f"Не найден файл: {path}")

    # manifest = json.loads(session.read.text(manifest_path.toString()).first()[0])
    # metrics = json.loads(session.read.text(metrics_path.toString()).first()[0])
    manifest = read_json(session, manifest_path)
    metrics = read_json(session, metrics_path)

    if manifest.get("status") != "READY_FOR_PUBLICATION":
        raise ValueError("Запуск не готов к публикации")

    expected_count = metrics.get("summary", {}).get("output_count")
    if expected_count is None:
        expected_count = metrics.get("output_count")

    if expected_count is None:
        raise ValueError("В metrics.json не найден output_count")

    actual_count = session.read.parquet(source.toString()).count()

    if actual_count != int(expected_count):
        raise RuntimeError(
            f"Количество записей STG не совпало: ожидалось {expected_count}, получено {actual_count}"
        )

    parent = current.getParent()
    if not fs.exists(parent) and not fs.mkdirs(parent):
        raise RuntimeError(f"Не удалось создать каталог: {parent}")

    # Готовим полную копию, не затрагивая текущие опубликованные данные.
    if fs.exists(temp) and not fs.delete(temp, True):
        raise RuntimeError(f"Не удалось очистить временный каталог: {temp}")

    copied = FileUtil.copy(
        fs,
        source,
        fs,
        temp,
        False,
        conf,
    )

    if not copied:
        raise RuntimeError(f"Не удалось скопировать STG в {temp}")

    copied_count = session.read.parquet(temp.toString()).count()

    if copied_count != int(expected_count):
        fs.delete(temp, True)
        raise RuntimeError(
            f"Проверка копии не пройдена: "
            f"ожидалось {expected_count}, получено {copied_count}"
        )

    # Сначала сохраняем прежнюю публикацию.
    had_current = fs.exists(current)

    if had_current and not fs.rename(current, backup):
        fs.delete(temp, True)
        raise RuntimeError(
            f"Не удалось переместить CURRENT в резервную копию: " f"{current}"
        )

    try:
        if not fs.rename(temp, current):
            raise RuntimeError(f"Не удалось опубликовать новый STG: {current}")
    except Exception:
        # При обычной ошибке сразу восстанавливаем предыдущие данные.
        if had_current and fs.exists(backup) and not fs.exists(current):
            if not fs.rename(backup, current):
                logger.error("Не удалось восстановить предыдущий CURRENT")
        raise

    logger.info(
        f"STG опубликован: run_id={manifest.get('run_id')}, path={current}, rows={actual_count}"
    )

    # Удаляем резервную копию только после успешной публикации.
    if had_current and fs.exists(backup):
        if not fs.delete(backup, True):
            logger.error(
                f"Новый STG опубликован, но резервная копия осталась: {backup}"
            )


# 17. emit_final_status
# - Записывать run_id, execution_date, путь зафиксированного манифеста,
#   input_count, invalid_count, счётчики правил, duplicate_count, output_count
#   и статус публикации.
# - Возвращать успешный результат только после проверки фиксации.
#
# 18. main
# - Выполнять функции в описанном порядке.
# - При ошибке сохранять прежний указатель CURRENT и последний зафиксированный набор.
# - Сохранять достаточный контекст для диагностики, не раскрывая секреты.
# - Гарантированно останавливать SparkSession в блоке очистки.
def main():
    spark = None
    try:
        args = parse_and_validate_config()
        spark = create_spark_session(args)

        log4jLogger = spark._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger("pyspark_logger")

        paths = resolve_run_paths(spark, args, logger)

        manifest = {
            "run_id": args.run_id,
            "execution_date": args.execution_date,
            "source_files_path": paths["raw"],
            "schema_version": "1.0",
            "rules_version": "1.0",
        }

        # трансформация
        metrics_report, quarantine_df, stg_df = calculate_metrics(
            spark, paths["raw"], args.execution_date, logger
        )

        publish_staging_run(
            spark,
            paths["staging_metadata"],
            paths["staging_quarantine"],
            paths["staging_stg"],
            manifest,
            metrics_report,
            quarantine_df,
            stg_df,
            logger,
        )

        publish_immutable_run(
            spark,
            paths["staging_metadata"],
            paths["staging_run"],
            paths["published_run"],
            args.run_id,
            logger,
        )

        commit_current_run(spark, paths["published_run"], paths["current"], logger)
    except Exception as e:
        raise ValueError(f"{e}")
    finally:
        if spark is not None:
            spark.stop()


#
# УПРАВЛЕНИЕ СРОКОМ ХРАНЕНИЯ И ВОССТАНОВЛЕНИЕ — ВНЕ ОСНОВНОГО ЗАПУСКА
# - Не удалять предыдущий карантин в начале повторного запуска.
# - Хранить предыдущие зафиксированные наборы, пока отдельная политика хранения
#   не разрешит их удаление.
# - Очистка никогда не должна удалять набор, на который указывает CURRENT,
#   или набор, участвующий в процедуре восстановления.
# - Неуспешные рабочие каталоги можно удалять только после проверки их возраста
#   и статуса, в рамках аудируемой политики.
#
# ПРИЁМОЧНЫЕ ТЕСТЫ, КОТОРЫЕ НУЖНО ОПРЕДЕЛИТЬ ДО РЕАЛИЗАЦИИ
# - Отсутствующий RAW, неверный заголовок, повреждённый CSV и ошибка чтения
#   не изменяют CURRENT.
# - Корректный CSV только с заголовком фиксирует пустые STG и карантин.
# - Непустой полностью невалидный вход по умолчанию завершает запуск ошибкой
#   и не обновляет CURRENT.
# - Несколько нарушений у одной записи создают одну запись карантина
#   со всеми идентификаторами нарушенных правил.
# - Дедупликация и выбор сохраняемой записи детерминированы между повторными запусками.
# - Для каждого успешного запуска выполняется равенство счётчиков входных записей.
# - Сбой во время записи или публикации набора не изменяет предыдущий CURRENT.
# - Сбой после фиксации, но до подтверждения Airflow, безопасно обрабатывается повтором.
# - Два параллельных запуска за одну дату не могут незаметно перезаписать фиксацию друг друга.
# - Потребитель никогда не может увидеть STG одного запуска вместе с карантином другого.

if __name__ == "__main__":
    main()
