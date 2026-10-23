from pyspark.sql import SparkSession
import argparse
import posixpath

parser = argparse.ArgumentParser(description="Spark OpenMeteo Validation")
parser.add_argument("--staging_path", required=True, type=str)
parser.add_argument("--publish_path", required=True, type=str)
parser.add_argument("--execution_date", required=True, type=str)

args = parser.parse_args()


spark = SparkSession.builder.appName("airflow-hdfs-test").getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("pyspark_logger")

year, month, day = args.execution_date.split("-")
staging_path = posixpath.join(
    args.staging_path,
    f"year={year}",
    f"month={month}",
    f"day={day}",
)

requested_params = set(
    [
        "temperature_2m",
        "weather_code",
        "cloud_cover",
        "rain",
        "showers",
        "precipitation",
        "precipitation_probability",
        "apparent_temperature",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "soil_temperature_0cm",
        "soil_moisture_0_to_1cm",
        "soil_moisture_1_to_3cm",
        "soil_moisture_3_to_9cm",
        "relative_humidity_2m",
        "wind_speed_80m",
        "wind_direction_80m",
        "dew_point_2m",
        "vapour_pressure_deficit",
        "evapotranspiration",
        "et0_fao_evapotranspiration",
        "cape",
        "boundary_layer_height",
    ]
)

try:
    staging = spark._jvm.org.apache.hadoop.fs.Path(staging_path)
    fs = staging.getFileSystem(spark._jsc.hadoopConfiguration())

    if not fs.exists(staging):
        raise FileNotFoundError(f"Staging-каталог не существует: {staging_path}")

    iterator = fs.listFiles(staging, True)
    files_to_validate = []

    while iterator.hasNext():
        file_status = iterator.next()
        if file_status.isFile():
            files_to_validate.append(file_status.getPath().toString())

    if not files_to_validate:
        raise ValueError(f"В staging нет файлов: {staging_path}")

    invalid_files = []

    for file_path in files_to_validate:

        try:
            df = (
                spark.read.option("multiLine", "true")
                .option("mode", "FAILFAST")
                .json(file_path)
            )

            if "hourly" not in df.columns:
                invalid_files.append(
                    (file_path, f"Нет объекта hourly. Поля: {df.columns}")
                )
                continue

            actual_columns = set(df.select("hourly.*").columns)
            missing_params = requested_params - actual_columns

            if missing_params:
                invalid_files.append(
                    (
                        file_path,
                        f"Отсутствуют параметры: {sorted(missing_params)}",
                    )
                )
                continue

            try:
                df.foreachPartition(lambda rows: sum(1 for _ in rows))
            except Exception as e:
                invalid_files.append((file_path, str(e)))

            logger.info(f"Файл прошёл валидацию: {file_path}")

        except Exception as e:
            logger.error(f"Ошибка в файле {file_path}: {e}")
            raise

    if invalid_files:
        details = "; ".join(f"{path}: {error}" for path, error in invalid_files)
        logger.error(
            f"Валидация не пройдена. "
            f"Невалидных файлов: {len(invalid_files)}. {details}"
        )

        for file_path, error in invalid_files:
            path = spark._jvm.org.apache.hadoop.fs.Path(file_path)

            try:
                success = fs.delete(path, False)
            except Exception as e:
                logger.error(f"Системная ошибка при удалении из HDFS: {str(e)}")
                raise RuntimeError(
                    f"Ошибка удаления невалидного файла {file_path}. "
                    f"Причина невалидности: {error}. "
                    f"Ошибка удаления: {e}"
                ) from e

            if not success:
                raise RuntimeError(
                    f"Не удалось удалить невалидный файл: {file_path} : ошибка обработки: {error}"
                )

        raise ValueError(
            f"Валидация не пройдена. некорректных файлов: {len(invalid_files)}: {details}"
        )

    logger.info(f"Все {len(files_to_validate)} файлов прошли валидацию.")

except Exception as e:
    logger.error(f"Ошибка в процессе валидации Spark: {e}")
    raise RuntimeError(f"Ошибка в процессе валидации Spark: {e}") from e

finally:
    spark.stop()
