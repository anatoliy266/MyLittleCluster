
from pyspark.sql import SparkSession
import argparse
import posixpath


EXPECTED_FIRMS_HEADERS = {
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
}


def main():
    parser = argparse.ArgumentParser(
        description="Spark NASA FIRMS CSV Validation"
    )
    parser.add_argument("--staging_path", required=True, type=str)
    parser.add_argument("--publish_path", required=True, type=str)
    parser.add_argument("--execution_date", required=True, type=str)

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("nasa-firms-validation")
        .getOrCreate()
    )

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("pyspark_logger")

    try:
        year, month, day = args.execution_date.split("-")

        staging_path = posixpath.join(
            args.staging_path,
            f"year={year}",
            f"month={month}",
            f"day={day}",
        )

        staging = spark._jvm.org.apache.hadoop.fs.Path(
            staging_path
        )
        fs = staging.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )

        if not fs.exists(staging):
            logger.info(
                f"STAGING-каталог отсутствует. "
                f"Валидация пропущена: {staging_path}"
            )
            return

        iterator = fs.listFiles(staging, True)
        files_to_validate = []

        while iterator.hasNext():
            file_status = iterator.next()

            if file_status.isFile():
                files_to_validate.append(
                    file_status.getPath().toString()
                )

        if not files_to_validate:
            logger.info(
                f"В STAGING нет файлов. "
                f"Валидация пропущена: {staging_path}"
            )
            return

        invalid_files = []

        for file_path in files_to_validate:
            try:
                df = (
                    spark.read
                    .option("header", "true")
                    .option("inferSchema", "false")
                    .option("mode", "FAILFAST")
                    .option("enforceSchema", "false")
                    .csv(file_path)
                )

                actual_headers = set(df.columns)

                missing_headers = (
                    EXPECTED_FIRMS_HEADERS - actual_headers
                )
                unexpected_headers = (
                    actual_headers - EXPECTED_FIRMS_HEADERS
                )

                if missing_headers or unexpected_headers:
                    invalid_files.append(
                        (
                            file_path,
                            f"Отсутствуют заголовки: "
                            f"{sorted(missing_headers)}; "
                            f"лишние заголовки: "
                            f"{sorted(unexpected_headers)}; "
                            f"фактические заголовки: {df.columns}",
                        )
                    )
                    continue

                try:
                    df.foreachPartition(
                        lambda rows: sum(1 for _ in rows)
                    )
                except Exception as e:
                    invalid_files.append((file_path, str(e)))
                    continue

                logger.info(
                    f"Файл прошёл валидацию: {file_path}"
                )

            except Exception as e:
                logger.error(
                    f"Ошибка чтения файла {file_path}: {e}"
                )
                raise

        if invalid_files:
            details = "; ".join(
                f"{path}: {error}"
                for path, error in invalid_files
            )

            logger.error(
                f"Валидация не пройдена. "
                f"Невалидных файлов: {len(invalid_files)}. "
                f"{details}"
            )

            for file_path, error in invalid_files:
                path = spark._jvm.org.apache.hadoop.fs.Path(
                    file_path
                )

                try:
                    success = fs.delete(path, False)
                except Exception as e:
                    logger.error(
                        f"Системная ошибка при удалении "
                        f"из HDFS: {e}"
                    )
                    raise RuntimeError(
                        f"Ошибка удаления невалидного файла "
                        f"{file_path}. Причина невалидности: "
                        f"{error}. Ошибка удаления: {e}"
                    ) from e

                if not success:
                    raise RuntimeError(
                        f"Не удалось удалить невалидный файл: "
                        f"{file_path}. Причина: {error}"
                    )

            raise ValueError(
                f"Валидация не пройдена. "
                f"Некорректных файлов: {len(invalid_files)}. "
                f"{details}"
            )

        logger.info(
            f"Все {len(files_to_validate)} файлов прошли валидацию."
        )

    except Exception as e:
        logger.error(
            f"Ошибка в процессе валидации Spark: {e}"
        )
        raise RuntimeError(
            f"Ошибка в процессе валидации Spark: {e}"
        ) from e

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
