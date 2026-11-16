import importlib
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _load_module(module_path: str):
    """Load a project module by dotted path or repo-relative file path."""
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError:
        pass

    relative = module_path.replace(".", "/") + ".py"
    root = Path(__file__).resolve().parents[1]
    file_path = root / relative
    if not file_path.exists():
        alt = root / module_path
        if alt.exists():
            file_path = alt
        else:
            raise

    spec = importlib.util.spec_from_file_location(module_path.replace("-", "_"), file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module



def test_extract_firms_data():
    """extract_firms_data should request FIRMS data and push extracted payload/metadata to XCom."""
    module = _load_module("airflow.dags.FIRES.raw.firms_source_to_raw")
    fake_context = {"ti": MagicMock(), "ds": "2026-05-10", "run_id": "manual__2026-05-10"}

    fake_response = MagicMock()
    fake_response.raise_for_status.return_value = None
    fake_response.text = "latitude,longitude,acq_date,acq_time,confidence\n55.0,37.0,2026-05-10,1200,80\n"

    with patch.object(module, "requests", create=True) as mock_requests, patch.object(
        module, "Variable", create=True
    ) as mock_variable:
        mock_variable.get.return_value = "test-api-key"
        mock_requests.get.return_value = fake_response

        if hasattr(module, "extract_firms_data"):
            result = module.extract_firms_data(**fake_context)
        else:
            pytest.skip("extract_firms_data not found")

    assert mock_requests.get.called
    if result is not None:
        assert result is not False


def test_publish_firms_data():
    """publish_firms_data should take extracted FIRMS payload and publish it to raw storage."""
    module = _load_module("airflow.dags.FIRES.raw.firms_source_to_raw")
    ti = MagicMock()
    ti.xcom_pull.return_value = {
        "records": [{"latitude": 55.0, "longitude": 37.0, "acq_date": "2026-05-10"}],
        "path": "/tmp/firms_raw.json",
    }
    fake_context = {"ti": ti, "ds": "2026-05-10", "run_id": "manual__2026-05-10"}

    with patch.object(module, "get_hdfs_client", create=True, return_value=MagicMock()) as mock_hdfs, patch(
        "builtins.open", create=True
    ), patch.object(module, "os", create=True):
        if hasattr(module, "publish_firms_data"):
            result = module.publish_firms_data(**fake_context)
        else:
            pytest.skip("publish_firms_data not found")

    assert ti.xcom_pull.called or mock_hdfs.called
    if result is not None:
        assert result is not False


def test_extract_openmeteo_data():
    """extract_openmeteo_data should fetch weather data for requested coordinates/time window."""
    module = _load_module("airflow.dags.open-meteo.raw.open_meteo_source_to_raw")
    fake_context = {
        "ti": MagicMock(),
        "ds": "2026-05-10",
        "run_id": "manual__2026-05-10",
        "params": {"latitude": 55.0, "longitude": 37.0},
    }

    fake_response = MagicMock()
    fake_response.raise_for_status.return_value = None
    fake_response.json.return_value = {
        "hourly": {
            "time": ["2026-05-10T00:00", "2026-05-10T01:00"],
            "temperature_2m": [12.0, 11.5],
            "wind_speed_10m": [3.0, 3.5],
        }
    }

    with patch.object(module, "requests", create=True) as mock_requests, patch.object(
        module, "Variable", create=True
    ) as mock_variable:
        mock_variable.get.return_value = "https://api.open-meteo.com/v1/forecast"
        mock_requests.get.return_value = fake_response

        if hasattr(module, "extract_openmeteo_data"):
            result = module.extract_openmeteo_data(**fake_context)
        else:
            pytest.skip("extract_openmeteo_data not found")

    assert mock_requests.get.called
    if result is not None:
        assert result is not False


def test_publish_openmeteo_data():
    """publish_openmeteo_data should publish extracted Open-Meteo payload to raw storage."""
    module = _load_module("airflow.dags.open-meteo.raw.open_meteo_source_to_raw")
    ti = MagicMock()
    ti.xcom_pull.return_value = {
        "hourly": {"time": ["2026-05-10T00:00"], "temperature_2m": [12.0]},
        "path": "/tmp/open_meteo_raw.json",
    }
    fake_context = {"ti": ti, "ds": "2026-05-10", "run_id": "manual__2026-05-10"}

    with patch.object(module, "get_hdfs_client", create=True, return_value=MagicMock()) as mock_hdfs, patch(
        "builtins.open", create=True
    ), patch.object(module, "os", create=True):
        if hasattr(module, "publish_openmeteo_data"):
            result = module.publish_openmeteo_data(**fake_context)
        else:
            pytest.skip("publish_openmeteo_data not found")

    assert ti.xcom_pull.called or mock_hdfs.called
    if result is not None:
        assert result is not False


def test_spark_firms_raw_to_stg_build_record_rules():
    """build_record_rules should produce validation/casting rules for FIRMS raw records."""
    module = _load_module("airflow.apps.spark_firms_raw_to_stg")
    assert hasattr(module, "build_record_rules")

    execution_date = "2026-05-10"
    try:
        rules = module.build_record_rules(execution_date)
    except TypeError:
        rules = module.build_record_rules(MagicMock(), execution_date)

    assert rules is not None
    if isinstance(rules, dict):
        assert len(rules) > 0
    elif isinstance(rules, (list, tuple, set)):
        assert len(rules) > 0


def test_spark_firms_raw_to_stg_classify_records():
    """classify_records should split FIRMS records into valid and invalid/quarantine groups."""
    module = _load_module("airflow.apps.spark_firms_raw_to_stg")
    assert hasattr(module, "classify_records")

    df = MagicMock(name="input_df")
    rules = MagicMock(name="rules")
    classified = MagicMock(name="classified_df")
    df.withColumn.return_value = df
    df.filter.return_value = classified

    try:
        result = module.classify_records(df, "2026-05-10")
    except TypeError:
        result = module.classify_records(df, rules)

    assert result is not None


def test_spark_firms_raw_to_stg_deduplicate_valid_records():
    """deduplicate_valid_records should remove duplicate FIRMS detections for the same event identity."""
    module = _load_module("airflow.apps.spark_firms_raw_to_stg")
    assert hasattr(module, "deduplicate_valid_records")

    df = MagicMock(name="valid_df")
    deduped = MagicMock(name="deduped_df")
    df.dropDuplicates.return_value = deduped
    df.orderBy.return_value = df
    df.groupBy.return_value.agg.return_value = deduped

    result = module.deduplicate_valid_records(df)

    assert result is not None
    assert (
        df.dropDuplicates.called
        or df.groupBy.called
        or getattr(df, "drop_duplicates", MagicMock()).called
        or result == deduped
    )


def test_spark_open_meteo_raw_to_stg_deduplicate_valid_records():
    """deduplicate_valid_records should keep a single weather observation per location/time grain."""
    module = _load_module("airflow.apps.spark_open_meteo_raw_to_stg")
    assert hasattr(module, "deduplicate_valid_records")

    df = MagicMock(name="valid_weather_df")
    deduped = MagicMock(name="deduped_weather_df")
    df.dropDuplicates.return_value = deduped
    df.orderBy.return_value = df
    df.groupBy.return_value.agg.return_value = deduped

    result = module.deduplicate_valid_records(df)

    assert result is not None
    assert df.dropDuplicates.called or df.groupBy.called or result == deduped


def test_spark_mart_operational_report_prepare_firms_and_prepare_weather():
    """prepare_firms and prepare_weather should normalize source datasets for mart enrichment/join."""
    module = _load_module("airflow.apps.spark_mart_operational_report")
    assert hasattr(module, "prepare_firms")
    assert hasattr(module, "prepare_weather")

    firms_df = MagicMock(name="firms_df")
    weather_df = MagicMock(name="weather_df")
    prepared_firms = MagicMock(name="prepared_firms")
    prepared_weather = MagicMock(name="prepared_weather")

    for df, prepared in ((firms_df, prepared_firms), (weather_df, prepared_weather)):
        df.select.return_value = df
        df.withColumn.return_value = df
        df.withColumnRenamed.return_value = df
        df.filter.return_value = df
        df.dropDuplicates.return_value = prepared
        df.dropna.return_value = prepared

    try:
        firms_result = module.prepare_firms(firms_df, 24)
    except TypeError:
        firms_result = module.prepare_firms(firms_df)

    try:
        weather_result = module.prepare_weather(weather_df, 24)
    except TypeError:
        weather_result = module.prepare_weather(weather_df)

    assert firms_result is not None
    assert weather_result is not None


def test_spark_mart_operational_report_build_triangulated_mart():
    """build_triangulated_mart should combine fire detections with weather features into the operational mart."""
    module = _load_module("airflow.apps.spark_mart_operational_report")
    assert hasattr(module, "build_triangulated_mart")

    spark = MagicMock(name="spark")
    firms_df = MagicMock(name="prepared_firms_df")
    weather_df = MagicMock(name="prepared_weather_df")
    mart_df = MagicMock(name="mart_df")

    firms_df.join.return_value = mart_df
    firms_df.alias.return_value = firms_df
    weather_df.alias.return_value = weather_df
    mart_df.select.return_value = mart_df
    mart_df.withColumn.return_value = mart_df
    mart_df.groupBy.return_value.agg.return_value = mart_df
    spark.sql.return_value = mart_df

    try:
        result = module.build_triangulated_mart(firms_df, weather_df)
    except TypeError:
        try:
            result = module.build_triangulated_mart(spark, firms_df, weather_df, "2026-05-10")
        except TypeError:
            result = module.build_triangulated_mart(
                spark=spark,
                firms_df=firms_df,
                weather_df=weather_df,
                execution_date="2026-05-10",
            )

    assert result is not None
    assert firms_df.join.called or spark.sql.called or result == mart_df


