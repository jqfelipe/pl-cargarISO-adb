import json
from datetime import datetime

from transformations.helpers import utils


def test_build_jdbc_url_returns_expected_sqlserver_connection_string():
    jdbc_url = utils.build_jdbc_url("mi-servidor.database.windows.net", "mi_db")

    assert jdbc_url == (
        "jdbc:sqlserver://mi-servidor.database.windows.net;"
        "database=mi_db;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30"
    )


def test_build_sql_config_returns_expected_defaults():
    sql_config = utils.build_sql_config("mi-servidor.database.windows.net", "mi_db")

    assert sql_config["jdbc_url"] == (
        "jdbc:sqlserver://mi-servidor.database.windows.net;"
        "database=mi_db;"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30"
    )
    assert sql_config["sql_driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert sql_config["table_file"] == "dbo.Archivo"
    assert sql_config["table_tx"] == "dbo.Transferencia"


def test_build_runtime_config_normalizes_endpoint_and_scopes():
    runtime = utils.build_runtime_config("https://namespace.servicebus.windows.net/topic/")

    assert runtime["sb_topic_endpoint"] == "https://namespace.servicebus.windows.net/topic"
    assert runtime["idp_bd"] == "https://database.windows.net/.default"
    assert runtime["idp_sb"] == "https://servicebus.azure.net/.default"


def test_build_runtime_config_keeps_endpoint_without_trailing_slash():
    runtime = utils.build_runtime_config("https://namespace.servicebus.windows.net/topic")

    assert runtime["sb_topic_endpoint"] == "https://namespace.servicebus.windows.net/topic"


def test_build_storage_path_returns_expected_path():
    base_path = "abfss://landing@sa.dfs.core.windows.net/root"

    inbound = utils.build_storage_path(base_path, utils.DEFAULT_STORAGE_IN)
    processed = utils.build_storage_path(base_path, utils.DEFAULT_STORAGE_PROC)

    assert inbound == "abfss://landing@sa.dfs.core.windows.net/root/inbound"
    assert processed == "abfss://landing@sa.dfs.core.windows.net/root/processed"


class _FakeServiceCredential:
    def __init__(self):
        self.requested_resource = None

    def get_token(self, resource):
        self.requested_resource = resource
        return {"token": "abc"}


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def test_get_token_delegates_to_service_credential():
    credential = _FakeServiceCredential()

    token = utils.get_token("scope://example", credential)

    assert credential.requested_resource == "scope://example"
    assert token == {"token": "abc"}


class _FakeReader:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self


class _FakeSpark:
    def __init__(self):
        self.read = _FakeReader()


class _FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def save(self):
        self.calls.append(("save",))


class _FakeDataFrame:
    def __init__(self):
        self.write = _FakeWriter()


def test_jdbc_reader_applies_expected_options():
    spark = _FakeSpark()

    reader = utils.jdbc_reader(
        spark,
        "jdbc:sqlserver://server;database=db",
        "driver.class",
        "token-123",
        "dbo.Tabla",
    )

    assert reader is spark.read
    assert spark.read.calls == [
        ("format", "jdbc"),
        ("option", "url", "jdbc:sqlserver://server;database=db"),
        ("option", "dbtable", "dbo.Tabla"),
        ("option", "driver", "driver.class"),
        ("option", "accessToken", "token-123"),
    ]


def test_write_jdbc_uses_append_mode_and_saves():
    df = _FakeDataFrame()

    utils.write_jdbc(
        df,
        "jdbc:sqlserver://server;database=db",
        "driver.class",
        "dbo.Tabla",
        "token-123",
    )

    assert df.write.calls == [
        ("format", "jdbc"),
        ("option", "url", "jdbc:sqlserver://server;database=db"),
        ("option", "dbtable", "dbo.Tabla"),
        ("option", "driver", "driver.class"),
        ("option", "accessToken", "token-123"),
        ("mode", "append"),
        ("save",),
    ]


def test_send_service_bus_notification_posts_expected_payload(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["method"] = request.get_method()
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data.decode("utf-8")
        return _FakeResponse()

    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    utils.send_service_bus_notification(
        "https://namespace.servicebus.windows.net/topic",
        "archivo.xml",
        "corr-001",
        "token-123",
    )

    assert captured["url"] == "https://namespace.servicebus.windows.net/topic/messages"
    assert captured["timeout"] == 15
    assert captured["method"] == "POST"
    assert captured["headers"]["Authorization"] == "Bearer token-123"
    assert captured["headers"]["Content-type"] == "application/json"
    assert '"nombreArchivo": "archivo.xml"' in captured["body"]
    assert '"correlationId": "corr-001"' in captured["body"]
    assert '"estado": "procesado"' in captured["body"]


def test_send_service_bus_notification_sets_timezone_aware_iso_timestamp(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = request.data.decode("utf-8")
        return _FakeResponse()

    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    utils.send_service_bus_notification(
        "https://namespace.servicebus.windows.net/topic",
        "archivo.xml",
        "corr-001",
        "token-123",
    )

    payload = json.loads(captured["body"])
    parsed_timestamp = datetime.fromisoformat(payload["fechaProceso"])

    assert parsed_timestamp.tzinfo is not None


def test_move_file_calls_dbutils_fs_mv(monkeypatch):
    calls = []

    class _FakeFs:
        @staticmethod
        def mv(src, dst):
            calls.append((src, dst))

    class _FakeDbutils:
        fs = _FakeFs()

    fake_dbutils = _FakeDbutils()

    utils.move_file(
        "abfss://landing/inbound",
        "abfss://landing/processed",
        "a.xml",
        fake_dbutils,
    )

    assert calls == [(
        "abfss://landing/inbound/a.xml",
        "abfss://landing/processed/a.xml",
    )]
