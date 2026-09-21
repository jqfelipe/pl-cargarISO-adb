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


def test_send_service_bus_notification_posts_expected_payload(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
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
    assert captured["headers"]["Authorization"] == "Bearer token-123"
    assert captured["headers"]["Content-type"] == "application/json"
    assert '"nombreArchivo": "archivo.xml"' in captured["body"]
    assert '"correlationId": "corr-001"' in captured["body"]
    assert '"estado": "procesado"' in captured["body"]


def test_move_file_calls_dbutils_fs_mv(monkeypatch):
    calls = []

    class _FakeFs:
        @staticmethod
        def mv(src, dst):
            calls.append((src, dst))

    class _FakeDbutils:
        fs = _FakeFs()

    monkeypatch.setattr(utils, "dbutils", _FakeDbutils(), raising=False)

    utils.move_file("abfss://landing/inbound", "abfss://landing/processed", "a.xml")

    assert calls == [(
        "abfss://landing/inbound/a.xml",
        "abfss://landing/processed/a.xml",
    )]
