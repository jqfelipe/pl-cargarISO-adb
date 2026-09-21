import urllib.request
import json
from datetime import datetime, timezone

DEFAULT_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
DEFAULT_JDBC_TABLE_FILE = "dbo.Archivo"
DEFAULT_JDBC_TABLE_TX = "dbo.Transferencia"
DEFAULT_IDP_BD = "https://database.windows.net/.default"
DEFAULT_IDP_SB = "https://servicebus.azure.net/.default"
DEFAULT_STORAGE_IN = "/inbound"
DEFAULT_STORAGE_PROC = "/processed"


def build_jdbc_url(sql_server, database):
    """Construye el JDBC URL para Azure SQL."""
    return (
        f"jdbc:sqlserver://{sql_server};"
        f"database={database};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30"
    )


def build_sql_config(sql_server, database):
    """Construye configuración SQL centralizada para el pipeline."""
    return {
        "jdbc_url": build_jdbc_url(sql_server, database),
        "sql_driver": DEFAULT_SQL_DRIVER,
        "table_file": DEFAULT_JDBC_TABLE_FILE,
        "table_tx": DEFAULT_JDBC_TABLE_TX,
    }


def build_runtime_config(topics_endpoint):
    """Construye configuración de runtime para autenticación y mensajería."""
    return {
        "sb_topic_endpoint": topics_endpoint.rstrip("/"),
        "idp_bd": DEFAULT_IDP_BD,
        "idp_sb": DEFAULT_IDP_SB,
    }


def build_storage_path(storage_path, file_path):
    """Construye una ruta de storage a partir del path base y su sufijo."""
    return f"{storage_path}{file_path}"


def get_token(resource, service_credential):
    """Obtiene token OAuth fresco. Encapsula dbutils para serialización del sink."""
    return service_credential.get_token(resource)


def jdbc_reader(spark, jdbc_url, sql_driver, access_token, dbtable):
    """Retorna un DataFrameReader JDBC preconfigurado para Azure SQL."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("driver", sql_driver)
        .option("accessToken", access_token)
    )


def write_jdbc(df, jdbc_url, sql_driver, table_name, access_token):
    """Escribe un DataFrame en Azure SQL usando append."""
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("driver", sql_driver)
        .option("accessToken", access_token)
        .mode("append")
        .save()
    )

# =============================================================================
# Helper: Notificación a Azure Service Bus
# Envía mensaje al topic tras escritura exitosa en SQL vía REST API.
# Autenticación via Bearer token (OAuth) del Service Credential.
# =============================================================================
def send_service_bus_notification(sb_topic_endpoint, file_name, correlation_id, sb_access_token):
    """Envía notificación al topic de Azure Service Bus."""
    payload = json.dumps({
        "nombreArchivo": file_name,
        "correlationId": correlation_id,
        "estado": "procesado",
        "fechaProceso": datetime.now(timezone.utc).isoformat(),
    })

    send_url = f"{sb_topic_endpoint}/messages"

    req = urllib.request.Request(
        send_url,
        data=payload.encode("utf-8"),
        headers={
            "Authorization": f"Bearer {sb_access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15):
        pass


def move_file(origin, dest, file_name):
    """Mueve archivo a carpeta de archivos procesados."""
    src = f"{origin}/{file_name}"
    dst = f"{dest}/{file_name}"
    dbutils.fs.mv(src, dst)
