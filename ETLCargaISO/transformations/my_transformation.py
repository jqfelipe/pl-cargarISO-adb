from pyspark import pipelines as dp
from datetime import datetime, timezone
import urllib.request
import json
from transformations.helpers.xml_parser import build_transferencias

# =============================================================================
# Configuración
# =============================================================================
# Parámetros del pipeline (Pipeline Settings > Configuration)
STORAGE_PATH_IN = f"{spark.conf.get('storagePath')}/inbound"
STORAGE_PATH_PROCESSED = f"{spark.conf.get('storagePath')}/processed"


# Azure SQL y Service Bus - autenticación via Service Credential (Managed Identity)
_service_credential = dbutils.credentials.getServiceCredentialsProvider('bncr-dati-conector')


def _get_fresh_token(resource):
    """Obtiene token OAuth fresco. Encapsula dbutils para serialización del sink."""
    return _service_credential.get_token(resource)

# Azure SQL - servidor y base de datos desde parámetros del pipeline
_sql_server = spark.conf.get("sqlserver")
_database = spark.conf.get("database")
JDBC_URL = (
    f"jdbc:sqlserver://{_sql_server};"
    f"database={_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30"
)
JDBC_TABLE = "dbo.Archivo"
# Azure Service Bus - endpoint desde parámetros del pipeline
SB_TOPIC_ENDPOINT = spark.conf.get("topicsEndPoint").rstrip("/")


# =============================================================================
# Helper: Notificación a Azure Service Bus
# Envía mensaje al topic tras escritura exitosa en SQL vía REST API.
# Autenticación via Bearer token (OAuth) del Service Credential.
# =============================================================================
def _send_service_bus_notification(file_name, correlation_id, sb_access_token):
    """Envía notificación al topic de Azure Service Bus."""
    payload = json.dumps({
        "nombreArchivo": file_name,
        "correlationId": correlation_id,
        "estado": "procesado",
        "fechaProceso": datetime.now(timezone.utc).isoformat(),
    })

    send_url = f"{SB_TOPIC_ENDPOINT}/messages"

    req = urllib.request.Request(
        send_url,
        data=payload.encode("utf-8"),
        headers={
            "Authorization": f"Bearer {sb_access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    urllib.request.urlopen(req)


# =============================================================================
# Sink: Escritura a Azure SQL Database (dbo.Archivo, dbo.Transferencia)
# Usa ForEachBatch para enviar cada micro-batch al servidor SQL.
# Tras escritura exitosa, envía mensaje al topic de Service Bus.
# =============================================================================
@dp.foreach_batch_sink(name="sql_archivo_sink")
def sql_archivo_sink(df, batch_id):
    # 1. Obtener token fresco via helper serializable (evita closure no serializable)
    _token = _get_fresh_token("https://database.windows.net/.default")

    # 2. Filtrar registros ya existentes en dbo.Archivo (evita PK duplicada)
    existing_ids = (df.sparkSession.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "(SELECT idArchivo FROM dbo.Archivo) AS existing")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", _token.token)
        .load()
    )
    df = df.join(existing_ids, "idArchivo", "left_anti")

    if df.isEmpty():
        return

    # Materializar para evitar re-evaluación del left_anti join
    # (sin cache, el paso de Transferencia re-lee dbo.Archivo y
    #  encuentra el registro recién insertado, filtrándolo)
    df = df.cache()

    # 3. Escribir metadata a dbo.Archivo (sin xml_content)
    (df.drop("xml_content").write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", JDBC_TABLE)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", _token.token)
        .mode("append")
        .save())

    # 4. Construir y escribir transacciones (débito + créditos) a dbo.Transferencia
    transferencias = build_transferencias(df)
    (transferencias.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbo.Transferencia")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", _token.token)
        .mode("append")
        .save())

    # 5. Notificar a Service Bus por cada archivo procesado
    sb_token = _get_fresh_token("https://servicebus.azure.net/.default")
    files_processed = df.select("nombre", "correlationId").distinct().collect()
    for row in files_processed:
        _send_service_bus_notification(row["nombre"], row["correlationId"], sb_token.token)

    # 6. Trasladar archivos procesados de inbound a processed
    for row in files_processed:
        src = f"{STORAGE_PATH_IN}/{row['nombre']}"
        dst = f"{STORAGE_PATH_PROCESSED}/{row['nombre']}"
        dbutils.fs.mv(src, dst)

    # Liberar cache
    df.unpersist()


@dp.append_flow(target="sql_archivo_sink")
def flow_archivo_to_sql():
    return spark.readStream.table("silver_archivo_metadata")
