"""Flujo principal del pipeline Databricks para persistir metadata y transferencias."""

from pyspark import pipelines as dp

from transformations.helpers.utils import (
    DEFAULT_STORAGE_IN,
    DEFAULT_STORAGE_PROC,
    build_runtime_config,
    build_sql_config,
    build_storage_path,
    get_token,
    jdbc_reader,
    move_file,
    send_service_bus_notification,
    write_jdbc,
)
from transformations.helpers.xml_parser import build_transferencias

# =============================================================================
# Configuracion
# =============================================================================
STORAGE_PATH = spark.conf.get("storagePath")
# Ruta de la carpeta del storage para los archivos por procesar
STORAGE_PATH_IN = build_storage_path(STORAGE_PATH, DEFAULT_STORAGE_IN)
# Ruta de la carpeta del storage para los archivos procesados
STORAGE_PATH_PROCESSED = build_storage_path(STORAGE_PATH, DEFAULT_STORAGE_PROC)
# Nombre del credencial para Azure SQL y Service Bus (Managed Identity)
SERVICE_CREDENTIAL_NAME = "bncr-dati-conector"
# Nombre del servidor Azure SQL
SQL_SERVER = spark.conf.get("sqlserver")
# Nombre de la base de datos
DATABASE = spark.conf.get("database")
SQL_CONFIG = build_sql_config(SQL_SERVER, DATABASE)
JDBC_URL = SQL_CONFIG["jdbc_url"]
SQL_DRIVER = SQL_CONFIG["sql_driver"]
JDBC_TABLE_FILE = SQL_CONFIG["table_file"]
JDBC_TABLE_TX = SQL_CONFIG["table_tx"]
RUNTIME_CONFIG = build_runtime_config(spark.conf.get("topicsEndPoint"))
SB_TOPIC_ENDPOINT = RUNTIME_CONFIG["sb_topic_endpoint"]
IDP_BD = RUNTIME_CONFIG["idp_bd"]
IDP_SB = RUNTIME_CONFIG["idp_sb"]


# =============================================================================
# Sink: Escritura a Azure SQL Database (dbo.Archivo, dbo.Transferencia)
# Usa ForEachBatch para enviar cada micro-batch al servidor SQL.
# Tras escritura exitosa, envía mensaje al topic de Service Bus.
# =============================================================================
@dp.foreach_batch_sink(name="sql_archivo_sink")
def sql_archivo_sink(df, batch_id):
    del batch_id

    # 1. Obtener credential y token fresco dentro del batch (serializable)
    service_credential = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
    sql_token = get_token(IDP_BD, service_credential)

    # 2. Filtrar registros ya existentes en dbo.Archivo (evita PK duplicada)
    existing_ids = jdbc_reader(
        spark,
        JDBC_URL,
        SQL_DRIVER,
        sql_token.token,
        "(SELECT idArchivo FROM dbo.Archivo) AS existing",
    ).load()
    df = df.join(existing_ids, "idArchivo", "left_anti")

    if df.isEmpty():
        return

    # Materializar para evitar re-evaluación del left_anti join.
    df = df.cache()
    try:
        # 3. Escribir metadata a dbo.Archivo (sin xml_content)
        write_jdbc(
            df.drop("xml_content"),
            JDBC_URL,
            SQL_DRIVER,
            JDBC_TABLE_FILE,
            sql_token.token,
        )

        # 4. Construir y escribir transacciones (débito + créditos) a dbo.Transferencia
        transferencias = build_transferencias(df)
        write_jdbc(
            transferencias,
            JDBC_URL,
            SQL_DRIVER,
            JDBC_TABLE_TX,
            sql_token.token,
        )

        # 5. Notificar a Service Bus por cada archivo procesado
        files_processed = df.select("nombre", "correlationId").distinct().collect()
        sb_token = get_token(IDP_SB, service_credential)
        for row in files_processed:
            send_service_bus_notification(
                SB_TOPIC_ENDPOINT,
                row["nombre"],
                row["correlationId"],
                sb_token.token,
            )
            move_file(STORAGE_PATH_IN, STORAGE_PATH_PROCESSED, row["nombre"], dbutils)
    finally:
        # Liberar cache aun cuando falle escritura o notificación.
        df.unpersist()


@dp.append_flow(target="sql_archivo_sink")
def flow_archivo_to_sql():
    return spark.readStream.table("silver_archivo_metadata")
