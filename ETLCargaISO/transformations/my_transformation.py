from pyspark import pipelines as dp
from transformations.helpers.xml_parser import build_transferencias
from transformations.helpers.utils import get_token, send_service_bus_notification, move_file

# =============================================================================
# Configuracion
# =============================================================================
# Ruta de la carpeta del storage para los archivos por procesar
STORAGE_PATH_IN = f"{spark.conf.get('storagePath')}/inbound"
# Ruta de la carpeta del storage para los archivos procesados
STORAGE_PATH_PROCESSED = f"{spark.conf.get('storagePath')}/processed"
# Azure SQL y Service Bus - autenticacion via Service Credential (Managed Identity)
_service_credential = dbutils.credentials.getServiceCredentialsProvider('bncr-dati-conector')
# Nombre del servidor Azure SQL
_sql_server = spark.conf.get("sqlserver")
# Nombre de la base de datos
_database = spark.conf.get("database")
# Configuracion del URL del servidor Azure SQL
JDBC_URL = (
    f"jdbc:sqlserver://{_sql_server};"
    f"database={_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30"
)
# Driver para conectarse a la base de datos
SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# Tabla SQL para salvar la metadata del archivo
JDBC_TABLE_FILE = "dbo.Archivo"
# Tabla SQL para transacciones
JDBC_TABLE_TX = "dbo.Transferencia"
# Azure Service Bus - endpoint desde parámetros del pipeline
SB_TOPIC_ENDPOINT = spark.conf.get("topicsEndPoint").rstrip("/")
# IDP Azure BD
IDP_BD = "https://database.windows.net/.default"
# IDP Azure Service Bus
IDP_SB = "https://servicebus.azure.net/.default"


# =============================================================================
# Sink: Escritura a Azure SQL Database (dbo.Archivo, dbo.Transferencia)
# Usa ForEachBatch para enviar cada micro-batch al servidor SQL.
# Tras escritura exitosa, envía mensaje al topic de Service Bus.
# =============================================================================
@dp.foreach_batch_sink(name="sql_archivo_sink")
def sql_archivo_sink(df, batch_id):
    # 1. Obtener token fresco via helper serializable (evita closure no serializable)
    _token = get_token(IDP_BD, _service_credential)

    # 2. Filtrar registros ya existentes en dbo.Archivo (evita PK duplicada)
    existing_ids = (df.sparkSession.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "(SELECT idArchivo FROM dbo.Archivo) AS existing")
        .option("driver", SQL_DRIVER)
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
        .option("dbtable", JDBC_TABLE_FILE)
        .option("driver", SQL_DRIVER)
        .option("accessToken", _token.token)
        .mode("append")
        .save())

    # 4. Construir y escribir transacciones (débito + créditos) a dbo.Transferencia
    transferencias = build_transferencias(df)
    (transferencias.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", JDBC_TABLE_TX)
        .option("driver", SQL_DRIVER)
        .option("accessToken", _token.token)
        .mode("append")
        .save())

    # 5. Notificar a Service Bus por cada archivo procesado
    sb_token = get_token(IDP_SB, _service_credential)
    files_processed = df.select("nombre", "correlationId").distinct().collect()
    for row in files_processed:
        send_service_bus_notification(SB_TOPIC_ENDPOINT, row["nombre"], row["correlationId"], sb_token.token)

    # 6. Trasladar archivos procesados de inbound a processed
    for row in files_processed:
        move_file(STORAGE_PATH_IN, STORAGE_PATH_PROCESSED, row['nombre'])

    # Liberar cache
    df.unpersist()


@dp.append_flow(target="sql_archivo_sink")
def flow_archivo_to_sql():
    return spark.readStream.table("silver_archivo_metadata")
