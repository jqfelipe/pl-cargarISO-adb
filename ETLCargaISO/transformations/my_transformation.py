from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime, timezone
import urllib.request
import json

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
# Bronze: Ingesta de archivos XML ISO 20022 desde Azure Blob Storage
# Cada archivo se lee completo como una sola fila de texto.
# Se captura metadata del archivo via _metadata de Auto Loader.
#
# Parámetro: nombreArchivo
#   - Si se proporciona, solo se ingesta ese archivo específico.
#   - Si no se proporciona, se ingestaN todos los XML nuevos (*.xml).
#   - Configurar en Pipeline Settings > Configuration:
#     nombreArchivo = "pain001_el_colono_sinpe_09_2026.xml"
# =============================================================================
@dp.temporary_view(
    name="bronze_iso_xml_raw",
    comment="Archivos XML ISO 20022 ingestados desde Azure Blob Storage"
)
def bronze_iso_xml_raw():
    file_filter = spark.conf.get("nombreArchivo", "*.xml")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("pathGlobFilter", file_filter)
        .load(STORAGE_PATH_IN)
        .select(
            F.col("value").alias("xml_content"),
            F.col("_metadata.file_name").alias("nombreArchivo"),
            F.col("_metadata.file_path").alias("ruta_archivo"),
            F.col("_metadata.file_size").alias("tamano_archivo"),
            F.col("_metadata.file_modification_time").alias("fecha_modificacion"),
            F.current_timestamp().alias("fecha_ingesta")
        )
    )


# =============================================================================
# Silver: Mapeo de metadata ISO 20022 al esquema dbo.Archivo
# Columnas: idArchivo, correlationId, idCliente, numeroCliente, nombre,
#           canal, tipoProceso, cantidadOperaciones, montoTotal,
#           fechaProceso, fechaCreacion, fechaModificacion,
#           codigoEstadoProceso, hashArchivo
# =============================================================================
@dp.temporary_view(
    name="silver_archivo_metadata",
    comment="Metadata ISO 20022 mapeada al esquema dbo.Archivo"
)
def silver_archivo_metadata():
    return (
        spark.readStream.table("bronze_iso_xml_raw")
        .select(
            # idArchivo <- MsgId del GroupHeader
            F.regexp_extract(
                "xml_content",
                r"<(?:\w+:)?MsgId>([^<]+)</(?:\w+:)?MsgId>", 1
            ).alias("idArchivo"),
            # correlationId <- parámetro del pipeline (o UUID si no se proporciona)
            F.when(
                F.lit(spark.conf.get("correlationId", "")) != "",
                F.lit(spark.conf.get("correlationId", ""))
            ).otherwise(F.expr("uuid()")).alias("correlationId"),
            # idCliente <- Othr/Id donde Prtry = CR-CJ
            F.regexp_extract(
                "xml_content",
                r"(?s)<(?:\w+:)?Othr>\s*<(?:\w+:)?Id>([^<]+)</(?:\w+:)?Id>\s*<(?:\w+:)?SchmeNm>\s*<(?:\w+:)?Prtry>CR-CJ</(?:\w+:)?Prtry>", 1
            ).alias("idCliente"),
            # numeroCliente <- Othr/Id donde Prtry = BNCR-CLIENTE
            F.regexp_extract(
                "xml_content",
                r"(?s)<(?:\w+:)?Othr>\s*<(?:\w+:)?Id>([^<]+)</(?:\w+:)?Id>\s*<(?:\w+:)?SchmeNm>\s*<(?:\w+:)?Prtry>BNCR-CLIENTE</(?:\w+:)?Prtry>", 1
            ).cast("int").alias("numeroCliente"),
            # nombre <- nombre del archivo XML
            F.col("nombreArchivo").alias("nombre"),
            # canal <- OB (Online Banking)
            F.lit("OB").alias("canal"),
            # tipoProceso <- derivado de los IBAN del archivo:
            #   CRXX0151... = BNCR (banco propio)
            #   CRXX????... (distinto de 0151) = SINPE (interbancario)
            #   Si al menos un IBAN no es 0151, es SINPE.
            F.when(
                F.regexp_extract(
                    "xml_content",
                    r"<(?:\w+:)?IBAN>CR\d{2}(?!0151)\d{4}", 0
                ) != "",
                F.lit("SINPE")
            ).otherwise(F.lit("BNCR")).alias("tipoProceso"),
            # cantidadOperaciones <- NbOfTxs del GroupHeader
            F.regexp_extract(
                "xml_content",
                r"<(?:\w+:)?NbOfTxs>([^<]+)</(?:\w+:)?NbOfTxs>", 1
            ).cast("int").alias("cantidadOperaciones"),
            # montoTotal <- CtrlSum del GroupHeader
            F.regexp_extract(
                "xml_content",
                r"<(?:\w+:)?CtrlSum>([^<]+)</(?:\w+:)?CtrlSum>", 1
            ).cast("decimal(18,2)").alias("montoTotal"),
            # fechaProceso <- CreDtTm del GroupHeader
            F.to_timestamp(
                F.regexp_extract(
                    "xml_content",
                    r"<(?:\w+:)?CreDtTm>([^<]+)</(?:\w+:)?CreDtTm>", 1
                )
            ).alias("fechaProceso"),
            # fechaCreacion <- timestamp de ingesta
            F.current_timestamp().alias("fechaCreacion"),
            # fechaModificacion <- timestamp de ingesta
            F.current_timestamp().alias("fechaModificacion"),
            # codigoEstadoProceso <- 1 (pendiente)
            F.lit(1).alias("codigoEstadoProceso"),
            # hashArchivo <- MD5 del contenido XML
            F.md5("xml_content").alias("hashArchivo"),
            # xml_content: se pasa al sink para parseo de transacciones
            F.col("xml_content")
        )
    )


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
# Helper: Escritura de transacciones a dbo.Transferencia
# Extrae débito (1 fila por PmtInf) y créditos (1 fila por CdtTrfTxInf)
# del XML ISO 20022 pain.001 y escribe a Azure SQL.
# =============================================================================
def _write_transferencias(df, access_token):
    """Extrae transacciones (débito + créditos) del XML y escribe a dbo.Transferencia."""
    base = df.select(
        F.col("correlationId"),
        F.col("idArchivo"),
        F.col("xml_content"),
        F.regexp_extract(
            "xml_content",
            r"(?s)<(?:\w+:)?DbtrAcct>\s*<(?:\w+:)?Id>\s*<(?:\w+:)?IBAN>([^<]+)", 1
        ).alias("ibanDebito"),
        F.to_timestamp(
            F.regexp_extract("xml_content", r"<(?:\w+:)?ReqdExctnDt>([^<]+)", 1)
        ).alias("fechaAplicacion"),
    )

    # --- Filas de crédito (una por cada CdtTrfTxInf) ---
    credits = (
        base.select(
            "*",
            F.posexplode(
                F.regexp_extract_all(
                    F.col("xml_content"),
                    F.lit(r"(?s)<(?:\w+:)?CdtTrfTxInf>(.*?)</(?:\w+:)?CdtTrfTxInf>"),
                    1
                )
            ).alias("pos", "txn_xml")
        )
        .select(
            F.expr("uuid()").alias("correlationId"),
            F.col("idArchivo"),
            F.lit(2).cast("short").alias("operacion"),  # 2 = Crédito
            F.col("ibanDebito"),
            F.regexp_extract("txn_xml", r"<(?:\w+:)?IBAN>([^<]+)", 1).alias("ibanCredito"),
            F.regexp_extract("txn_xml", r"<(?:\w+:)?InstdAmt[^>]*>([^<]+)", 1)
                .cast("decimal(18,2)").alias("monto"),
            F.regexp_extract("txn_xml", r'Ccy="([^"]+)"', 1).alias("moneda"),
            F.regexp_extract("txn_xml", r"<(?:\w+:)?EndToEndId>([^<]+)", 1)
                .alias("referenciaCliente"),
            F.lit(None).cast("string").alias("referenciaBanco"),  # se asigna en proceso posterior
            F.col("fechaAplicacion"),
            F.current_timestamp().alias("fechaCreacion"),
            F.lit(None).cast("timestamp").alias("fechaModificacion"),  # se ajusta en proceso posterior
            F.lit(None).cast("double").alias("comprobante"),  # se asigna en proceso posterior
            F.regexp_extract("txn_xml", r"<(?:\w+:)?Ustrd>([^<]+)", 1).alias("detalle"),
            (F.col("pos") + 1).cast("int").alias("numeroLinea"),
            F.lit(1).alias("codigoEstadoProceso"),  # 1 = ingesta
            F.lit(1).alias("codigoEstadoTransferencia"),  # 1 = estado inicial ingesta
        )
    )

    # --- Fila de débito (una por archivo/PmtInf) ---
    debits = base.select(
        F.expr("uuid()").alias("correlationId"),
        F.col("idArchivo"),
        F.lit(1).cast("short").alias("operacion"),  # 1 = Débito
        F.col("ibanDebito"),
        # TODO: ibanCredito para débito - ¿Mismo IBAN deudor? ¿Vacío?
        F.col("ibanDebito").alias("ibanCredito"),
        F.regexp_extract("xml_content", r"<(?:\w+:)?CtrlSum>([^<]+)", 1)
            .cast("decimal(18,2)").alias("monto"),
        F.regexp_extract("xml_content", r'Ccy="([^"]+)"', 1).alias("moneda"),
        # TODO: referenciaCliente para débito - ¿PmtInfId?
        F.regexp_extract("xml_content", r"<(?:\w+:)?PmtInfId>([^<]+)", 1)
            .alias("referenciaCliente"),
        F.lit(None).cast("string").alias("referenciaBanco"),  # se asigna en proceso posterior
        F.col("fechaAplicacion"),
        F.current_timestamp().alias("fechaCreacion"),
        F.lit(None).cast("timestamp").alias("fechaModificacion"),  # se ajusta en proceso posterior
        F.lit(None).cast("double").alias("comprobante"),  # se asigna en proceso posterior
        # TODO: detalle para débito - ¿Texto descriptivo?
        F.lit(None).cast("string").alias("detalle"),
        F.lit(0).cast("int").alias("numeroLinea"),
        F.lit(1).alias("codigoEstadoProceso"),  # 1 = ingesta
        F.lit(1).alias("codigoEstadoTransferencia"),  # 1 = estado inicial ingesta
    )

    # Combinar débito + créditos y escribir
    transferencias = debits.unionByName(credits)

    (transferencias.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dbo.Transferencia")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", access_token)
        .mode("append")
        .save())


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

    # 4. Escribir transacciones (débito + créditos) a dbo.Transferencia
    _write_transferencias(df, _token.token)

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
