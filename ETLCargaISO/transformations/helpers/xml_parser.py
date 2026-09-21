"""
Funciones puras de transformación para archivos ISO 20022 (pain.001).
Independientes del runtime del pipeline — reciben y retornan DataFrames.
"""
from pyspark.sql import functions as F


def extract_archivo_metadata(df):
    """
    Extrae metadata ISO 20022 del XML y la mapea al esquema dbo.Archivo.

    Args:
        df: DataFrame con columnas 'xml_content' y 'nombreArchivo'

    Returns:
        DataFrame con columnas del esquema dbo.Archivo + xml_content
    """
    return df.select(
        # idArchivo <- MsgId del GroupHeader
        F.regexp_extract(
            "xml_content",
            r"<(?:\w+:)?MsgId>([^<]+)</(?:\w+:)?MsgId>", 1
        ).alias("idArchivo"),
        # correlationId <- UUID único por archivo
        F.expr("uuid()").alias("correlationId"),
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


def build_transferencias(df):
    """
    Extrae transacciones (débito + créditos) del XML ISO 20022 pain.001.

    Args:
        df: DataFrame con columnas 'correlationId', 'idArchivo', 'xml_content'

    Returns:
        DataFrame con esquema dbo.Transferencia (débitos + créditos)
    """
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

    return debits.unionByName(credits)
