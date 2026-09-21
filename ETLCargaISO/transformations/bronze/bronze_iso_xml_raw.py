"""
Bronze: Ingesta de archivos XML ISO 20022 desde Azure Blob Storage.
Cada archivo se lee completo como una sola fila de texto.
Se captura metadata del archivo via _metadata de Auto Loader.

Parámetro: nombreArchivo
  - Si se proporciona, solo se ingesta ese archivo específico.
  - Si no se proporciona, se ingestan todos los XML nuevos (*.xml).
  - Configurar en Pipeline Settings > Configuration:
    nombreArchivo = "pain001_el_colono_sinpe_09_2026.xml"
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations.helpers.utils import DEFAULT_STORAGE_IN, build_storage_path

STORAGE_PATH_IN = build_storage_path(spark.conf.get("storagePath"), DEFAULT_STORAGE_IN)



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
