"""
Bronze: Ingesta de archivos XML ISO 20022 desde Azure Blob Storage.
Cada archivo se lee completo como una sola fila de texto.
Se captura metadata del archivo via _metadata de Auto Loader.

Auto Loader procesa automáticamente todos los archivos XML nuevos
que aparezcan en la carpeta inbound. El checkpoint del pipeline
garantiza que cada archivo se procesa exactamente una vez.
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
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.xml")
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
