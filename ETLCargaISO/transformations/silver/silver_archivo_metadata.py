"""
Silver: Mapeo de metadata ISO 20022 al esquema dbo.Archivo.
Columnas: idArchivo, correlationId, idCliente, numeroCliente, nombre,
          canal, tipoProceso, cantidadOperaciones, montoTotal,
          fechaProceso, fechaCreacion, fechaModificacion,
          codigoEstadoProceso, hashArchivo
"""
from pyspark import pipelines as dp
from transformations.helpers.xml_parser import extract_archivo_metadata


@dp.temporary_view(
    name="silver_archivo_metadata",
    comment="Metadata ISO 20022 mapeada al esquema dbo.Archivo"
)
def silver_archivo_metadata():
    return extract_archivo_metadata(
        spark.readStream.table("bronze_iso_xml_raw")
    )
