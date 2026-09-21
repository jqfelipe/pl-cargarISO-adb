from decimal import Decimal
from pathlib import Path
import tempfile

import pytest
from pyspark.sql import SparkSession

from transformations.helpers.xml_parser import build_transferencias, extract_archivo_metadata


@pytest.fixture(scope="module")
def spark():
  local_dir = Path(tempfile.mkdtemp(prefix="spark-local-"))
  session = (
    SparkSession.builder.master("local[1]")
    .appName("test-xml-parser")
    .config("spark.local.dir", str(local_dir))
    .getOrCreate()
  )
  yield session
  session.stop()


@pytest.fixture()
def sample_xml():
    return """<Document>
  <CstmrCdtTrfInitn>
    <GrpHdr>
      <MsgId>MSG-001</MsgId>
      <CreDtTm>2026-09-18T12:30:00</CreDtTm>
      <NbOfTxs>2</NbOfTxs>
      <CtrlSum>300.50</CtrlSum>
      <InitgPty>
        <Id>
          <OrgId>
            <Othr>
              <Id>3101123456</Id>
              <SchmeNm><Prtry>CR-CJ</Prtry></SchmeNm>
            </Othr>
            <Othr>
              <Id>12345</Id>
              <SchmeNm><Prtry>BNCR-CLIENTE</Prtry></SchmeNm>
            </Othr>
          </OrgId>
        </Id>
      </InitgPty>
    </GrpHdr>
    <PmtInf>
      <PmtInfId>PMT-001</PmtInfId>
      <ReqdExctnDt>2026-09-19</ReqdExctnDt>
      <DbtrAcct>
        <Id>
          <IBAN>CR12015100010000000001</IBAN>
        </Id>
      </DbtrAcct>
      <CdtTrfTxInf>
        <PmtId><EndToEndId>E2E-001</EndToEndId></PmtId>
        <Amt><InstdAmt Ccy="CRC">100.25</InstdAmt></Amt>
        <CdtrAcct><Id><IBAN>CR44010200010000000002</IBAN></Id></CdtrAcct>
        <RmtInf><Ustrd>Pago 1</Ustrd></RmtInf>
      </CdtTrfTxInf>
      <CdtTrfTxInf>
        <PmtId><EndToEndId>E2E-002</EndToEndId></PmtId>
        <Amt><InstdAmt Ccy="CRC">200.25</InstdAmt></Amt>
        <CdtrAcct><Id><IBAN>CR44015100010000000003</IBAN></Id></CdtrAcct>
        <RmtInf><Ustrd>Pago 2</Ustrd></RmtInf>
      </CdtTrfTxInf>
    </PmtInf>
  </CstmrCdtTrfInitn>
</Document>"""


def test_extract_archivo_metadata_maps_expected_fields(spark, sample_xml):
    source_df = spark.createDataFrame(
        [(sample_xml, "pain001_test.xml")],
        ["xml_content", "nombreArchivo"],
    )

    result = extract_archivo_metadata(source_df).collect()[0]

    assert result["idArchivo"] == "MSG-001"
    assert result["correlationId"] is not None
    assert result["correlationId"] != ""
    assert result["idCliente"] == "3101123456"
    assert result["numeroCliente"] == 12345
    assert result["nombre"] == "pain001_test.xml"
    assert result["canal"] == "OB"
    assert result["tipoProceso"] == "SINPE"
    assert result["cantidadOperaciones"] == 2
    assert result["montoTotal"] == Decimal("300.50")
    assert str(result["fechaProceso"]) == "2026-09-18 12:30:00"
    assert result["codigoEstadoProceso"] == 1
    assert result["hashArchivo"] is not None
    assert result["xml_content"] == sample_xml


def test_build_transferencias_returns_one_debit_and_two_credits(spark, sample_xml):
    source_df = spark.createDataFrame(
        [("corr-123", "MSG-001", sample_xml)],
        ["correlationId", "idArchivo", "xml_content"],
    )

    rows = build_transferencias(source_df).collect()

    assert len(rows) == 3

    debit = next(row for row in rows if row["operacion"] == 1)
    assert debit["operacion"] == 1
    assert debit["idArchivo"] == "MSG-001"
    assert debit["ibanDebito"] == "CR12015100010000000001"
    assert debit["ibanCredito"] == "CR12015100010000000001"
    assert debit["monto"] == Decimal("300.50")
    assert debit["moneda"] == "CRC"
    assert debit["referenciaCliente"] == "PMT-001"
    assert debit["numeroLinea"] == 0
    assert debit["codigoEstadoProceso"] == 1
    assert debit["codigoEstadoTransferencia"] == 1

    credits = sorted(
      [row for row in rows if row["operacion"] == 2],
      key=lambda row: row["numeroLinea"],
    )

    first_credit = credits[0]
    assert first_credit["operacion"] == 2
    assert first_credit["ibanCredito"] == "CR44010200010000000002"
    assert first_credit["monto"] == Decimal("100.25")
    assert first_credit["referenciaCliente"] == "E2E-001"
    assert first_credit["detalle"] == "Pago 1"
    assert first_credit["numeroLinea"] == 1

    second_credit = credits[1]
    assert second_credit["operacion"] == 2
    assert second_credit["ibanCredito"] == "CR44015100010000000003"
    assert second_credit["monto"] == Decimal("200.25")
    assert second_credit["referenciaCliente"] == "E2E-002"
    assert second_credit["detalle"] == "Pago 2"
    assert second_credit["numeroLinea"] == 2


def test_extract_archivo_metadata_generates_correlation_id_when_empty(spark, sample_xml):
    source_df = spark.createDataFrame(
        [(sample_xml, "pain001_test.xml")],
        ["xml_content", "nombreArchivo"],
    )

    result = extract_archivo_metadata(source_df).collect()[0]

    assert result["correlationId"] is not None
    assert result["correlationId"] != ""


def test_extract_archivo_metadata_sets_tipo_proceso_bncr_when_all_ibans_are_0151(spark):
    xml_bncr = """<Document>
  <CstmrCdtTrfInitn>
    <GrpHdr>
      <MsgId>MSG-002</MsgId>
      <CreDtTm>2026-09-18T12:30:00</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <CtrlSum>10.00</CtrlSum>
      <InitgPty>
        <Id>
          <OrgId>
            <Othr>
              <Id>3101123456</Id>
              <SchmeNm><Prtry>CR-CJ</Prtry></SchmeNm>
            </Othr>
            <Othr>
              <Id>12345</Id>
              <SchmeNm><Prtry>BNCR-CLIENTE</Prtry></SchmeNm>
            </Othr>
          </OrgId>
        </Id>
      </InitgPty>
    </GrpHdr>
    <PmtInf>
      <PmtInfId>PMT-002</PmtInfId>
      <ReqdExctnDt>2026-09-19</ReqdExctnDt>
      <DbtrAcct><Id><IBAN>CR12015100010000000001</IBAN></Id></DbtrAcct>
      <CdtTrfTxInf>
        <PmtId><EndToEndId>E2E-003</EndToEndId></PmtId>
        <Amt><InstdAmt Ccy="CRC">10.00</InstdAmt></Amt>
        <CdtrAcct><Id><IBAN>CR44015100010000000003</IBAN></Id></CdtrAcct>
      </CdtTrfTxInf>
    </PmtInf>
  </CstmrCdtTrfInitn>
</Document>"""

    source_df = spark.createDataFrame(
        [(xml_bncr, "pain001_bncr.xml")],
        ["xml_content", "nombreArchivo"],
    )

    result = extract_archivo_metadata(source_df).collect()[0]

    assert result["tipoProceso"] == "BNCR"
