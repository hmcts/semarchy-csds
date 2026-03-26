// actions/getOffenceSummary.js
const xml2js = require("xml2js");
const path = require("path");
const { v4: uuidv4 } = require("uuid");

const {
  cleanXml,
  readJsonSafe
} = require("../utils/xmlUtils");

const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleGetOffenceSummary() {
  return async function handleGetOffenceSummary(xmlBody) {
    // ------------------------------------
    // 1. CLEAN + PARSE XML
    // ------------------------------------
    xmlBody = cleanXml(xmlBody);

    const parser = new xml2js.Parser({
      explicitArray: false,
      ignoreAttrs: false,
      tagNameProcessors: [xml2js.processors.stripPrefix]
    });

    let parsed;
    try {
      parsed = await parser.parseStringPromise(xmlBody);
    } catch (err) {
      throw new Error("Invalid XML: " + err.message);
    }

    const envelope = parsed.Envelope;
    const header = envelope.Header || {};
    const body = envelope.Body || {};

    const inboundMessageId =
      header.MessageID ||
      header["addr:MessageID"] ||
      uuidv4();

    const reqRoot = body.GetOffenceSummaryRequest;
    const inner = reqRoot.GetOffenceSummaryRequestType;

    const cjsCode = inner.CJSCode;

    // ------------------------------------
    // 2. LOOK UP OFFENCE IN PERSISTENCE
    // ------------------------------------
    const dataRoot = path.join(__dirname, "..");
    const indexFile = path.join(dataRoot, "persisted_keys/offencesIndex.json");
    const offencesDir = path.join(dataRoot, "persisted_keys/offences");

    const index = readJsonSafe(indexFile, {});
    const offenceId = index[cjsCode];

    // If offence does not exist → return empty dataset
    if (!offenceId) {
      const headerXml = buildWsaHeader({
        action: "getOffenceSummary",
        messageId: uuidv4(),
        relatesTo: inboundMessageId
      });

      const emptyBody = `
        <ns7:GetReferenceDataResponse xmlns:ns7="http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse">
          <ReferenceDataSet ReferenceDataDescription="OFFENCES">
            <ReferenceDataType>
              <ReferenceRowType />
            </ReferenceDataType>
          </ReferenceDataSet>
        </ns7:GetReferenceDataResponse>
      `;

      return soapEnvelope(headerXml, emptyBody);
    }

    const offenceFile = path.join(offencesDir, `${offenceId}.json`);
    const offence = readJsonSafe(offenceFile, null);

    if (!offence) {
      throw new Error(`Offence data missing for ID ${offenceId}`);
    }

    // Map offence JSON fields to the expected SOAP fields
    const OFR_ID = offence.OffenceID;
    const CJS_CODE = offence.CJSCode || "";
    const CJS_TITLE = offence.OffenceRevisions?.CJSTitle || "";
    const OAS_TEXT = offence.ActAndSection?.ActAndSection || "";
    const OFFENCE_TYPE = offence.OffenceRevisions?.OffenceType || "";
    const SOW_REFERENCE = offence.OffenceRevisions?.SOWReference || "";
    const STATUS = "Draft"; // Matches your UpdateOffenceFull model
    const RELEASE_DETAILS = ""; // No release logic yet
    const DATE_USED_FROM = offence.OffenceRevisions?.UseFrom || "";
    const DATE_USED_TO = offence.OffenceRevisions?.UseTo || "";

    // ------------------------------------
    // 3. BUILD SOAP RESPONSE BODY
    // ------------------------------------
    const bodyXml = `
      <ns7:GetReferenceDataResponse xmlns:ns7="http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse">
        <ReferenceDataSet ReferenceDataDescription="OFFENCES">
          <ReferenceDataType>

            <ReferenceColumnType>
              <ColumnItem ColumnOrder="1"><ColumnName>OFR_ID</ColumnName><DisplayName>Offence Revision ID</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>Integer</ColumnType><ReadOnly>Y</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="2"><ColumnName>CJS_CODE</ColumnName><DisplayName>CJS Code</DisplayName><Visible>Y</Visible><TableName>OFFENCE_HEADERS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>string</ColumnType><ReadOnly>N</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="3"><ColumnName>CJS_TITLE</ColumnName><DisplayName>CJS Title</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>120</ColumnSize><ColumnType>string</ColumnType><ReadOnly>Y</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="4"><ColumnName>OAS_TEXT</ColumnName><DisplayName>Acts and Sections</DisplayName><Visible>Y</Visible><TableName>OFFENCE_ACTS_AND_SECTIONS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>4000</ColumnSize><ColumnType>string</ColumnType><ReadOnly>Y</ReadOnly><Mandatory>N</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="5"><ColumnName>OFFENCE_TYPE</ColumnName><DisplayName>Offence Type</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>2</ColumnSize><ColumnType>string</ColumnType><ReadOnly>Y</ReadOnly><Mandatory>N</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="6"><ColumnName>SOW_REFERENCE</ColumnName><DisplayName>SOW Reference</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>string</ColumnType><ReadOnly>Y</ReadOnly><Mandatory>N</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="7"><ColumnName>STATUS</ColumnName><DisplayName>CSH Status</DisplayName><Visible>Y</Visible><TableName>CHANGE_SET_HEADER</TableName><SchemaName>PSS</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>string</ColumnType><ReadOnly>N</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="8"><ColumnName>RELEASE_DETAILS</ColumnName><DisplayName>Release Description and Status</DisplayName><Visible>Y</Visible><TableName>RELEASE_PACKAGE</TableName><SchemaName>LIBRA</SchemaName><UpdateType>N</UpdateType><ColumnSize>400</ColumnSize><ColumnType>String</ColumnType><ReadOnly>N</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="9"><ColumnName>DATE_USED_FROM</ColumnName><DisplayName>Start Date</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>PSS</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>Date</ColumnType><ReadOnly>N</ReadOnly><Mandatory>Y</Mandatory></ColumnItem>
              <ColumnItem ColumnOrder="10"><ColumnName>DATE_USED_TO</ColumnName><DisplayName>End Date</DisplayName><Visible>Y</Visible><TableName>OFFENCE_REVISIONS</TableName><SchemaName>PSS</SchemaName><UpdateType>N</UpdateType><ColumnSize>10</ColumnSize><ColumnType>Date</ColumnType><ReadOnly>N</ReadOnly><Mandatory>N</Mandatory></ColumnItem>
            </ReferenceColumnType>

            <ReferenceRowType>
              <DataItem ColumnOrder="1" ReferenceTypePK="${OFR_ID}"><Value>${OFR_ID}</Value></DataItem>
              <DataItem ColumnOrder="2" ReferenceTypePK="${OFR_ID}"><Value>${CJS_CODE}</Value></DataItem>
              <DataItem ColumnOrder="3" ReferenceTypePK="${OFR_ID}"><Value>${CJS_TITLE}</Value></DataItem>
              <DataItem ColumnOrder="4" ReferenceTypePK="${OFR_ID}"><Value>${OAS_TEXT}</Value></DataItem>
              <DataItem ColumnOrder="5" ReferenceTypePK="${OFR_ID}"><Value>${OFFENCE_TYPE}</Value></DataItem>
              <DataItem ColumnOrder="6" ReferenceTypePK="${OFR_ID}"><Value>${SOW_REFERENCE}</Value></DataItem>
              <DataItem ColumnOrder="7" ReferenceTypePK="${OFR_ID}"><Value>${STATUS}</Value></DataItem>
              <DataItem ColumnOrder="8" ReferenceTypePK="${OFR_ID}"><Value>${RELEASE_DETAILS}</Value></DataItem>
              <DataItem ColumnOrder="9" ReferenceTypePK="${OFR_ID}"><Value>${DATE_USED_FROM}</Value></DataItem>
              <DataItem ColumnOrder="10" ReferenceTypePK="${OFR_ID}"><Value>${DATE_USED_TO}</Value></DataItem>
            </ReferenceRowType>

          </ReferenceDataType>
        </ReferenceDataSet>
      </ns7:GetReferenceDataResponse>
    `;

    // ------------------------------------
    // 4. WS-A HEADER
    // ------------------------------------
    const headerXml = buildWsaHeader({
      action: "getOffenceSummary",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleGetOffenceSummary };