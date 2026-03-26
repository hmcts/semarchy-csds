// actions/getOffenceMenuSummary.js
const xml2js = require("xml2js");
const path = require("path");
const { v4: uuidv4 } = require("uuid");

const {
  cleanXml,
  readJsonSafe
} = require("../utils/xmlUtils");

const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleGetOffenceMenuSummary() {
  return async function handleGetOffenceMenuSummary(xmlBody) {

    // -------------------------------
    // 1. CLEAN + PARSE XML
    // -------------------------------
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

    const reqRoot = body.GetOffenceMenuSummaryRequest;
    const inner = reqRoot.GetOffenceMenuSummaryRequest;

    const title = inner.Title;

    // -------------------------------
    // 2. LOOK UP OFFENCE MENU INDEX
    // -------------------------------
    const dataRoot = path.join(__dirname, "..");
    const indexFile = path.join(dataRoot, "persisted_keys/offenceMenus.json");

    const index = readJsonSafe(indexFile, {});

    const OM_ID = index[title] ?? null;

    // If no match, return empty dataset
    if (!OM_ID) {
      const headerXml = buildWsaHeader({
        action: "getOffenceMenuSummary",
        messageId: uuidv4(),
        relatesTo: inboundMessageId
      });

      const emptyBodyXml = `
        <ns7:GetReferenceDataResponse xmlns:ns7="http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse">
          <ReferenceDataSet ReferenceDataDescription="MENU OFFENCES">
            <ReferenceDataType>
              <ReferenceRowType /> 
            </ReferenceDataType>
          </ReferenceDataSet>
        </ns7:GetReferenceDataResponse>
      `;
      return soapEnvelope(headerXml, emptyBodyXml);
    }

    // -------------------------------
    // 3. BUILD SOAP RESPONSE BODY
    // -------------------------------
    const today = new Date().toISOString().slice(0, 10);

    const bodyXml = `
      <ns7:GetReferenceDataResponse xmlns:ns7="http://www.justice.gov.uk/magistrates/pss/GetReferenceDataResponse">
        <ReferenceDataSet ReferenceDataDescription="MENU OFFENCES">
          <ReferenceDataType>
            <ReferenceColumnType>

                <ColumnItem ColumnOrder="1">
                    <ColumnName>OM_ID</ColumnName>
                    <DisplayName>OM_ID</DisplayName>
                    <Visible>Y</Visible>
                    <TableName>OTE_MENUS</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>10</ColumnSize>
                    <ColumnType>Integer</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>Y</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="2">
                    <ColumnName>NAME</ColumnName>
                    <DisplayName>Menu Name</DisplayName>
                    <Visible>Y</Visible>
                    <TableName>OTE_MENUS</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>50</ColumnSize>
                    <ColumnType>string</ColumnType>
                    <ReadOnly>N</ReadOnly>
                    <Mandatory>N</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="3">
                    <ColumnName>CHANGED_BY</ColumnName>
                    <DisplayName>Created By</DisplayName>
                    <Visible>N</Visible>
                    <TableName>OTE_MENUS</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>10</ColumnSize>
                    <ColumnType>Integer</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>Y</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="4">
                    <ColumnName>CHANGED_DATE</ColumnName>
                    <DisplayName>Created Date</DisplayName>
                    <Visible>N</Visible>
                    <TableName>OTE_MENUS</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>10</ColumnSize>
                    <ColumnType>Date</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>Y</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="5">
                    <ColumnName>STATUS</ColumnName>
                    <DisplayName>CSH Status</DisplayName>
                    <Visible>Y</Visible>
                    <TableName>CHANGE_SET_HEADER</TableName>
                    <SchemaName>PSS</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>10</ColumnSize>
                    <ColumnType>string</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>Y</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="6">
                    <ColumnName>HMCTS_NOTES</ColumnName>
                    <DisplayName>HMCTS Notes</DisplayName>
                    <Visible>Y</Visible>
                    <TableName>OTE_MENUS</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>4000</ColumnSize>
                    <ColumnType>String</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>N</Mandatory>
                </ColumnItem>
                <ColumnItem ColumnOrder="7">
                    <ColumnName>RELEASE_DETAILS</ColumnName>
                    <DisplayName>Release Description and Status</DisplayName>
                    <Visible>Y</Visible>
                    <TableName>RELEASE_PACKAGE</TableName>
                    <SchemaName>LIBRA</SchemaName>
                    <UpdateType>N</UpdateType>
                    <ColumnSize>400</ColumnSize>
                    <ColumnType>String</ColumnType>
                    <ReadOnly>Y</ReadOnly>
                    <Mandatory>Y</Mandatory>
                </ColumnItem> 

            </ReferenceColumnType>

            <ReferenceRowType>

            <DataItem ColumnOrder="1" ReferenceTypePK="${OM_ID}">
                <Value>${OM_ID}</Value>
            </DataItem>
            <DataItem ColumnOrder="2" ReferenceTypePK="${OM_ID}">
                <Value>${title}</Value>
            </DataItem>
            <DataItem ColumnOrder="3" ReferenceTypePK="${OM_ID}">
                <Value>629</Value>
            </DataItem>
            <DataItem ColumnOrder="4" ReferenceTypePK="${OM_ID}">
                <Value>${today}</Value>
            </DataItem>
            <DataItem ColumnOrder="5" ReferenceTypePK="${OM_ID}">
                <Value>Draft</Value>
            </DataItem>
            <DataItem ColumnOrder="6" ReferenceTypePK="${OM_ID}">
                <Value/>
            </DataItem>
            <DataItem ColumnOrder="7" ReferenceTypePK="${OM_ID}">
                <Value></Value>
            </DataItem> 

            </ReferenceRowType>

          </ReferenceDataType>
        </ReferenceDataSet>
      </ns7:GetReferenceDataResponse>
    `;

    // -------------------------------
    // 4. WS-A HEADER
    // -------------------------------
    const headerXml = buildWsaHeader({
      action: "getOffenceMenuSummary",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleGetOffenceMenuSummary };