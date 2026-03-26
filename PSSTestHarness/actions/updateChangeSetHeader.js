// actions/updateChangeSetHeader.js
const xml2js = require("xml2js");
const { v4: uuidv4 } = require("uuid");
const { cleanXml } = require("../utils/xmlUtils");
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleUpdateChangeSetHeader(store) {
  return async function handleUpdateChangeSetHeader(xmlBody) {

    //
    // 1. CLEAN XML
    //
    xmlBody = cleanXml(xmlBody);

    //
    // 2. PARSE XML (strip prefixes)
    //
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

    //
    // 3. WS-A inbound correlation ID
    //
    const inboundMessageId =
      header.MessageID ||
      header["addr:MessageID"] ||
      uuidv4();

    //
    // 4. Extract ChangeSetHeaderType
    //
    const req = body.UpdateChangeSetHeaderRequest?.ChangeSetHeaderType;
    if (!req) throw new Error("Missing ChangeSetHeaderType");

    //
    // 5. Extract fields from XML
    //
    const pk = parseInt(req.ChangeSetHeaderPK);
    if (!pk) throw new Error("Missing or invalid ChangeSetHeaderPK");

    const referenceType = req.ReferenceType?.trim() || `CSH_${pk}`;
    const status = req.Status?.trim() || "Draft";

    const releasePackagePk = req.ReleasePackagePK
      ? parseInt(req.ReleasePackagePK)
      : null;

    // ⚠ XML may include ParentRefType in later integrations
    const parentRefType = req.ParentRefType?.trim() || null;

    //
    // 6. Retrieve existing record using PK as the top-level key
    //
    let record = store.get(String(pk));

    if (record) {
      //
      // Update fields while preserving ParentRefType if XML doesn't override it
      //
      record.ReferenceType = referenceType;
      record.Status = status;
      record.ReleasePackagePK = releasePackagePk;

      if (parentRefType !== null) {
        record.ParentRefType = parentRefType;
      } else {
        if (!record.hasOwnProperty("ParentRefType")) {
          record.ParentRefType = null;
        }
      }

    } else {
      //
      // 7. Create new record if not found
      //
      record = {
        ReferenceType: referenceType,
        PK: pk,
        Status: status,
        ParentRefType: parentRefType,
        ReleasePackagePK: releasePackagePk
      };
    }

    //
    // 8. SAVE using PK as top-level key
    //
    store.set(String(pk), record);

    //
    // 9. Build WS-A Header
    //
    const headerXml = buildWsaHeader({
      action: "updateChangeSetHeader",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    //
    // 10. SOAP Response
    //
    const today = new Date().toISOString().slice(0, 10);

    const bodyXml = `
      <ns5:Acknowledgement xmlns:ns5="http://www.justice.gov.uk/magistrates/ack">
        <Ack>
          <MessageStatus>Change made</MessageStatus>
          <TimeStamp>${today}</TimeStamp>
        </Ack>
      </ns5:Acknowledgement>
    `;

    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleUpdateChangeSetHeader };