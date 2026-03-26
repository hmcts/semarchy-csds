// actions/createReleasePackage.js
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");
const { v4: uuidv4 } = require("uuid");

const generateRandomPK = () => Math.floor(10000 + Math.random() * 90000);

function createHandleCreateReleasePackage(store) {
  return function handleCreateReleasePackage({ body, header }) {

    //
    // 1. Detect CreateReleasePackageRequest regardless of namespace
    //
    const reqNode =
      body?.CreateReleasePackageRequest ||
      Object.values(body).find(n => n?.ReleasePackageType);

    if (!reqNode) {
      throw new Error("Missing CreateReleasePackageRequest");
    }

    const rpType = reqNode.ReleasePackageType;
    if (!rpType) {
      throw new Error("Missing ReleasePackageType");
    }

    //
    // 2. Extract all fields from XML
    //
    const xmlPK = rpType.ReleasePackagePK;
    const providedPK =
      typeof xmlPK === "string" ? xmlPK.trim() : xmlPK;

    const description = rpType.Description?.trim();
    if (!description) {
      throw new Error("Missing ReleasePackageType.Description");
    }

    const status = rpType.Status?.trim() || "Draft";

    const publishDate = rpType.PublishDate?.trim() || "";
    const publishTime = rpType.PublishTime?.trim() || "";
    const releaseDate = rpType.ReleaseDate?.trim() || "";
    const publishedBy = rpType.PublishedBy?.trim() || "";
    const notes = rpType.Notes?.trim() || "";
    const updateType = rpType.UpdateType?.trim() || "";

    //
    // 3. Choose PK: use XML value if provided, else generate
    //
    const pk = providedPK || generateRandomPK();

    //
    // 4. Build stored JSON object
    //
    const storedObj = {
      Description: description,
      PK: Number(pk),
      Status: status,
      PublishDate: publishDate,
      PublishTime: publishTime,
      ReleaseDate: releaseDate,
      PublishedBy: publishedBy,
      Notes: notes,
      UpdateType: updateType
    };

    //
    // 5. Save to store: key = PK
    //
    store.set(String(pk), storedObj);

    //
    // 6. WS-A SOAP response header
    //
    const inboundMessageId =
      header?.MessageID ||
      header?.["wsa:MessageID"] ||
      "";

    const headerXml = buildWsaHeader({
      action: "createReleasePackage",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    //
    // 7. SOAP response body
    //
    const bodyXml = `
      <ns63:CreateReleasePackageResponse 
        xmlns:ns63="http://www.justice.gov.uk/magistrates/pss/CreateReleasePackageResponse">
        <ReleasePackagePK>${pk}</ReleasePackagePK>
      </ns63:CreateReleasePackageResponse>
    `;

    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleCreateReleasePackage };