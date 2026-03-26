// actions/getReleasePackage.js
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");
const { v4: uuidv4 } = require("uuid");

function createHandleGetReleasePackage(store) {
  return function handleGetReleasePackage({ body, header }) {

    //
    // 1. Auto-detect the request node
    //
    const reqNode =
      body?.GetReleasePackageRequest ||
      Object.values(body).find(
        n => n && typeof n === "object" && n.ReleasePackagePK
      );

    if (!reqNode) {
      throw new Error("Missing GetReleasePackageRequest");
    }

    //
    // 2. Extract PK
    //
    const rawPk = reqNode.ReleasePackagePK;
    const releasePackagePK =
      typeof rawPk === "string" ? rawPk.trim() : rawPk;

    if (!releasePackagePK) {
      throw new Error("Missing ReleasePackagePK");
    }

    //
    // 3. Lookup JSON entry by PK
    //
    const entry = store.get(String(releasePackagePK));
    if (!entry) {
      throw new Error(
        `ReleasePackage with PK ${releasePackagePK} not found`
      );
    }

    //
    //  Generate a random number 1–5.
    //  If it's 5 → update entry.Status = "Published".
    //
    const random = Math.floor(Math.random() * 3) + 1; // 1–5
    
    if (random === 3) {
      entry.Status = "Published";
    }

    // Save updated entry back into store
    store.set(String(releasePackagePK), entry);

    //
    // 4. Build WS-A header
    //
    const inboundMessageId =
      header?.MessageID ||
      header?.["wsa:MessageID"] ||
      "";

    const headerXml = buildWsaHeader({
      action: "getReleasePackage",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    //
    // 5. Build SOAP body XML
    //
    const bodyXml = `
      <ns99:GetReleasePackageResponse 
        xmlns:ns99="http://www.justice.gov.uk/magistrates/pss/GetReleasePackageResponse">

        <ReleasePackageType>
          <ReleasePackagePK>${entry.PK}</ReleasePackagePK>
          <Description>${entry.Description || ""}</Description>
          <Status>${entry.Status || ""}</Status>
          <PublishDate>${entry.PublishDate || ""}</PublishDate>
          <PublishTime>${entry.PublishTime || ""}</PublishTime>
          <ReleaseDate>${entry.ReleaseDate || ""}</ReleaseDate>
          <PublishedBy>${entry.PublishedBy || ""}</PublishedBy>
          <Notes>${entry.Notes || ""}</Notes>
          <UpdateType>${entry.UpdateType || ""}</UpdateType>
        </ReleasePackageType>

      </ns99:GetReleasePackageResponse>
    `;

    //
    // 6. Wrap in standard SOAP envelope
    //
    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleGetReleasePackage };