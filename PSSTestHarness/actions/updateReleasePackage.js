// actions/updateReleasePackage.js
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");
const { v4: uuidv4 } = require("uuid");

function createHandleUpdateReleasePackage(store) {
  return function handleUpdateReleasePackage({ body, header }) {

    //
    // 1. AUTO-DETECT THE UPDATE REQUEST NODE (supports namespace prefixes)
    //
    const reqNode = Object.values(body).find(
      node =>
        node &&
        typeof node === "object" &&
        node.ReleasePackageType
    );

    if (!reqNode) {
      throw new Error(
        "Missing UpdateReleasePackageRequest - no node containing ReleasePackageType"
      );
    }

    const rpType = reqNode.ReleasePackageType;
    if (!rpType) throw new Error("Missing ReleasePackageType");

    //
    // 2. Extract PK
    //
    const rpPkRaw = rpType.ReleasePackagePK;
    const releasePackagePK =
      typeof rpPkRaw === "string" ? rpPkRaw.trim() : rpPkRaw;

    if (!releasePackagePK) {
      throw new Error("Missing ReleasePackagePK");
    }

    //
    // 3. Fetch the existing JSON entry
    //
    const existing = store.get(String(releasePackagePK));
    if (!existing) {
      throw new Error(
        `ReleasePackage with PK ${releasePackagePK} not found in JSON`
      );
    }

    //
    // 4. Extract ALL fields and update the JSON
    //
    const updated = {
      ...existing,

      // Always keep PK correct in JSON
      PK: Number(releasePackagePK),

      // Overwrite fields only if present in XML
      ...(rpType.Description
        ? { Description: rpType.Description.trim() }
        : {}),

      ...(rpType.Status
        ? { Status: rpType.Status.trim() }
        : {}),

      ...(rpType.PublishDate
        ? { PublishDate: rpType.PublishDate.trim() }
        : {}),

      ...(rpType.PublishTime
        ? { PublishTime: rpType.PublishTime.trim() }
        : {}),

      ...(rpType.ReleaseDate
        ? { ReleaseDate: rpType.ReleaseDate.trim() }
        : {}),

      ...(rpType.PublishedBy
        ? { PublishedBy: rpType.PublishedBy.trim() }
        : {}),

      ...(rpType.Notes
        ? { Notes: rpType.Notes.trim() }
        : {}),

      ...(rpType.UpdateType
        ? { UpdateType: rpType.UpdateType.trim() }
        : {})
    };

    //
    // 5. Store updated entry under PK key
    //
    store.set(String(releasePackagePK), updated);

    //
    // 6. Build WS-A header
    //
    const inboundMessageId =
      header?.MessageID ||
      header?.["wsa:MessageID"] ||
      "";

    const headerXml = buildWsaHeader({
      action: "updateReleasePackage",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    //
    // 7. SOAP response body
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

module.exports = { createHandleUpdateReleasePackage };