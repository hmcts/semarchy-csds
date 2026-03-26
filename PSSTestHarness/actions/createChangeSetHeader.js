// actions/createChangeSetHeader.js
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");
const { v4: uuidv4 } = require("uuid");

// Generate 5‑digit PK
const generateRandomPK = () => Math.floor(10000 + Math.random() * 90000);

// Safe string normalizer
const normalize = v =>
  v === null || v === undefined ? "" : String(v).trim();

function createHandleCreateChangeSetHeader(store) {
  const INDEX_KEY = "__refIndex";

  // Load or init the reference index
  function loadIndex() {
    return store.get(INDEX_KEY) || {};
  }

  function saveIndex(idx) {
    store.set(INDEX_KEY, idx);
  }

  return function handleCreateChangeSetHeader({ body, header }) {

    //
    // 1. Extract ChangeSetHeaderType
    //
    const reqNode =
      body?.CreateChangeSetHeaderRequest ||
      Object.values(body).find(
        n => n && typeof n === "object" && n.ChangeSetHeaderType
      );

    if (!reqNode) throw new Error("Missing CreateChangeSetHeaderRequest");

    const headerType = reqNode.ChangeSetHeaderType;
    if (!headerType) throw new Error("Missing ChangeSetHeaderType");

    //
    // 2. Extract & normalise fields
    //
    const referenceType = normalize(headerType.ReferenceType);
    if (!referenceType)
      throw new Error("Missing ChangeSetHeaderType.ReferenceType");

    const status = normalize(headerType.Status) || "Draft";
    const parentRefType = normalize(headerType.ParentRefType);

    //
    // 🔥 3. Build composite index key
    //
    const compositeKey = `${referenceType}::${parentRefType}`;

    //
    // 4. Load index and look up composite key
    //
    const index = loadIndex();
    let pk = index[compositeKey];

    if (!pk) {
      // No entry → generate PK
      pk = String(generateRandomPK());

      // Save composite key → PK mapping
      index[compositeKey] = pk;
      saveIndex(index);
    }

    //
    // 5. Insert/update actual record
    //
    const storedObj = {
      ReferenceType: referenceType,
      PK: Number(pk),
      Status: status,
      ParentRefType: parentRefType
    };

    store.set(pk, storedObj);

    //
    // 6. WS-A header
    //
    const inboundMessageId =
      header?.MessageID ||
      header?.["wsa:MessageID"] ||
      "";

    const headerXml = buildWsaHeader({
      action: "createChangeSetHeader",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    //
    // 7. SOAP body
    //
    const bodyXml = `
      <ns35:CreateChangeSetHeaderResponse
          xmlns:ns35="http://www.justice.gov.uk/magistrates/pss/CreateChangeSetHeaderResponse">
        <ChangeSetHeaderNode>
          <ChangeSetHeaderPK>${pk}</ChangeSetHeaderPK>
        </ChangeSetHeaderNode>
      </ns35:CreateChangeSetHeaderResponse>
    `;

    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleCreateChangeSetHeader };