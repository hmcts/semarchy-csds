// actions/updateOffenceMenuFull.js
// Fully fixed – correct factory, correct imports, correct SOAP response.

const { v4: uuidv4 } = require("uuid");
const xml2js = require("xml2js");
const path = require("path");
const {
  randomId,
  cleanXml,
  toInt,
  asArray,
  ensureDir,
  readJsonSafe,
  writeJson
} = require("../utils/xmlUtils");

const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleUpdateOffenceMenuFull() {
  return async function handleUpdateOffenceMenuFull(xmlBody) {

    // ----------------------------------------
    // 1. Clean + Parse XML
    // ----------------------------------------
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

    const incomingMessageId =
      header.MessageID ||
      header["addr:MessageID"] ||
      uuidv4();

    const reqRoot = body.UpdateOffenceMenuFullRequest;
    const request = reqRoot.UpdateOffenceMenuFullType;
    const audit = reqRoot.AuditingInformation || {};

    // ----------------------------------------
    // 2. Storage setup
    // ----------------------------------------
    const dataRoot = path.join(__dirname, "..");
    const menusDir = path.join(dataRoot, "persisted_keys", "menus");
    const indexFile = path.join(dataRoot, "persisted_keys", "offenceMenus.json");

    ensureDir(menusDir);

    const name = request.Name;
    const index = readJsonSafe(indexFile, {});

    let OM_ID = toInt(request.OM_ID, { fallbackToRandom: false });

    if (!OM_ID || OM_ID === -1) {
      if (index[name]) {
        OM_ID = index[name];
      } else {
        OM_ID = randomId();
        index[name] = OM_ID;
        writeJson(indexFile, index);
      }
    }

    // ----------------------------------------
    // 3. Menu Options + Elements
    //    (THIS IS WHERE WE FIX OED_ID)
    // ----------------------------------------
    const menuOptions = asArray(request.OTEMenuOptions).map(opt => {
      const OMO_ID = toInt(opt.OMO_ID, { fallbackToRandom: true });

      const elements = asArray(opt.OTEMenuOptionElements).map(elem => {
        const OMOP_ID = toInt(elem.OMOP_ID, { fallbackToRandom: true });
        const def = elem.OTEElementDefinitions || {};

        // ⭐ NEW — Generate a new OED_ID for each element
        const NEW_OED_ID = randomId();

        return {
          OMOP_ID,
          OperationType: elem.OperationType,
          ElementNumber: toInt(elem.ElementNumber),
          VersionNumber: null,
          OMO_OMO_ID: OMO_ID,

          // ⭐ These three ALL become the new random value
          OED_ID: NEW_OED_ID,
          OED_OED_ID: NEW_OED_ID,

          CSH_CSH_ID: toInt(elem.CSH_CSH_ID),
          RelatedItemsIdentifier: toInt(elem.RelatedItemsIdentifier),
          RelatedItemsIdentifierIndex: toInt(elem.RelatedItemsIdentifierIndex),

          ElementDefinition: {
            // ⭐ And here too:
            OED_ID: NEW_OED_ID,

            OperationType: def.OperationType,
            OEDMin: toInt(def.OEDMin),
            OEDMax: toInt(def.OEDMax),
            EntryFormat: def.EntryFormat,
            EntryPrompt: def.EntryPrompt,
            VersionNumber: null,
            CSH_CSH_ID: toInt(def.CSH_CSH_ID),
            RelatedItemsIdentifier: toInt(def.RelatedItemsIdentifier),
            RelatedItemsIdentifierIndex: toInt(def.RelatedItemsIdentifierIndex)
          }
        };
      });

      return {
        OMO_ID,
        OMO_OMO_ID: OMO_ID,
        OperationType: opt.OperationType,
        OptionNumber: toInt(opt.OptionNumber),
        OptionText: opt.OptionText,
        VersionNumber: null,
        OM_OM_ID: OM_ID,
        CSH_CSH_ID: toInt(opt.CSH_CSH_ID),
        RelatedItemsIdentifier: toInt(opt.RelatedItemsIdentifier),
        RelatedItemsIdentifierIndex: toInt(opt.RelatedItemsIdentifierIndex),
        MenuOptionElements: elements
      };
    });

    // ----------------------------------------
    // 4. Write JSON to disk
    // ----------------------------------------
    const finalJson = {
      OM_ID,
      Name: name,
      OperationType: request.OperationType,
      VersionNumber: null,
      CSH_CSH_ID: toInt(request.CSH_CSH_ID),
      HMCTSNotes: request.HMCTSNotes,
      EditType: request.EditType,
      MenuOptions: menuOptions,
      AuditingInformation: {
        ChangedBy: audit.ChangedBy,
        ChangedDate: audit.ChangedDate
      }
    };

    writeJson(path.join(menusDir, `${OM_ID}.json`), finalJson);

    // ----------------------------------------
    // 5. SOAP Acknowledgement Response
    // ----------------------------------------
    const headerXml = buildWsaHeader({
      action: "updateOffenceMenuFull",
      messageId: uuidv4(),
      relatesTo: incomingMessageId
    });

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

module.exports = { createHandleUpdateOffenceMenuFull };
``