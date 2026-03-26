// actions/updateOffenceFull.js
// Built using updateOffenceMenuFull.js as the template

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

function createHandleUpdateOffenceFull() {
  return async function handleUpdateOffenceFull(xmlBody) {
    // --- CLEAN & PARSE XML ---
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

    const reqRoot = body.UpdateOffenceFullRequest;

    // Inner structure
    const request = reqRoot.UpdateOffenceFullRequest;
    const audit = reqRoot.AuditingInformation || {};

    // --- DATA STORAGE LOCATIONS ---
    const dataRoot = path.join(__dirname, "..");
    const offencesDir = path.join(dataRoot, "persisted_keys/offences");
    const indexFile = path.join(dataRoot, "persisted_keys/offencesIndex.json");

    ensureDir(offencesDir);

    // CJS Code uniquely identifies Offence
    const cjsCode = request.OffenceHeaderType?.CJSCode;
    const index = readJsonSafe(indexFile, {});

    let offenceId = index[cjsCode];
    if (!offenceId) {
      offenceId = randomId();
      index[cjsCode] = offenceId;
      writeJson(indexFile, index);
    }

    // --- TERMINAL ENTRIES (array-safe) ---
    const terminalEntries = asArray(request.OffenceTerminalEntriesType).map(ent => ({
      OTE_ID: toInt(ent.OTE_ID, { fallbackToRandom: true }),
      EntryNumber: toInt(ent.EntryNumber),
      Min: toInt(ent.Min),
      Max: toInt(ent.Max),
      EntryFormat: ent.EntryFormat,
      EntryPrompt: ent.EntryPrompt,
      StandardEntryIdentifier: ent.StandardEntryIdentifier,
      OM_OM_ID: toInt(ent.OM_OM_ID),
      MenuName: ent.MenuName,
      CSH_CSH_ID: toInt(ent.CSH_CSH_ID)
    }));

    // --- FINAL JSON PERSISTED STRUCTURE ---
    const finalJson = {
      OffenceID: offenceId,
      CJSCode: cjsCode,
      OffenceHeader: request.OffenceHeaderType,
      OffenceRevisions: request.OffenceRevisionsType,
      OffenceWordings: request.OffenceWordingsType,
      ActAndSection: request.ActAndSectionType,
      StatementOfFacts: request.StatementOfFactsType,
      TerminalEntries: terminalEntries,
      CrownOffence: request.CrownOffenceType,
      CppOffenceHeader: request.CppOffenceHeaderType,
      CppOffenceRevisions: request.CppOffenceRevisionsType,
      AuditingInformation: {
        ChangedBy: audit.ChangedBy,
        ChangedDate: audit.ChangedDate
      }
    };

    writeJson(path.join(offencesDir, `${offenceId}.json`), finalJson);

    // --- SOAP RESPONSE BUILD ---
    const headerXml = buildWsaHeader({
      action: "updateOffenceFull",
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

module.exports = { createHandleUpdateOffenceFull };