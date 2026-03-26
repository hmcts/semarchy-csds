// actions/getOffenceMenuFull.js

const xml2js = require("xml2js");
const path = require("path");
const fs = require("fs");
const { v4: uuidv4 } = require("uuid");
const { cleanXml } = require("../utils/xmlUtils");
const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleGetOffenceMenuFull() {
  return async function handleGetOffenceMenuFull(xmlBody) {

    // ----------------------------
    // 1. Clean + Parse SOAP XML
    // ----------------------------
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

    const req = body.GetOffenceMenuFullRequest;
    const OM_ID = parseInt(req.OM_ID, 10);
    const status = req.Status || "DRAFT";

    if (!OM_ID) throw new Error("Missing OM_ID in GetOffenceMenuFullRequest");

    // ----------------------------
    // 2. Load menus/{OM_ID}.json
    // ----------------------------
    const menusDir = path.join(__dirname, "..", "persisted_keys/menus");
    const menuFile = path.join(menusDir, `${OM_ID}.json`);

    if (!fs.existsSync(menuFile)) {
      throw new Error(`Menu file not found: ${menuFile}`);
    }

    const menuJson = JSON.parse(fs.readFileSync(menuFile, "utf8"));

    // ----------------------------
    // 3. Build XML fragments for menu options
    // ----------------------------

    function buildElements(opt) {
      return opt.MenuOptionElements.map(el => `
          <OTEMenuOptionElements>
            <OMOP_ID>${el.OMOP_ID}</OMOP_ID>
            <ElementNumber>${el.ElementNumber}</ElementNumber>
            <VersionNumber>1</VersionNumber>
            <ChangedBy>${menuJson.AuditingInformation.ChangedBy}</ChangedBy>
            <ChangedDate>${menuJson.AuditingInformation.ChangedDate}</ChangedDate>
            <OMO_OMO_ID>${el.OMO_OMO_ID}</OMO_OMO_ID>
            <OED_OED_ID>${el.OED_OED_ID}</OED_OED_ID>
            <CSH_CSH_ID>${el.CSH_CSH_ID}</CSH_CSH_ID>

            <OTEElementDefinitions>
              <OED_ID>${el.ElementDefinition.OED_ID}</OED_ID>
              <OEDMin>${el.ElementDefinition.OEDMin}</OEDMin>
              <OEDMax>${el.ElementDefinition.OEDMax}</OEDMax>
              <EntryFormat>${el.ElementDefinition.EntryFormat}</EntryFormat>
              <EntryPrompt>${el.ElementDefinition.EntryPrompt}</EntryPrompt>
              <VersionNumber>1</VersionNumber>
              <ChangedBy>${menuJson.AuditingInformation.ChangedBy}</ChangedBy>
              <ChangedDate>${menuJson.AuditingInformation.ChangedDate}</ChangedDate>
              <CSH_CSH_ID>${el.ElementDefinition.CSH_CSH_ID}</CSH_CSH_ID>
            </OTEElementDefinitions>
          </OTEMenuOptionElements>
      `).join("");
    }

    function buildOptions() {
      return menuJson.MenuOptions.map(opt => `
        <OTEMenuOptions>
          <OMO_ID>${opt.OMO_ID}</OMO_ID>
          <OptionNumber>${opt.OptionNumber}</OptionNumber>
          <OptionText>${opt.OptionText}</OptionText>
          <VersionNumber>1</VersionNumber>
          <ChangedBy>${menuJson.AuditingInformation.ChangedBy}</ChangedBy>
          <ChangedDate>${menuJson.AuditingInformation.ChangedDate}</ChangedDate>
          <OM_OM_ID>${OM_ID}</OM_OM_ID>
          <CSH_CSH_ID>${opt.CSH_CSH_ID}</CSH_CSH_ID>
          ${buildElements(opt)}
        </OTEMenuOptions>
      `).join("");
    }

    // ----------------------------
    // 4. Build SOAP Body
    // ----------------------------

    const bodyXml = `
      <ns33:GetOffenceMenuFullResponse xmlns:ns33="http://www.justice.gov.uk/magistrates/pss/GetOffenceMenuFullResponse">
        <GetOffenceMenuFullResponseType>
          <EditType>${menuJson.EditType || "NEW"}</EditType>
          <OM_ID>${OM_ID}</OM_ID>
          <Name>${menuJson.Name}</Name>
          <VersionNumber>1</VersionNumber>
          <ChangedBy>${menuJson.AuditingInformation.ChangedBy}</ChangedBy>
          <ChangedDate>${menuJson.AuditingInformation.ChangedDate.split("T")[0]}</ChangedDate>
          <CSH_CSH_ID>${menuJson.CSH_CSH_ID}</CSH_CSH_ID>

          ${buildOptions()}

          <LinkedOffencesType />
        </GetOffenceMenuFullResponseType>
      </ns33:GetOffenceMenuFullResponse>
    `;

    // ----------------------------
    // 5. WS‑Addressing Header
    // ----------------------------

    const headerXml = buildWsaHeader({
      action: "getOffenceMenuFull",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    // ----------------------------
    // 6. Build Complete SOAP Envelope
    // ----------------------------
    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleGetOffenceMenuFull };