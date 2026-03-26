// actions/getOffenceFull.js
// Generated to match the expected full Offence response payload

const { v4: uuidv4 } = require("uuid");
const xml2js = require("xml2js");
const path = require("path");

const {
  cleanXml,
  readJsonSafe,
  asArray
} = require("../utils/xmlUtils");

const { soapEnvelope } = require("../utils/soapEnvelope");
const { buildWsaHeader } = require("../utils/wsHeaders");

function createHandleGetOffenceFull() {
  return async function handleGetOffenceFull(xmlBody) {

    // ------------------------------------------------------
    // 1. CLEAN + PARSE XML
    // ------------------------------------------------------
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
    const body   = envelope.Body || {};

    const inboundMessageId =
      header.MessageID ||
      header["addr:MessageID"] ||
      uuidv4();

    const reqRoot = body.GetOffenceFullRequest;
    const OFR_ID  = reqRoot.OFR_ID;

    // ------------------------------------------------------
    // 2. LOAD PERSISTED OFFENCE JSON
    // ------------------------------------------------------
    const dataRoot    = path.join(__dirname, "..");
    const offencesDir = path.join(dataRoot, "persisted_keys/offences");
    const offenceFile = path.join(offencesDir, `${OFR_ID}.json`);

    const offence = readJsonSafe(offenceFile, null);

    if (!offence) {
      throw new Error(`No offence data stored for OFR_ID ${OFR_ID}`);
    }

    // ------------------------------------------------------
    // 3. MAP FROM STORED JSON TO FULL RESPONSE FIELDS
    // ------------------------------------------------------
    const headerData   = offence.OffenceHeader || {};
    const rev          = offence.OffenceRevisions || {};
    const wordings     = offence.OffenceWordings || {};
    const actSection   = offence.ActAndSection || {};
    const facts        = offence.StatementOfFacts || {};
    const crown        = offence.CrownOffence || {};
    const cppHeader    = offence.CppOffenceHeader || {};
    const cppRevs      = offence.CppOffenceRevisions || {};
    const terminals    = asArray(offence.TerminalEntries);

    // ------------------------------------------------------
    // 4. BUILD TERMINAL ENTRY XML
    // ------------------------------------------------------
    const terminalsXml = terminals.map(t => `
      <OffenceTerminalEntriesType>
        <OTE_ID>${t.OTE_ID}</OTE_ID>
        <EntryNumber>${t.EntryNumber}</EntryNumber>
        <Min>${t.Min}</Min>
        <Max>${t.Max}</Max>
        <EntryFormat>${t.EntryFormat}</EntryFormat>
        <EntryPrompt>${t.EntryPrompt}</EntryPrompt>
        <StandardEntryIdentifier>${t.StandardEntryIdentifier || ""}</StandardEntryIdentifier>
        <VersionNumber>3</VersionNumber>
        <ChangedBy>585</ChangedBy>
        <ChangedDate>12/02/2026</ChangedDate>
        <OFR_ID>${OFR_ID}</OFR_ID>
        <OM_OM_ID>${t.OM_OM_ID || ""}</OM_OM_ID>
        <MenuName>${t.MenuName || ""}</MenuName>
        <CSH_CSH_ID>${t.CSH_CSH_ID || ""}</CSH_CSH_ID>
        <STATUS>DRAFT</STATUS>
      </OffenceTerminalEntriesType>
    `).join("");

    // ------------------------------------------------------
    // 5. BUILD RESPONSE BODY XML
    // ------------------------------------------------------
    const bodyXml = `
      <ns58:GetOffenceFullResponse xmlns:ns58="http://www.justice.gov.uk/magistrates/pss/GetOffenceFullResponse">
        <GetOffenceFullResponseType>

          <OffenceHeaderType>
            <OH_ID>${headerData.OH_ID || "579069"}</OH_ID>
            <CJSCode>${offence.CJSCode}</CJSCode>
            <VersionNumber>4</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <GoBCode>0</GoBCode>
            <Blocked>${headerData.Blocked || "N"}</Blocked>
            <CSH_CSH_ID>${headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </OffenceHeaderType>

          <OffenceRevisionsType>
            <EditType>${rev.EditType || "NEW"}</EditType>
            <OFR_ID>${OFR_ID}</OFR_ID>
            <Recordable>${rev.Recordable || "N"}</Recordable>
            <Reportable>${rev.Reportable || "N"}</Reportable>
            <CJSTitle>${rev.CJSTitle || ""}</CJSTitle>
            <CustodialIndicator>${rev.CustodialIndicator || "N"}</CustodialIndicator>
            <SOWReference>${rev.SOWReference || ""}</SOWReference>
            <MISClass>${rev.MISClass || ""}</MISClass>
            <OffenceType>${rev.OffenceType || ""}</OffenceType>
            <DVLACode>${rev.DVLACode || ""}</DVLACode>
            <UseFrom>${rev.UseFrom || ""}</UseFrom>
            <UseTo>${rev.UseTo || ""}</UseTo>
            <Notes>${rev.Notes || ""}</Notes>
            <StandardList>${rev.StandardList || "N"}</StandardList>
            <MaxPenalty>${rev.MaxPenalty || ""}</MaxPenalty>
            <VersionNumber>4</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <OAS_OAS_ID>${actSection.OAS_ID || "628162"}</OAS_OAS_ID>
            <OSF_OSF_ID>${facts.OSF_ID || "209733"}</OSF_OSF_ID>
            <OW_OW_ID>${wordings.OW_ID || "647108"}</OW_OW_ID>
            <OH_OH_ID>${headerData.OH_ID || "579069"}</OH_OH_ID>
            <Description>${rev.Description || ""}</Description>
            <HOClass>${rev.HOClass || ""}</HOClass>
            <HOSubClass>${rev.HOSubClass || ""}</HOSubClass>
            <ProceedingsCode>${rev.ProceedingsCode || ""}</ProceedingsCode>
            <CJSTitleCY>${rev.CJSTitleCY || ""}</CJSTitleCY>
            <CSH_CSH_ID>${rev.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </OffenceRevisionsType>

          <OffenceWordingsType>
            <OW_ID>${wordings.OW_ID || "647108"}</OW_ID>
            <OffenceWording>${wordings.OffenceWording || ""}</OffenceWording>
            <VersionNumber>4</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <SLOffenceWording>${wordings.SLOffenceWording || ""}</SLOffenceWording>
            <CSH_CSH_ID>${wordings.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </OffenceWordingsType>

          <ActAndSectionType>
            <OAS_ID>${actSection.OAS_ID || "628162"}</OAS_ID>
            <ActAndSection>${actSection.ActAndSection || ""}</ActAndSection>
            <VersionNumber>4</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <SLActAndSection>${actSection.SLActAndSection || ""}</SLActAndSection>
            <CSH_CSH_ID>${actSection.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </ActAndSectionType>

          <StatementOfFactsType>
            <OSF_ID>${facts.OSF_ID || "209733"}</OSF_ID>
            <StatementOfFacts>${facts.StatementOfFacts || ""}</StatementOfFacts>
            <VersionNumber>4</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <SLStatementOfFacts>${facts.SLStatementOfFacts || ""}</SLStatementOfFacts>
            <CSH_CSH_ID>${facts.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </StatementOfFactsType>

          ${terminalsXml}

          <CrownOffenceType>
            <RefOffenceId>${crown.RefOffenceId || "26703"}</RefOffenceId>
            <OffenceClass>${crown.OffenceClass || "1"}</OffenceClass>
            <ObsInd>${crown.ObsInd || "N"}</ObsInd>
            <VersionNumber>3</VersionNumber>
            <CSH_CSH_ID>${crown.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </CrownOffenceType>

          <CppOffenceHeaderType>
            <POH_ID>${cppHeader.POH_ID || "50765"}</POH_ID>
            <OH_OH_ID>${headerData.OH_ID || "579069"}</OH_OH_ID>
            <PNLDOffenceStartDate>${cppHeader.PNLDOffenceStartDate || ""}</PNLDOffenceStartDate>
            <PNLDOffenceEndDate>${cppHeader.PNLDOffenceEndDate || ""}</PNLDOffenceEndDate>
            <VersionNumber>3</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <CSH_CSH_ID>${cppHeader.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </CppOffenceHeaderType>

          <CppOffenceRevisionsType>
            <POR_ID>${cppRevs.POR_ID || "1105464"}</POR_ID>
            <OFR_OFR_ID>${OFR_ID}</OFR_OFR_ID>
            <DateOfLastUpdate>${cppRevs.DateOfLastUpdate || ""}</DateOfLastUpdate>
            <MaxFineTypeMagCtCode>${cppRevs.MaxFineTypeMagCtCode || ""}</MaxFineTypeMagCtCode>
            <MaxFineTypeMagCtDescription>${cppRevs.MaxFineTypeMagCtDescription || ""}</MaxFineTypeMagCtDescription>
            <PNLDStandardOffenceWording>${cppRevs.PNLDStandardOffenceWording || ""}</PNLDStandardOffenceWording>
            <SLPNLDStandardOffenceWording>${cppRevs.SLPNLDStandardOffenceWording || ""}</SLPNLDStandardOffenceWording>
            <ProsecutionTimeLimit>${cppRevs.ProsecutionTimeLimit || ""}</ProsecutionTimeLimit>
            <ModeOfTrial>${cppRevs.ModeOfTrial || "Indictable"}</ModeOfTrial>
            <EndorsableFlag>${cppRevs.EndorsableFlag || "N"}</EndorsableFlag>
            <LocationFlag>${cppRevs.LocationFlag || "Y"}</LocationFlag>
            <PrincipalOffenceCategory>${cppRevs.PrincipalOffenceCategory || ""}</PrincipalOffenceCategory>
            <VersionNumber>3</VersionNumber>
            <ChangedBy>585</ChangedBy>
            <ChangedDate>12/02/2026</ChangedDate>
            <CSH_CSH_ID>${cppRevs.CSH_CSH_ID || headerData.CSH_CSH_ID || ""}</CSH_CSH_ID>
          </CppOffenceRevisionsType>

        </GetOffenceFullResponseType>
      </ns58:GetOffenceFullResponse>
    `;

    // ------------------------------------------------------
    // 6. WS-A HEADER
    // ------------------------------------------------------
    const headerXml = buildWsaHeader({
      action: "getOffenceFull",
      messageId: uuidv4(),
      relatesTo: inboundMessageId
    });

    // ------------------------------------------------------
    // 7. RETURN RESPONSE
    // ------------------------------------------------------
    return soapEnvelope(headerXml, bodyXml);
  };
}

module.exports = { createHandleGetOffenceFull };