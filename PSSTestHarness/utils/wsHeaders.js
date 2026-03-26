// utils/wsHeaders.js
const { v4: uuidv4 } = require("uuid");

/**
 * Build WS-Addressing header XML.
 * - If relatesTo is provided, include <addr:RelatesTo>.
 * - If messageId is not provided, a new UUID will be generated.
 */
function buildWsaHeader({ action, messageId, relatesTo, to = "pss_sdui", from = "pss_db" }) {
  const mid = messageId || uuidv4();
  const relatesToXml = relatesTo
    ? `\n    <addr:RelatesTo>${relatesTo}</addr:RelatesTo>`
    : "";

  return `
    <addr:Action>${action}</addr:Action>
    <addr:MessageID>${mid}</addr:MessageID>
    <addr:To>${to}</addr:To>${relatesToXml}
    <addr:From>
      <addr:Address>${from}</addr:Address>
    </addr:From>
  `;
}

module.exports = { buildWsaHeader };