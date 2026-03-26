// utils/soapEnvelope.js
function soapEnvelope(headerXml, bodyXml) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope
  xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:addr="http://schemas.xmlsoap.org/ws/2004/08/addressing">

  <soap:Header>
${headerXml}
  </soap:Header>

  <soap:Body>
${bodyXml}
  </soap:Body>

</soap:Envelope>`;
}

module.exports = { soapEnvelope };