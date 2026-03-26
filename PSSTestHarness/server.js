// server.js
const express = require("express");
const bodyParser = require("body-parser");
const xml2js = require("xml2js");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

// ────────────────────────────────────────────────────────────
// ANSI color helpers
// ────────────────────────────────────────────────────────────
const color = {
  reset: "\x1b[0m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  cyan: "\x1b[36m",
  magenta: "\x1b[35m",
  blue: "\x1b[34m",
};

// ────────────────────────────────────────────────────────────
// Persisted keys bootstrap (NO store dependency)
// ────────────────────────────────────────────────────────────
const PERSIST_DIR = process.env.PERSIST_DIR
  ? path.resolve(process.env.PERSIST_DIR)
  : path.resolve(__dirname, "persisted_keys");

// IMPORTANT: Use the exact filenames your validator expects.
const REQUIRED_FILES = [
  "changeSetHeaders.json",
  "offenceMenus.json",
  "offencesIndex.json",
  "releasePackages.json",
];

// Provide a default JSON shape that satisfies validators.
// Adjust fields if your validator requires specific keys.
const DEFAULT_CONTENT = (name) => ({});

// Create directory and required files synchronously (no race conditions).
function bootstrapPersistedFiles() {
  console.log(
    `${color.blue}[INIT]${color.reset} Persist dir: ${PERSIST_DIR} | cwd: ${process.cwd()} | __dirname: ${__dirname}`
  );

  if (!fs.existsSync(PERSIST_DIR)) {
    fs.mkdirSync(PERSIST_DIR, { recursive: true });
    console.log(`${color.green}[INIT] Created directory:${color.reset} ${PERSIST_DIR}`);
  }

  for (const fname of REQUIRED_FILES) {
    const fpath = path.join(PERSIST_DIR, fname);
    if (!fs.existsSync(fpath)) {
      fs.writeFileSync(
        fpath,
        JSON.stringify(DEFAULT_CONTENT(fname), null, 2),
        { encoding: "utf8", flag: "wx" }
      );
      console.log(`${color.green}[INIT] Created file:${color.reset} ${fpath}`);
    } else {
      // Ensure valid JSON; if invalid/empty, repair.
      try {
        const text = fs.readFileSync(fpath, "utf8");
        JSON.parse(text);
      } catch {
        fs.writeFileSync(
          fpath,
          JSON.stringify(DEFAULT_CONTENT(fname), null, 2),
          "utf8"
        );
        console.log(`${color.yellow}[INIT] Repaired invalid JSON:${color.reset} ${fpath}`);
      }
    }
  }
}

// Load files into the in-memory store (Map or similar)
function hydrateStoreFromFiles(store) {
  const files = fs.readdirSync(PERSIST_DIR).filter((f) => f.endsWith(".json"));
  let count = 0;

  for (const fname of files) {
    const fpath = path.join(PERSIST_DIR, fname);
    try {
      const data = JSON.parse(fs.readFileSync(fpath, "utf8"));
      // Choose a sensible key: PK if present, else filename without .json
      const key = String(
        data?.PK !== null && data?.PK !== undefined
          ? data.PK
          : path.basename(fname, ".json")
      );
      store.set(key, data);
      count += 1;
    } catch (e) {
      console.warn(
        `${color.yellow}[INIT] Skipping unreadable JSON:${color.reset} ${fpath} (${e.message})`
      );
    }
  }

  console.log(
    `${color.green}[INIT] Hydrated store${color.reset} with ${count} entries from ${PERSIST_DIR}`
  );
}

// Optionally persist entries back to disk (helper)
function persistEntryToDisk(entry, fileHint) {
  const fileName =
    fileHint ||
    (entry?.PK !== null && entry?.PK !== undefined
      ? `${entry.PK}.json`
      : `${crypto.randomUUID?.() || uuidFallback()}.json`);
  const fpath = path.join(PERSIST_DIR, fileName);
  fs.writeFileSync(fpath, JSON.stringify(entry, null, 2), "utf8");
}

// ────────────────────────────────────────────────────────────
function uuidFallback() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// Escape XML content
function escapeXml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// SOAP Fault envelope
function buildSoapFaultEnvelope({
  faultcode = "soap:Server",
  faultstring = "An error occurred",
  detail = "",
  actionKey = "",
  messageId = "",
  relatesTo = "",
} = {}) {
  const outboundMessageId =
    messageId || `urn:uuid:${crypto.randomUUID?.() || uuidFallback()}`;

  const relatesToTag = relatesTo
    ? `<addr:RelatesTo>${escapeXml(relatesTo)}</addr:RelatesTo>`
    : "";

  return `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope 
  xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:addr="http://schemas.xmlsoap.org/ws/2004/08/addressing">
  <soap:Header>
    <addr:Action>${escapeXml(
      actionKey + "/Fault/RequestProcessingException"
    )}</addr:Action>
    <addr:MessageID>${escapeXml(outboundMessageId)}</addr:MessageID>
    <addr:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</addr:To>
    ${relatesToTag}
  </soap:Header>
  <soap:Body>
    <soap:Fault>
      <faultcode>${escapeXml(faultcode)}</faultcode>
      <faultstring>${escapeXml(faultstring)}</faultstring>
      ${detail ? `<detail>${escapeXml(detail)}</detail>` : ""}
    </soap:Fault>
  </soap:Body>
</soap:Envelope>`;
}

// Fallback wrapper used only if handler returns JS object/string
function wrapSoapResponse(payload = {}) {
  const body = typeof payload === "string" ? payload : JSON.stringify(payload);
  return `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Response>${escapeXml(body)}</Response>
  </soap:Body>
</soap:Envelope>`;
}

// Extract WS-A Action (namespace-agnostic)
function getPropByVariants(obj, variants = []) {
  if (!obj) return undefined;

  // Direct hit on provided variants
  for (const key of variants) {
    if (obj[key] !== undefined) return obj[key];
  }

  // Namespace-insensitive: compare suffixes
  const suffixes = variants.map((v) => v.split(":").pop());
  for (const key of Object.keys(obj)) {
    const suffix = key.split(":").pop();
    if (suffixes.includes(suffix)) return obj[key];
  }

  return undefined;
}

// ────────────────────────────────────────────────────────────
// SOAP Server entry point
// ────────────────────────────────────────────────────────────
function startServer(port) {
  // 1) Create the in-memory store first
  const store = new Map();

  // 2) Ensure files/dir exist BEFORE loading handlers/validators
  bootstrapPersistedFiles();

  // 3) Hydrate store from those files
  hydrateStoreFromFiles(store);

  // 4) Load action handlers AFTER bootstrap.
  //    Supports both:
  //      - module.exports = new Map([ [action, handler], ... ])
  //      - module.exports = (store) => new Map(...)
  const ACTIONS_MODULE = require("./actions");
  const ACTION_HANDLERS =
    typeof ACTIONS_MODULE === "function"
      ? ACTIONS_MODULE(store, { persistEntryToDisk, PERSIST_DIR })
      : ACTIONS_MODULE;

  // 5) Spin up Express
  const app = express();

  // Accept SOAP/XML as plain text so handlers get RAW XML when needed
  app.use(bodyParser.text({ type: ["text/xml", "application/soap+xml"] }));

  const LOG_XML = process.env.LOG_XML === "true";

  // Shared SOAP request processor
  async function processSoapRequest(xml, res) {
    const startTime = Date.now();
    console.log(
      `${color.blue}──────────────────────────────────────────────────────────${color.reset}`
    );
    console.log(`${color.magenta}[SOAP REQUEST RECEIVED]${color.reset}`);

    if (LOG_XML) {
      console.log(`${color.yellow}[RAW XML]${color.reset}`);
      console.log(xml);
    }

    // Parse just enough to detect action/header
    let parsed;
    let header = {};
    let messageId = "";
    let relatesTo = "";

    try {
      parsed = await new xml2js.Parser({
        explicitArray: false,
        ignoreAttrs: false,
        tagNameProcessors: [xml2js.processors.stripPrefix],
      }).parseStringPromise(xml);

      const envelope = parsed?.Envelope || parsed;
      header = envelope?.Header || {};

      messageId =
        getPropByVariants(header, ["wsa:MessageID", "MessageID", "addr:MessageID"]) ||
        "";
      relatesTo =
        getPropByVariants(header, ["wsa:RelatesTo", "RelatesTo", "addr:RelatesTo"]) ||
        messageId;
    } catch (err) {
      console.error(`${color.red}[XML PARSE ERROR]${color.reset}`, err);
      const fault = buildSoapFaultEnvelope({
        faultstring: "Invalid SOAP XML",
        actionKey: "UnknownAction",
      });
      return res.status(500).set("Content-Type", "text/xml").send(fault);
    }

    const envelope = parsed.Envelope || parsed;
    const body = envelope.Body || {};

    // Extract WS-A Action
    const actionKey = getPropByVariants(header, ["wsa:Action", "Action"]);
    if (!actionKey) {
      const fault = buildSoapFaultEnvelope({
        faultstring: "Missing WS-Addressing Action",
        actionKey: "UnknownAction",
        relatesTo: messageId,
      });
      return res.status(500).set("Content-Type", "text/xml").send(fault);
    }

    console.log(`${color.magenta}[SOAP ACTION]${color.reset} ${actionKey}`);

    const handler = ACTION_HANDLERS.get(actionKey);
    if (!handler) {
      const fault = buildSoapFaultEnvelope({
        faultstring: `Unsupported Action: ${actionKey}`,
        actionKey,
        relatesTo: messageId,
      });
      return res.status(500).set("Content-Type", "text/xml").send(fault);
    }

    // Actions that want RAW XML instead of parsed envelope
    const rawXmlActions = [
      "updateOffenceMenuFull",
      "getOffenceMenuSummary",
      "getOffenceMenuFull",
      "updateChangeSetHeader",
      "updateOffenceFull",
      "getOffenceSummary",
      "getOffenceFull",
    ];

    try {
      console.log(`${color.yellow}[EXECUTING HANDLER]${color.reset} ${actionKey}`);

      const result = rawXmlActions.includes(actionKey)
        ? await handler(xml, { store, persistEntryToDisk, PERSIST_DIR })
        : await handler({ envelope, header, body, raw: parsed }, { store, persistEntryToDisk, PERSIST_DIR });

      // Handler may explicitly return a fault object
      if (result && typeof result === "object" && result.fault) {
        const faultInfo = result.fault;
        const fault = buildSoapFaultEnvelope({
          faultstring: faultInfo.faultstring || "Handler Error",
          faultcode: faultInfo.faultcode || "soap:Server",
          detail: faultInfo.detail || "",
          actionKey,
          relatesTo: messageId,
        });
        return res.status(500).set("Content-Type", "text/xml").send(fault);
      }

      // Normal response
      const responseXml = typeof result === "string" ? result : wrapSoapResponse(result);

      res.set("Content-Type", "text/xml").send(responseXml);

      console.log(`${color.green}[RESPONSE XML SENT]${color.reset}`);
      console.log(
        `${color.cyan}[REQUEST COMPLETE]${color.reset} ${Date.now() - startTime}ms`
      );
    } catch (err) {
      console.error(`${color.red}[HANDLER ERROR]${color.reset}`, err);

      const fault = buildSoapFaultEnvelope({
        faultstring: err?.message || "Internal server error",
        actionKey,
        relatesTo: messageId,
      });

      return res.status(500).set("Content-Type", "text/xml").send(fault);
    }
  }

  // SOAP endpoint
  app.post("/soap", async (req, res) => {
    const xml = typeof req.body === "string" ? req.body : "";
    return processSoapRequest(xml, res);
  });

  // Start server
  app.listen(port, () => {
    console.log(
      `${color.green}SOAP Server running at http://localhost:${port}/soap${color.reset}`
    );
  });
}

module.exports = { startServer, bootstrapPersistedFiles  };