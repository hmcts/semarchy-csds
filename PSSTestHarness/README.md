# SOAP Server (PSS Test Harness)

A lightweight Node.js SOAP server for simulating PSS‑style SOAP endpoints. The server exposes a single `/soap` **POST** endpoint and dispatches based on the WS‑Addressing `<wsa:Action>` header inside the SOAP envelope.

Supported actions:
- `createChangeSetHeader` 
- `createReleasePackage`
- `getOffenceFull`
- `getOffenceMenuFull`
- `getOffenceMenuSummary`
- `getOffenceSummary`
- `getReleasePackage`
- `updateChangeSetHeader`
- `updateOffenceFull`
- `updateOffenceMenuFull`
- `updateReleasePackage`

---

## 1) Clone the repository

```bash
git clone <REPOSITORY_URL>.git
cd <PROJECT_FOLDER>
```

> `node_modules/` is intentionally excluded from Git. You will restore it in step 2.

---

## 2) Install dependencies (`node_modules`)

**Standard (developer machines):**
```bash
npm install
```

**CI/Production (faster, locked to `package-lock.json`):**
```bash
npm ci
```

If you hit install issues on Windows:
- Run the terminal **as Administrator**
- `npm cache verify`
- Delete `node_modules` and retry `npm ci`

---

## 3) Start the SOAP server (pass port at runtime)

This project exports a server factory so you can **inject the port** at startup.

Create `runServer.js` (if not already present):

```js
// runServer.js
const { startServer } = require("./server");
const port = process.argv[2] || process.env.PORT || 3000;
startServer(port);
```

Start the server:

```bash
node runServer.js 4000
# or
PORT=4000 node runServer.js
```

**Windows PowerShell**
```powershell
$env:PORT=4000; node runServer.js
```

You should see:
```
SOAP Server running at http://localhost:<PORT>/soap
```

---


## 4) Project structure (key files)

```
soap-server/
├─ actions/
│  ├─ .js file per action
│  ├─ index.js
│
├─ utils/
│  ├─ xmlUtils.js
│  ├─ soapEnvelope.js
│  ├─ wsHeaders.js
│  └─ pkStore.js
│
├─ persisted_keys/
│  └─ menus/
│     └─ {OM_ID}.json             # stored menu definitions
│  └─ offences/
│     └─ {OFR_ID}.json            # stored offence definitions
│  └─ offenceMenus.json           # name → OM_ID mapping
│  └─ offencesIndex.json          # CJS Code → OFR_ID mapping
│  └─ changeSetHeaders.json       # stored change set header definitions
│  └─ releasePackages.json        # stored release package definitions
│
├─ server.js                      # Express server factory (port injected)
├─ runServer.js                   # CLI runner
├─ package.json
├─ package-lock.json
└─ .gitignore
```
---

## Troubleshooting

- **Invalid SOAP XML**: Ensure `Content-Type: text/xml` and send **raw XML**. If handlers log `"[object Object]"`, confirm the action is listed under the raw‑XML routing in `server.js`.
- **LF/CRLF warnings**: Harmless. To enforce consistent endings, add a `.gitattributes`:
  ```gitattributes
  * text=auto eol=lf
  *.json text eol=lf
  ```
- After a fresh clone or pull, always run:
  ```bash
  npm install
  ```

---

## Git hygiene

Keep these committed:
- `package.json`, `package-lock.json`
- `actions/`, `utils/`, `menus/`, `persisted_keys/`

Ignore generated and local files:
```gitignore
node_modules/
*.log
.env
.DS_Store
Thumbs.db
coverage/
```
