// runServer.js
const { startServer, bootstrapPersistedFiles } = require("./server");

// Default or argument‑based port
const port = process.argv[2] || 3000;

// 1. Run file creation FIRST (synchronous, no race conditions)
bootstrapPersistedFiles();

// 2. Start the SOAP server
startServer(port);