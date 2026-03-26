// utils/pkStore.js
const fs = require("fs");
const path = require("path");

/**
 * Create a key/value store for idempotency keys.
 * Options:
 *  - type: "file" | "memory"  (default "file" unless PK_STORE_TYPE is set)
 *  - filePath: path to JSON file (default based on caller; can be overridden via env)
 */
function createStore(options = {}) {
  const type = (options.type || process.env.PK_STORE_TYPE || "file").toLowerCase();

  if (type === "memory") {
    const mem = {};
    return {
      type: "memory",
      has: (k) => Object.prototype.hasOwnProperty.call(mem, k),
      get: (k) => mem[k],
      set: (k, v) => { mem[k] = v; },
    };
  }

  // File-backed store
  const fallbackPath = options.fallbackFileName
    ? path.join(process.cwd(), options.fallbackFileName)
    : path.join(process.cwd(), "pkStore.json");

  const filePath = options.filePath || fallbackPath;

  function ensureFile() {
    try {
      if (!fs.existsSync(filePath)) {
        fs.writeFileSync(filePath, "{}", { encoding: "utf8" });
      }
    } catch (e) {
      console.error("Error ensuring PK store file:", e);
    }
  }

  function load() {
    ensureFile();
    try {
      const raw = fs.readFileSync(filePath, "utf8");
      return JSON.parse(raw || "{}");
    } catch (e) {
      console.error("Error loading PK store:", e);
      return {};
    }
  }

  function save(obj) {
    try {
      fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), { encoding: "utf8" });
    } catch (e) {
      console.error("Error saving PK store:", e);
    }
  }

  let cache = load();

  return {
    type: "file",
    filePath,
    has: (k) => Object.prototype.hasOwnProperty.call(cache, k),
    get: (k) => cache[k],
    set: (k, v) => { cache[k] = v; save(cache); },
  };
}

module.exports = { createStore };