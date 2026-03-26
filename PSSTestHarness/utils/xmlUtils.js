// utils/xmlUtils.js
// Shared XML + JSON utilities used by updateOffenceMenuFull and other actions.

const fs = require("fs");
const path = require("path");

/**
 * Generate a random positive 31-bit integer
 */
function randomId() {
  return Math.floor(Math.random() * 0x7fffffff);
}

/**
 * Cleans XML:
 * - Removes BOM
 * - Strips text before first "<"
 * - Trims whitespace
 */
function cleanXml(xml) {
  if (typeof xml !== "string") xml = String(xml ?? "");
  // Strip BOM (Byte Order Mark)
  if (xml.charCodeAt(0) === 0xfeff) {
    xml = xml.slice(1);
  }
  // Remove any characters before first '<'
  const firstTag = xml.indexOf("<");
  if (firstTag > 0) {
    xml = xml.slice(firstTag);
  }
  return xml.trim();
}

/**
 * Convert to integer with optional random fallback.
 * allowNull: return null when value is absent
 * fallbackToRandom: generate integer when value is missing/bad
 */
function toInt(value, { fallbackToRandom = false, allowNull = false } = {}) {
  if (value === undefined || value === null || value === "") {
    if (allowNull) return null;
    return fallbackToRandom ? randomId() : null;
  }
  // Already a number?
  const n = Number(value);
  if (Number.isInteger(n)) return n;

  const parsed = parseInt(String(value), 10);
  if (!Number.isNaN(parsed)) return parsed;

  return fallbackToRandom ? randomId() : null;
}

/**
 * Guarantee an array (single or undefined → [])
 */
function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

/**
 * Ensure directory exists
 */
function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Safe JSON file reader
 */
function readJsonSafe(filePath, fallback) {
  try {
    if (fs.existsSync(filePath)) {
      const text = fs.readFileSync(filePath, "utf8");
      return JSON.parse(text);
    }
  } catch (err) {
    // swallow errors — return fallback
  }
  return fallback;
}

/**
 * Safe JSON file writer
 */
function writeJson(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf8");
}

module.exports = {
  randomId,
  cleanXml,
  toInt,
  asArray,
  ensureDir,
  readJsonSafe,
  writeJson
};