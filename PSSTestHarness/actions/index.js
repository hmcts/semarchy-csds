// actions/index.js

const { createStore } = require("../utils/pkStore");

// Release Package Handlers
const { createHandleCreateReleasePackage } = require("./createReleasePackage");
const { createHandleUpdateReleasePackage } = require("./updateReleasePackage");
const { createHandleGetReleasePackage } = require("./getReleasePackage");

// ChangeSetHeader Handlers
const { createHandleCreateChangeSetHeader } = require("./createChangeSetHeader");
const { createHandleUpdateChangeSetHeader } = require("./updateChangeSetHeader");

// Offence Menu + Offence Handlers
const { createHandleUpdateOffenceMenuFull } = require("./updateOffenceMenuFull");
const { createHandleGetOffenceMenuSummary } = require("./getOffenceMenuSummary");
const { createHandleGetOffenceMenuFull } = require("./getOffenceMenuFull");

// Update Offence Full
const { createHandleUpdateOffenceFull } = require("./updateOffenceFull");
const { createHandleGetOffenceSummary } = require("./getOffenceSummary");
const { createHandleGetOffenceFull } = require("./getOffenceFull");

// ---------------------------
// LOAD STORES
// ---------------------------

// Release package store (PK–keyed JSON)
const releasePkgPath =
  process.env.RELEASE_PKG_STORE_PATH ||
  "persisted_keys/releasePackages.json";

// ChangeSetHeader store
const changeSetPath =
  process.env.CHANGESET_STORE_PATH ||
  "persisted_keys/changeSetHeaders.json";

const releasePackageStore = createStore({
  type: process.env.PK_STORE_TYPE || "file",
  filePath: releasePkgPath,
  fallbackFileName: "releasePackages.json"
});

const changeSetHeaderStore = createStore({
  type: process.env.PK_STORE_TYPE || "file",
  filePath: changeSetPath,
  fallbackFileName: "changeSetHeaders.json"
});

// ---------------------------
// CREATE HANDLERS
// ---------------------------

// Release Package
const handleCreateReleasePackage =
  createHandleCreateReleasePackage(releasePackageStore);

const handleUpdateReleasePackage =
  createHandleUpdateReleasePackage(releasePackageStore);

const handleGetReleasePackage =
  createHandleGetReleasePackage(releasePackageStore);

// ChangeSetHeader
const handleCreateChangeSetHeader =
  createHandleCreateChangeSetHeader(changeSetHeaderStore);

const handleUpdateChangeSetHeader =
  createHandleUpdateChangeSetHeader(changeSetHeaderStore);

// Offence Menu
const handleUpdateOffenceMenuFull =
  createHandleUpdateOffenceMenuFull();

const handleGetOffenceMenuSummary =
  createHandleGetOffenceMenuSummary();

const handleGetOffenceMenuFull =
  createHandleGetOffenceMenuFull();

// Offence Full
const handleUpdateOffenceFull =
  createHandleUpdateOffenceFull();

const handleGetOffenceSummary = createHandleGetOffenceSummary();
const handleGetOffenceFull = createHandleGetOffenceFull();


// ---------------------------
// EXPORT ACTION MAP
// ---------------------------

module.exports = new Map([
  // Release Package Actions
  ["createReleasePackage", handleCreateReleasePackage],
  ["updateReleasePackage", handleUpdateReleasePackage],
  ["getReleasePackage", handleGetReleasePackage],

  // ChangeSetHeader
  ["createChangeSetHeader", handleCreateChangeSetHeader],
  ["updateChangeSetHeader", handleUpdateChangeSetHeader],

  // Offence Menu
  ["updateOffenceMenuFull", handleUpdateOffenceMenuFull],
  ["getOffenceMenuSummary", handleGetOffenceMenuSummary],
  ["getOffenceMenuFull", handleGetOffenceMenuFull],

  // Offence Full Action
  ["updateOffenceFull", handleUpdateOffenceFull],
  ["getOffenceSummary", handleGetOffenceSummary],
  ["getOffenceFull", handleGetOffenceFull]
]);
