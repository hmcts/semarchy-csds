
# Publishing Orchestration Overview

## Process: Publishing Orchestration

### Overview
The **HTTP Starter** function is configured to accept the name of an orchestrator. When triggered, it launches the corresponding Durable Function orchestrator.

This HTTP Starter is invoked automatically through two **Semarchy Data Notifications**:

1. **Urgent Notification** – Triggered when a Release Package is marked as **Urgent**.
2. **Overnight Notification** – Triggered for Release Packages that:
   - Are set to **Publish to Live**, and
   - Have a **Publish Date in the past**.

### Orchestrators
The HTTP Starter may launch one of two orchestrators based on routing logic:

#### 1. **NonPSS Orchestrator**
- Receives the full Release Package payload.
- Automatically updates the Release Package status to **Published** in Semarchy.
- No external system calls are required.

#### 2. **PSS Orchestrator**
- Receives the Release Package payload.
- Sends the data to **PSS** for further processing.
- Updates Semarchy with the outcome once PSS completes processing.

---

## Process: Status Timer

### Overview
The **Status Timer** is a time‑based Durable Function configured to run **every night at midnight**.

### Purpose
It sends a status‑update job to Semarchy that:
- Reviews all **Offence** and **Offence Menu** records.
- Automatically updates their statuses based on:
  - **Offence Start Date**
  - **Offence End Date**

This ensures data consistency and accurate operational status for all relevant entities.

---

## Local Development Settings

When running the solution locally, the following **local.settings.json** values must be defined under the `Values` section:

```
SemarchyBaseURL
SemarchyAPIKey
PSSBaseURL
UrgentWaitPeriodSeconds
```

These key names must exist, but local developers should supply their own values.

---

## Summary
| Component | Trigger | Purpose |
|----------|---------|---------|
| **HTTP Starter** | Semarchy Data Notification | Launch the correct orchestrator (PSS or NonPSS) |
| **NonPSS Orchestrator** | HTTP Starter | Auto‑publish Release Packages in Semarchy |
| **PSS Orchestrator** | HTTP Starter | Integrate with PSS, then update Semarchy |
| **Status Timer** | Midnight Daily | Keep Offence & Menu statuses aligned with effective dates |

---
