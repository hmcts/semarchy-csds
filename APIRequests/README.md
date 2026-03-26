
# API Requests

The JSON files within this directory contain the BODY for the API requests that are used to set configuration within the CSDS Semarchy Instance.

For all API Request it is the Semarchy Base URL is to be used (e.g. https://csds.env.apps.hmcts.net/api/rest)

---

## **DataNotificationLoad.json**
Generates all Data Notifications that are utilised by the CSDS.

**IMPORTANT NOTE:** When this API Requerst is executed, all Data notifications are deleted and remade with the onfgiuration within the BODY of the request.

**Type of Request: PUT**

**Request URL: {BaseURL}/app-builder/data-locations/CSDS/data-notifications**

Data Notifications Generated:
- **SourceFileLandedNotification**
    - RequestURL: PNLD Azure Function Endpoint for pnld_process
- **SourceZIPLandedNotification**
    - RequestURL: PNLD Azure Function Endpoint for zip_extract
- **UrgentReleasePackageDataNotification**
    - RequestURL: Publishing Azure Function Endpoint for start/*NonPSSOrchestrator*
- **OvernightReleasePackageDataNotification**
    - RequestURL: Publishing Azure Function Endpoint for start/*NonPSSOrchestrator*

**IMPORTANT NOTE:** The Azure Function Endpoint for the Publishing Azure Function will have a format of start/{OrchestratorName}, the OrchestratorName should be populated as highlighted above.

---


## **ReferenceDataLoad.json**
Populates all reference data for CSDS Semarchy.

**Type of Request: POST**

**Request URL: {BaseURL}/loads/CSDS**
