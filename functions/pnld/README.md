
# Azure Functions – ZIP Extraction & PNLD Processing

This repository contains two Azure Functions that support automated data ingestion for **Semarchy**. Both functions are triggered by Semarchy Data Notifications and return processed or validated data back to Semarchy for further ingestion.
---

## **Local Variables**
The following Local Variables are required for the Azure Function to work as expected
- SemarchyBaseURL
- SemarchyAPIKey

---

## `zip_extract`

### **Trigger**
HTTP Trigger  
Called when Semarchy uploads a ZIP file into the entity **`SourceZIP`**.

### **Purpose**
Extract XML files from an incoming ZIP provided within the Semarchy Data Notification.

### **Process Overview**
- Receives a ZIP file embedded in the HTTP request body.  
- Extracts all files from the ZIP.  
- **Non‑XML files are ignored.**  
- XML files are collected and packaged into an API call back to Semarchy.  

### **Output**
A response containing the extracted XML files, sent back to Semarchy for ingestion.

---

## `pnld_process`

### **Trigger**
HTTP Trigger  
Called when Semarchy uploads XML files into the entity **`SourceFile`**.

### **Purpose**
Validate and transform XML files containing Offence and Menu information before returning them to Semarchy.

### **Process Overview**
- Receives one or more XML files in the request body.  
- Validates and transforms each file **in parallel** to improve performance.  
- Extracts Offence + Menu details from each XML file.  
- If processing succeeds:
  - The transformed data is collated and returned to Semarchy.
- If processing fails:
  - An error message is returned to Semarchy indicating the failure.

### **Output**
- **Success:** Transformed Offence and Menu data ready for ingestion.  
- **Failure:** Error details for any XML files that did not pass validation.  

---

## Summary

Both Azure Functions:

- Are triggered by Semarchy Data Notifications  
- Process XML content received directly in the HTTP body  
- Return either validated/transformed data or error responses back to Semarchy  
- Enable automated, reliable ingestion of ZIP and XML files into Semarchy MDM  

