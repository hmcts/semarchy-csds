
import logging 
from datetime import datetime

def generate_offence_attributes(record):

    
    ### ENDORSABLE TRANSFORMATION
    if record.get('dvlacode') is None or record['dvlacode'].startswith('NE'):
        record['G_endorsable'] = 'N'
    else:
        record['G_endorsable'] = 'Y'
    

    ### HOCLASS TRANSFORMATION
    # Defaults
    record['G_hoclass'] = None
    record['G_hosubclass'] = None

    raw = record.get('hoclassification')

    if raw:
        text = str(raw).strip()

        # Only allow '/' as the delimiter per your snippet
        if '/' in text and text.count('/') == 1:
            left, right = (p.strip() for p in text.split('/'))

            try:
                # Attempt to parse both sides as integers
                g_class = int(left)
                g_subclass = int(right)

                # If both parse fine, assign them; else leave as None
                record['G_hoclass'] = g_class
                record['G_hosubclass'] = g_subclass

            except (ValueError, TypeError):
                # Non-integer values encountered — keep defaults as None
                pass
        else:
            # No valid single '/' delimiter — keep defaults as None
            pass

            


    ### MODE OF TRIAL TRANSFORMATION
    raw_cjs = record.get('cjsoffencecategory')
    cjs_prefix = str(raw_cjs).strip().upper()[:2] if raw_cjs is not None else None

    if cjs_prefix == 'CE':
        record['G_modeoftrial'] = 'Either Way'
    elif cjs_prefix == 'CI':
        record['G_modeoftrial'] = 'Indictable'
    elif cjs_prefix in ('CM', 'CS'):
        record['G_modeoftrial'] = 'Summary'
    else:
        record['G_modeoftrial'] = None


    ### LOCATION FLAG TRANSFORMATION
    sow_raw = record.get('sow_raw')
    if sow_raw and 'SPECIFY TOWNSHIP' in sow_raw:
        record['G_locationflag'] = 'Y'
    else:
        record['G_locationflag'] = 'N'

    

    ### MAXIMUM PENALTY TRANSFORMATION
    maxCustSentLen = record.get('maxcustodialsentencelengthmagct')
    maxCustSentUnit = record.get('maxcustodialsentenceunitmagct')
    maxFineType = record.get('maxfinetypemagct_code')
    maxFineMag = record.get('maxfinemagct')
    disqualificationClass = record.get('disqualificationclass')
    minPenaltyPoints = record.get('minpenaltypoints')
    maxPenaltyPoints = record.get('maxpenaltypoints')
    code = record.get('cjsoffencecategory')

    # Check if code is one of the main categories
    if code in ["CS", "CM", "CE", "CI"]:

        # Define mode of trial
        if code in ["CS", "CM"]:
            maxPenalty = "S:"
        elif code == "CE":
            maxPenalty = "EW:"
        elif code == "CI":
            maxPenalty = "Indictable only"
        else:
            maxPenalty = ""

        # Define imprisonment
        try:
            if maxCustSentLen is not None:
                maxCustSentLen = int(maxCustSentLen)
                if maxCustSentLen > 0:
                    if maxCustSentUnit == "Days":
                        maxCustSentUnit = "D"
                    elif maxCustSentUnit == "Weeks":
                        maxCustSentUnit = "W"
                    elif maxCustSentUnit == "Months":
                        maxCustSentUnit = "M"

                    maxPenalty += f"{maxCustSentLen}{maxCustSentUnit}"

                    if maxFineType:
                        maxPenalty += " &/or "
        except ValueError:
            pass  # Ignore invalid number format

        # Define fine type
        numMaxFineType = 0
        try:
            numMaxFineType = int(maxFineType)
        except (ValueError, TypeError):
            pass  # Must be character fine type

        # Define fine
        if maxFineType in ["S", "U"]:
            maxPenalty += "Ultd Fine"
        elif maxFineType == "O":
            maxPenalty = ""
            try:
                if maxFineMag and float(maxFineMag) > 0:
                    maxPenalty = f"£{maxFineMag}"
            except ValueError:
                pass
        elif numMaxFineType > 0:
            maxPenalty += f"L{maxFineType}"
        elif maxFineMag and float(maxFineMag) > 0:
            maxPenalty += f"£{maxFineMag}"

        # Define driving penalties
        if disqualificationClass == "O":
            maxPenalty += "Oblig disq LE "

        # Define penalty points
        try:
            if minPenaltyPoints is not None:
                minPenaltyPoints = int(minPenaltyPoints)
                if minPenaltyPoints > 0:
                    if maxPenaltyPoints:
                        maxPenalty += f" LE {minPenaltyPoints}-"
                    else:
                        maxPenalty += f" LE {minPenaltyPoints}pp"
        except ValueError:
            pass

        try:
            if maxPenaltyPoints is not None:
                maxPenalty += f"{maxPenaltyPoints}pp"
        except ValueError:
            pass


    elif code in ["CB", "CR"]:
        maxPenalty = ""
    else:
        maxPenalty=None
    record['G_maxpenalty']=maxPenalty

    return record


def define_offence(record, terminal_entries, ingestion_type, uploaded_by, rp_id):

    current_time = datetime.utcnow()
    current_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    record = generate_offence_attributes(record)

    boolean_attributes = ['custodialindicator']
    for b in boolean_attributes:
        val = str(record.get(b, '')).strip().upper()
        if val in ['Y', 'YES', '1']:
            record[b] = 'No'
        elif val in ['N', 'NO', '0']:
            record[b] = 'Yes'
        else:
            record[b] = None  # Explicitly set None for invalid or empty values


    output_record = {
               # "OffenceRevisionID": 0,
                "Recordable": record['recordable'], #No
                "Reportable": record['reportable'], #No
                "CJSTitle": record['title_CLEANSED'],
                "CustodialIndicator": record['custodialindicator'], #"No"
                "DateUsedFrom": record['offencestartdate'],
                "DateUsedTo": record['offenceenddate'],
                "StandardList": "No",
                # "TrafficControl": True,
                # "VersionNumber": 0,
                "ChangedBy": uploaded_by,
                "ChangedDate": current_time,
                "DVLACode": record['dvlacode'],
                "OffenceNotes":  record['notes'],
                "MaximumPenalty": record['G_maxpenalty'],
                "Description": record['title_CLEANSED'],
                # "DerivedFromCJSCode": "string",
                "HOClass": record['G_hoclass'],
                "HOSubClass": record['G_hosubclass'],
                # "ProceedingsCode": 0,
                "SecondLanguageCJSTitle": record['welshoffencetitle'],
                "PNLDStandardOffenceWording": record['standardoffencewording_CLEANSED'], # raw or cleansed or final?
                "PNLDWelshStandardOffenceWording": record['welshstandardoffencewording'],
                "PNLDDateOfLastUpdate": record['dateoflastupdate'],
                "PNLDProsecutionTimeLimit": record['timelimitforprosecutions'],
                "PNLDMaxFineTypeMagistratesCourt": record['maxfinetypemagct_code'],
                "PNLDMaxFineTypeMagistratesCourtDescription": record['maxfinetypemagct_desc'],
                "PNLDModeOfTrial": record['G_modeoftrial'],
                "PNLDEndorsableFlag": record['G_endorsable'],
                "PNLDLocationFlag": record['G_locationflag'],
                "PNLDPrincipalOffenceCategory": record['cjsoffencecategory'],
                "UserOffenceWording": record['standardoffencewording'],
                "UserStatementOfFacts": record['standardstatementoffacts'],
                "UserActsAndSection": record['legislation'],
                # "EntryPromptSubstitutionSOW": "string",
                # "EntryPromptSubstitutionSOF": "string",
                # "EntryPromptSubstitutionANS": "string",
                # "CurrentEditor": "string",
                "CJSCode": record['cjsoffencecode'],
                # "Area": 0,
                "Blocked": "N",
                "SecondLanguageOffenceStatementOfFactsText": record['welshstandardstatementoffacts'],
                "SecondLanguageOffenceWordingText": record['welshstandardoffencewording'],
                "SecondLanguageOffenceActAndSectionText": record['welshlegislation'],
                "OffenceCode": 0,
                "PNLDOffenceStartDate": record['offencestartdate'],
                "PNLDOffenceEndDate": record['offenceenddate'],
                "SOWReference": record['pnldref'],
                # "ClonedFrom": "string",
                # "CloneTypeCode": "string",
                # "SysClonedTo": 0,
                # "SysShowDeleted": True,
                "AuthoringStatus": "Final",
                "PublishingStatus": "Not Published",
                "OffenceType": record['cjsoffencecategory'],
                "OffenceSource": "PNLD",
                "MISClassification": record['miscode'],
                "OffenceClass": "S",
                # "CanBeBulk": True,
                # "InitialFeeApplicable": True,
                # "ContestedFee": True,
                # "ApplicationSynonym": "string",
                # "Exparte": True,
                # "Jurisdiction": "Crown",
                # "AppealFlag": True,
                # "SummonsTemplateType": "BREACH",
                # "LinkType": "FIRST_HEARING",
                # "HearingCode": "Appeal",
                # "ApplicantAppellantFlag": "Appellant",
                # "PleaApplicableFlag": True,
                # "ActiveOffenceOrder": "COURT_ORDER",
                # "CommissionerOfOath": True,
                # "BreachType": "COMMISSION_OF_NEW_OFFENCE_BREACH",
                # "CourtOfAppealFlag": True,
                # "CourtExtractAvailable": True,
                # "ListingNotificationTemplate": "NOT_APPLICABLE",
                # "BoxworkNotificationTemplate": "string",
                # "ProsecutorAsThirdPartyFlag": True,
                # "ResentencingActivationCode": "string",
                # "Prefix": "string",
                "ObsoleteIndicator": "N",
                "PNLDHashMD5": record['md5_hash'],
                "VersionType": ingestion_type,
                'FID_ReleasePackage': rp_id
            }
    

    output_record.update(terminal_entries)

    logging.debug(output_record)

    return output_record