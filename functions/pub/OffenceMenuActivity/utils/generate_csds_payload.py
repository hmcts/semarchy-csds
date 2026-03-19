
def generate_omo_payload(csds_details, pss_details):
        
    # Build OptionNumber → OffenceMenuOptionsID
    csds_mapping = {}
    for opt in csds_details.get("OTEMenuOptions", []):
        opt_no = opt.get("OptionNumber")
        opt_id = opt.get("OffenceMenuOptionsID")
        if opt_no is not None and opt_id is not None:
            csds_mapping[int(opt_no)] = int(opt_id)

    # Build OptionNumber → PSS OMO_ID
    pss_map = {}
    for opt in pss_details.get("OTEMenuOptions", []):
        opt_no = opt.get("OptionNumber")
        omo_id = opt.get("OMO_ID")   # PSSOffenceMenuOptionsID
        if opt_no is not None and omo_id is not None:
            pss_map[int(opt_no)] = int(omo_id)

    # Combine both into a final array
    omo_payload = []
    for opt_no, offence_id in csds_mapping.items():
        omo_payload.append({
            "PSSOffenceMenuOptionsID": int(pss_map.get(opt_no)),
            "OffenceMenuOptionsID": int(offence_id)
        })

    return omo_payload

def generate_oed_payload(csds_details, pss_details):
    csds_map = {}
    for opt in csds_details.get("OTEMenuOptions", []):
        opt_no = opt.get("OptionNumber")
        if opt_no is None:
            continue
        for elem in opt.get("OffenceMenuElementDefinitions", []):
            el_no = elem.get("ElementNumber")
            el_def_id = elem.get("OffenceMenuElementDefinitionID")
            if el_no is None or el_def_id is None:
                continue
            csds_map[(int(opt_no), int(el_no))] = int(el_def_id)

    # 2) Walk PSS and join on (OptionNumber, ElementNumber)
    oed_payload = []
    for opt in pss_details.get("OTEMenuOptions", []):
        opt_no = opt.get("OptionNumber")
        if opt_no is None:
            continue
        for elem in opt.get("OTEMenuOptionElements", []):
            el_no = elem.get("ElementNumber")
            omo_id = elem.get("OMOP_ID")     # PSSOffenceMenuOptionElementDefinitionID
            oed_id = elem.get("OED_ID")      # PSSOffenceMenuElementDefinitionID
            if el_no is None or omo_id is None or oed_id is None:
                continue

            key = (int(opt_no), int(el_no))
            csds_el_def_id = csds_map.get(key)

            # Only include if we can match to a CSDS element definition
            if csds_el_def_id is not None:
                oed_payload.append({
                    "OffenceMenuElementDefinitionID": int(csds_el_def_id),
                    "PSSOffenceMenuElementDefinitionID": int(oed_id),
                    "PSSOffenceMenuOptionElementDefinitionID": int(omo_id),
                })
    return oed_payload


