from HHAExchange.APIkeys import app_name, app_secret, app_key
import xml.etree.ElementTree as ET
from HHAExchange.get_requests import get_caregiver_id, get_caregiver_medicals
from HHAExchange.asynchronous import retry_soap_request
from datetime import datetime


def has_completed_medical_in_year(xml_string, target_medical_id, target_year):
    ns = {"ns": "https://www.hhaexchange.com/apis/hhaws.integration"}
    root = ET.fromstring(xml_string)

    for medical in root.findall(".//ns:CaregiverMedicalDetails", ns):
        medical_id = medical.findtext("ns:MedicalID", namespaces=ns)
        due_date_text = medical.findtext("ns:DueDate", namespaces=ns)
        status = medical.findtext("ns:Status", namespaces=ns)

        if str(medical_id) != str(target_medical_id):
            continue

        if not due_date_text or not status:
            continue

        due_date = datetime.strptime(due_date_text, "%Y-%m-%d")

        if due_date.year == target_year and status.strip().lower() == "completed":
            return True

    return False


def get_latest_pending_or_overdue_medical_id(xml_string, target_medical_id, target_year):
    ns = {"ns": "https://www.hhaexchange.com/apis/hhaws.integration"}
    root = ET.fromstring(xml_string)

    latest_due_date = None
    latest_caregiver_medical_id = None

    for medical in root.findall(".//ns:CaregiverMedicalDetails", ns):
        medical_id = medical.findtext("ns:MedicalID", namespaces=ns)
        caregiver_medical_id = medical.findtext("ns:CaregiverMedicalID", namespaces=ns)
        due_date_text = medical.findtext("ns:DueDate", namespaces=ns)
        status = medical.findtext("ns:Status", namespaces=ns)

        if str(medical_id) != str(target_medical_id):
            continue

        if not caregiver_medical_id or not due_date_text or not status:
            continue

        due_date = datetime.strptime(due_date_text, "%Y-%m-%d")
        status_clean = status.strip().lower()

        if due_date.year != target_year:
            continue

        if status_clean not in {"pending", "overdue"}:
            continue

        if latest_due_date is None or due_date > latest_due_date:
            latest_due_date = due_date
            latest_caregiver_medical_id = caregiver_medical_id

    return latest_caregiver_medical_id


async def process_update_only_medical(caregiver_code, medical_id, date_performed, result_id, target_year=2026):
    print(caregiver_code)

    try:
        caregiver_id = await get_caregiver_id(caregiver_code)
        if not caregiver_id:
            return caregiver_code, False, "Invalid Caregiver ID"

        all_medicals = await get_caregiver_medicals(caregiver_id)

        if has_completed_medical_in_year(all_medicals, medical_id, target_year):
            return caregiver_code, False, f"SKIPPED: Completed medical already exists in {target_year}"

        caregiver_medical_id = get_latest_pending_or_overdue_medical_id(
            all_medicals,
            medical_id,
            target_year
        )

        if caregiver_medical_id:
            return await send_update_medical(
                caregiver_code,
                caregiver_id,
                caregiver_medical_id,
                medical_id,
                date_performed,
                result_id
            )

        return caregiver_code, False, f"Could not find medical in {target_year}"

    except Exception as e:
        return caregiver_code, False, str(e)


def get_latest_overdue_caregiver_medical_id(xml_string, target_medical_id):
    ns = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "ns": "https://www.hhaexchange.com/apis/hhaws.integration"
    }

    root = ET.fromstring(xml_string)

    latest_due_date = None
    latest_caregiver_medical_id = None

    cutoff_date = datetime(2025, 9, 1)

    for medical in root.findall(".//ns:CaregiverMedicalDetails", ns):
        medical_id = medical.findtext("ns:MedicalID", namespaces=ns)
        caregiver_medical_id = medical.findtext("ns:CaregiverMedicalID", namespaces=ns)
        due_date_text = medical.findtext("ns:DueDate", namespaces=ns)
        status = medical.findtext("ns:Status", namespaces=ns)

        if medical_id != target_medical_id:
            continue

        if not caregiver_medical_id or not due_date_text or not due_date_text.strip():
            continue

        due_date = datetime.strptime(due_date_text, "%Y-%m-%d")

        if due_date < cutoff_date:
            continue

        if status.strip().lower() != "overdue":
            continue

        if latest_due_date is None or due_date > latest_due_date:
            latest_due_date = due_date
            latest_caregiver_medical_id = caregiver_medical_id

    return latest_caregiver_medical_id


async def process_flu_medical(caregiver_code, medical_id, date_performed, result_id):
    print(caregiver_code)

    try:
        caregiver_id = await get_caregiver_id(caregiver_code)
        if not caregiver_id:
            return caregiver_code, False, "Invalid Caregiver ID"

        all_medicals = await get_caregiver_medicals(caregiver_id)

        caregiver_medical_id = get_latest_overdue_caregiver_medical_id(all_medicals, medical_id)

        if caregiver_medical_id:
            return await send_update_medical(
                caregiver_code,
                caregiver_id,
                caregiver_medical_id,
                medical_id,
                date_performed,
                result_id
            )

        if has_valid_medical_for_season(all_medicals, medical_id, "2025-09-01"):
            return caregiver_code, False, "SKIPPED: Medical already exists for season"

        return await send_create_medical(
            caregiver_code,
            caregiver_id,
            medical_id,
            date_performed,
            result_id
        )

    except Exception as e:
        return caregiver_code, False, str(e)


async def update_medical(caregiver_code, medical_id, date_performed, result_id):
    caregiver_id = await get_caregiver_id(caregiver_code)
    all_overdue_medicals = await get_caregiver_medicals(caregiver_id)
    caregiver_medical_id = get_latest_overdue_caregiver_medical_id(all_overdue_medicals, medical_id)

    print(caregiver_code)

    if not caregiver_medical_id:
        return caregiver_code, False, "Could Not Find Overdue Flu Medical"

    return await send_update_medical(caregiver_code, caregiver_id, caregiver_medical_id, medical_id, date_performed,
                                     result_id)


def has_valid_medical_for_season(xml_string, target_medical_id, season_start):
    ns = {"ns": "https://www.hhaexchange.com/apis/hhaws.integration"}
    root = ET.fromstring(xml_string)

    cutoff = datetime.strptime(season_start, "%Y-%m-%d")

    for medical in root.findall(".//ns:CaregiverMedicalDetails", ns):
        medical_id = medical.findtext("ns:MedicalID", namespaces=ns)
        due_date_text = medical.findtext("ns:DueDate", namespaces=ns)
        status = medical.findtext("ns:Status", namespaces=ns)

        if str(medical_id) != str(target_medical_id):
            continue

        if not due_date_text or not status:
            continue

        due_date = datetime.strptime(due_date_text, "%Y-%m-%d")

        if due_date >= cutoff and status.strip().lower() != "pending":
            return True

    return False


async def create_medical(caregiver_code, medical_id, date_performed, result_id):
    print(caregiver_code)

    caregiver_id = await get_caregiver_id(caregiver_code)
    if not caregiver_id:
        return caregiver_code, False, "Invalid Caregiver ID"

    all_medicals = await get_caregiver_medicals(caregiver_id)  # now returns ALL

    if has_valid_medical_for_season(all_medicals, medical_id, "2025-09-01"):
        # Not an error — intentional skip
        return caregiver_code, False, "SKIPPED: Flu medical already exists for season"

    return await send_create_medical(caregiver_code, caregiver_id, medical_id, date_performed, result_id)


async def send_update_medical(caregiver_code, caregiver_id, caregiver_medical_id, medical_id, date_performed,
                              result_id):
    try:
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                       xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <UpdateCaregiverMedical xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <CaregiverMedicalInfo>
                <CaregiverID>{caregiver_id}</CaregiverID>
                <MedicalID>{medical_id}</MedicalID>
                <DateCompleted>{date_performed}</DateCompleted>
                <ResultID>{result_id}</ResultID>
                <CaregiverMedicalID>{caregiver_medical_id}</CaregiverMedicalID>
              </CaregiverMedicalInfo>
            </UpdateCaregiverMedical>
          </soap:Body>
        </soap:Envelope>"""

        response_content = await retry_soap_request(
            'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx',
            payload,
            '"https://www.hhaexchange.com/apis/hhaws.integration/UpdateCaregiverMedical"'
        )

        if "Success" in response_content:
            return caregiver_code, True, None

        root = ET.fromstring(response_content)
        error_message_element = root.find(
            './/ns1:ErrorMessage',
            namespaces={'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
        )
        error_message = (
                error_message_element.text or "").strip() if error_message_element is not None else "No error message provided"
        return caregiver_code, False, error_message

    except Exception as e:
        return caregiver_code, False, str(e)


async def send_create_medical(caregiver_code, caregiver_id, medical_id, date_performed, result_id):
    try:
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                       xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <CreateCaregiverMedical xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <CaregiverMedicalInfo>
                <CaregiverID>{caregiver_id}</CaregiverID>
                <MedicalID>{medical_id}</MedicalID>
                <DueDate>2025-09-01</DueDate>
                <DateCompleted>{date_performed}</DateCompleted>
                <ResultID>{result_id}</ResultID>
              </CaregiverMedicalInfo>
            </CreateCaregiverMedical>
          </soap:Body>
        </soap:Envelope>"""

        response_content = await retry_soap_request(
            'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx',
            payload,
            '"https://www.hhaexchange.com/apis/hhaws.integration/CreateCaregiverMedical"'
        )

        if "Success" in response_content:
            return caregiver_code, True, None

        root = ET.fromstring(response_content)
        error_message_element = root.find(
            './/ns1:ErrorMessage',
            namespaces={'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
        )
        error_message = (
                error_message_element.text or "").strip() if error_message_element is not None else "No error message provided"
        return caregiver_code, False, error_message

    except Exception as e:
        return caregiver_code, False, str(e)
