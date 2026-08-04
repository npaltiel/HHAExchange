import pandas as pd
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from HHAExchange.APIkeys import app_name, app_secret, app_key
from HHAExchange.get_requests import get_caregiver_id, get_caregiver_medicals
from HHAExchange.asynchronous import retry_soap_request

semaphore = asyncio.Semaphore(3)


def has_any_medical_in_year(xml_string, target_medical_id, target_year):
    ns = {"ns": "https://www.hhaexchange.com/apis/hhaws.integration"}
    root = ET.fromstring(xml_string)

    for medical in root.findall(".//ns:CaregiverMedicalDetails", ns):
        medical_id = medical.findtext("ns:MedicalID", namespaces=ns)
        due_date_text = medical.findtext("ns:DueDate", namespaces=ns)

        if str(medical_id) != str(target_medical_id):
            continue

        if not due_date_text:
            continue

        due_date = datetime.strptime(due_date_text, "%Y-%m-%d")

        if due_date.year == target_year:
            return True

    return False


async def send_create_flu_medical_2026(caregiver_code, caregiver_id, medical_id):
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
                <DueDate>2026-09-01</DueDate>
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


async def process_create_flu_season_2026(caregiver_code):
    print(caregiver_code)

    try:
        caregiver_id = await get_caregiver_id(caregiver_code)
        if not caregiver_id:
            return caregiver_code, False, "Invalid Caregiver ID"

        all_medicals = await get_caregiver_medicals(caregiver_id)

        if has_any_medical_in_year(all_medicals, '75560', 2026):
            return caregiver_code, False, "SKIPPED: Flu medical already exists for 2026"

        return await send_create_flu_medical_2026(caregiver_code, caregiver_id, '75560')

    except Exception as e:
        return caregiver_code, False, str(e)


async def safe_process_create_flu_season_2026(caregiver_code):
    async with semaphore:
        return await process_create_flu_season_2026(caregiver_code)


async def main():
    df = pd.read_csv(
        r"C:\Users\nochum.paltiel\OneDrive - Anchor Home Health care\Documents\Exchange API Updates\Caregiver Codes for Flu 2026.csv")

    tasks = [safe_process_create_flu_season_2026(row['Caregiver Code']) for _, row in df.iterrows()]

    results = await asyncio.gather(*tasks)

    failure_rows = []
    for caregiver_code, success, error_message in results:
        if not success:
            failure_rows.append({
                "Caregiver Code": caregiver_code,
                "Error Message": error_message
            })

    if failure_rows:
        failures_df = pd.DataFrame(failure_rows)
        failures_df.to_csv(
            r"C:\Users\nochum.paltiel\OneDrive - Anchor Home Health care\Documents\Exchange API Updates\Flu_2026_Failures.csv",
            index=False
        )
        print(f"Saved {len(failure_rows)} failures to CSV")
    else:
        print("No failures 🎉")


asyncio.run(main())
