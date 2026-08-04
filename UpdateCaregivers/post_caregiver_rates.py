from HHAExchange.APIkeys import app_name, app_secret, app_key
from HHAExchange.asynchronous import retry_soap_request
import xml.etree.ElementTree as ET


def build_rate_payload(rate_info):
    """Builds the SOAP payload for UpdateCaregiverRate from a rate_info dict."""

    caregiver_rate_id = rate_info.get('CaregiverRateID', '')
    patient_id = rate_info.get('PatientID', '')
    from_date = rate_info.get('FromDate', '')
    to_date = rate_info.get('ToDate', '')
    hourly_rate = rate_info.get('HourlyRate', '')
    daily_rate = rate_info.get('DailyRate', '')
    visit_rate = rate_info.get('VisitRate', '')
    status = rate_info.get('Status', '')

    # These are typed as decimal on HHAX's side - an empty tag (e.g. <VisitRate></VisitRate>)
    # fails to parse as a decimal, so omit the tag entirely when there's no value.
    def rate_tag(name, value):
        value = str(value).strip()
        return f"<{name}>{value}</{name}>" if value else ""

    hourly_rate_tag = rate_tag("HourlyRate", hourly_rate)
    daily_rate_tag = rate_tag("DailyRate", daily_rate)
    visit_rate_tag = rate_tag("VisitRate", visit_rate)

    payload = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <UpdateCaregiverRate xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
      <Authentication>
        <AppName>{app_name}</AppName>
        <AppSecret>{app_secret}</AppSecret>
        <AppKey>{app_key}</AppKey>
      </Authentication>
      <CaregiverRateInfo>
        <CaregiverRateID>{caregiver_rate_id}</CaregiverRateID>
        <PatientID>{patient_id}</PatientID>
        <FromDate>{from_date}</FromDate>
        <ToDate>{to_date}</ToDate>
        {hourly_rate_tag}
        {daily_rate_tag}
        {visit_rate_tag}
        <Status>{status}</Status>
      </CaregiverRateInfo>
    </UpdateCaregiverRate>
  </soap:Body>
</soap:Envelope>"""

    return payload


async def update_caregiver_rate(rate_info):
    """Sends an UpdateCaregiverRate request for a single row and returns
    (caregiver_rate_id, success_bool, error_message_or_None)."""

    caregiver_rate_id = rate_info.get('CaregiverRateID', '')
    payload = build_rate_payload(rate_info)

    try:
        response_content = await retry_soap_request(
            'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx',
            payload,
            '"https://www.hhaexchange.com/apis/hhaws.integration/UpdateCaregiverRate"'
        )

        if "Success" in response_content:
            return caregiver_rate_id, True, None
        else:
            root = ET.fromstring(response_content)
            error_message_element = root.find(
                './/ns1:ErrorMessage',
                namespaces={'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
            )
            error_message = (
                error_message_element.text
                if error_message_element is not None and error_message_element.text
                else "No error message provided"
            )
            return caregiver_rate_id, False, error_message

    except Exception as e:
        return caregiver_rate_id, False, str(e)
