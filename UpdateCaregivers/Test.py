from HHAExchange.APIkeys import app_name, app_secret, app_key
import xml.etree.ElementTree as ET
from HHAExchange.get_requests import get_caregiver_id
from HHAExchange.asynchronous import retry_soap_request


async def update_I9(caregiver_code):
    caregiver_id = await get_caregiver_id(caregiver_code)

    print(caregiver_code)

    try:

        # Define the XML payload with correct SOAP 1.1 envelope for Update Patient Demographics
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <UpdateCaregiverI9Requirements xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
                  <Authentication>
                    <AppName>{app_name}</AppName>
                    <AppSecret>{app_secret}</AppSecret>
                    <AppKey>{app_key}</AppKey>
                  </Authentication>
                  <UpdateCaregiverI9RequirementsInfo>
                    <CaregiverID>{caregiver_id}</CaregiverID>
                    <I9DocumentExpiration>2026-03-15</I9DocumentExpiration>
                  </UpdateCaregiverI9RequirementsInfo>
                </UpdateCaregiverI9Requirements>
              </soap:Body>
            </soap:Envelope>"""

        response_content = await retry_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                    '"https://www.hhaexchange.com/apis/hhaws.integration/UpdateCaregiverI9Requirements"')
        # Check response content for success (you may want to look for specific elements in `response_content` if needed)
        if "Success" in response_content:  # Replace "Success" with the actual success check from response
            return caregiver_code, True, None
        else:
            # Extract error message from response (you may need to parse the XML response)
            root = ET.fromstring(response_content)
            error_message_element = root.find('.//ns1:ErrorMessage',
                                              namespaces={'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'})

            error_message = error_message_element.text if error_message_element is not None and error_message_element.text else "No error message provided"
            return caregiver_code, False, error_message

    except Exception as e:
        # Return error message if an exception occurs
        return caregiver_code
