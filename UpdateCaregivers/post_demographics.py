from HHAExchange.APIkeys import app_name, app_secret, app_key
import xml.etree.ElementTree as ET
from HHAExchange.get_requests import get_caregiver_id, get_caregiver_demographics
from HHAExchange.asynchronous import retry_soap_request
import re


def transform_caregiver_info(xml_string, caregiver_id):
    """Extracts relevant patient info and converts it into the required structure."""

    def get_text(element, path):
        node = element.find(path, namespaces)
        return node.text if node is not None and node.text is not None else ""

    # Define namespaces
    namespaces = {
        'ns0': "http://schemas.xmlsoap.org/soap/envelope/",
        'ns1': "https://www.hhaexchange.com/apis/hhaws.integration"
    }

    # Parse XML
    root = ET.fromstring(xml_string)

    # Find CaregiverInfo
    caregiver_info = root.find(".//ns1:CaregiverInfo", namespaces)

    # Extract required fields
    first_name = caregiver_info.find("ns1:FirstName", namespaces).text if caregiver_info.find("ns1:FirstName",
                                                                                              namespaces) is not None else ""

    middle_name = caregiver_info.find("ns1:MiddleName", namespaces)
    middle_name = middle_name.text if middle_name is not None and middle_name.text is not None else ""

    last_name = caregiver_info.find("ns1:LastName", namespaces).text if caregiver_info.find("ns1:LastName",
                                                                                            namespaces) is not None else ""
    birth_date = caregiver_info.find("ns1:BirthDate", namespaces).text if caregiver_info.find("ns1:BirthDate",
                                                                                              namespaces) is not None else ""
    gender = caregiver_info.find("ns1:Gender", namespaces).text if caregiver_info.find("ns1:Gender",
                                                                                       namespaces) is not None else ""
    ssn = caregiver_info.find("ns1:SSN", namespaces).text if caregiver_info.find("ns1:SSN",
                                                                                 namespaces) is not None else ""
    employee_type = caregiver_info.find("ns1:EmployeeType", namespaces).text if caregiver_info.find("ns1:EmployeeType",
                                                                                                    namespaces) is not None else ""
    status_id = caregiver_info.find("ns1:Status/ns1:ID", namespaces).text if caregiver_info.find("ns1:Status/ns1:ID",
                                                                                                 namespaces) is not None else ""
    application_date = get_text(caregiver_info, "ns1:ApplicationDate")
    terminated_date = get_text(caregiver_info, "ns1:TerminatedDate")
    terminated_date_xml = f"<TerminatedDate>{terminated_date}</TerminatedDate>" if terminated_date else ""
    hha_pca_registry = caregiver_info.find("ns1:RegistryNumber", namespaces).text if caregiver_info.find(
        "ns1:RegistryNumber",
        namespaces) is not None else ""

    # Get Notification Preferences
    notif_elem = caregiver_info.find("ns1:NotificationPreferences", namespaces)
    # Build a clean NotificationPreferences element
    if notif_elem is not None:
        clean_elem = ET.Element("NotificationPreferences")

        # Extract MethodID if exists
        method = notif_elem.find("ns1:Method/ns1:ID", namespaces)
        method_text = method.text if method is not None else ''
        ET.SubElement(clean_elem, "MethodID").text = method_text

        # Copy other fields
        for tag in ["Email", "MobileOrSMS", "VoiceMessage"]:
            sub_elem = notif_elem.find(f"ns1:{tag}", namespaces)
            text = sub_elem.text if sub_elem is not None else ''
            ET.SubElement(clean_elem, tag).text = text

        # Convert back to string
        notification_preferences = ET.tostring(clean_elem, encoding='unicode')
    else:
        notification_preferences = ""

    zip5_elem = caregiver_info.find(".//ns1:Zip5", namespaces)
    zip5 = zip5_elem.text if zip5_elem is not None else ""

    # Get disciplines
    disciplines = [d.text for d in caregiver_info.findall(".//ns1:EmploymentTypes/ns1:Discipline", namespaces) if
                   d.text]
    disciplines = [d for d in disciplines if d != 'HCSS']

    # Construct new XML structure
    new_xml = f"""<CaregiverInfo>
    <CaregiverID>{caregiver_id}</CaregiverID>
    <FirstName>{first_name}</FirstName>
    <MiddleName>{middle_name}</MiddleName>
    <LastName>{last_name}</LastName>
    <Gender>{gender}</Gender>
    <BirthDate>{birth_date}</BirthDate>
    <SSN>{ssn}</SSN>
    <EmployeeType>{employee_type}</EmployeeType>
    <StatusID>{status_id}</StatusID>
    <EmploymentTypes>"""

    # Add disciplines if present
    if disciplines:
        new_xml += "".join([f"\n        <Discipline>{disc}</Discipline>" for disc in disciplines])

    # Close AcceptedServices properly
    new_xml += "\n    </EmploymentTypes>"

    new_xml += f"""
        <ApplicationDate>{application_date}</ApplicationDate>
        {terminated_date_xml}
        <HHAPCARegistryNumber>{hha_pca_registry}</HHAPCARegistryNumber>
        <Address>
          <Zip5>{zip5}</Zip5>
        </Address>
        {notification_preferences}
      </CaregiverInfo>"""

    # Remove extra namespace prefixes (e.g., ns1:)
    new_xml = re.sub(r'ns\d+:', '', new_xml)

    return new_xml


async def update_demographics(caregiver_code):
    caregiver_id = await get_caregiver_id(caregiver_code)
    demographics = await get_caregiver_demographics(caregiver_id)

    print(caregiver_code)
    caregiver_stuff = transform_caregiver_info(demographics, caregiver_id)

    try:

        # Define the XML payload with correct SOAP 1.1 envelope for Update Patient Demographics
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <UpdateCaregiverDemographics xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              {caregiver_stuff}
            </UpdateCaregiverDemographics>
          </soap:Body>
        </soap:Envelope>"""

        response_content = await retry_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                    '"https://www.hhaexchange.com/apis/hhaws.integration/UpdateCaregiverDemographics"')
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
        error_message = str(e)
        return caregiver_code, False, error_message
