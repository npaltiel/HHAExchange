from APIkeys import app_name, app_secret, app_key
import math
import xml.etree.ElementTree as ET
import re
from HHAExchange.get_requests import get_patient_id, get_patient_demographics
from HHAExchange.asynchronous import retry_soap_request


def remove_empty_elements(root):
    """Recursively remove empty elements from an XML tree."""
    for elem in list(root):  # Convert to list to avoid runtime modification issues
        remove_empty_elements(elem)  # Recurse into children
        if (elem.text is None or elem.text.strip() == "") and not elem.attrib and not list(elem):
            root.remove(elem)  # Remove empty element

    return root


def clean_and_update_branch(xml_string, new_branch_id, new_branch_name):
    # Remove namespace prefixes like ns0: and ns1:
    xml_string = re.sub(r'xmlns(:\w+)?="[^"]+"', '', xml_string)  # Remove xmlns attributes
    xml_string = re.sub(r'(<\/?)ns\d+:', r'\1', xml_string)  # Remove ns1:, ns0: from tags

    # Parse XML
    root = ET.fromstring(xml_string)

    root = remove_empty_elements(root)

    # Find PatientInfo
    patient_info = root.find(".//PatientInfo")
    if patient_info is None:
        return "Error: No PatientInfo found in XML"

    # Find Branch ID element and update
    branch_id_element = patient_info.find("./Branch/ID")
    if branch_id_element is not None:
        branch_id_element.text = str(new_branch_id)

    # Find Branch Name element and update
    branch_name_element = patient_info.find("./Branch/Name")
    if branch_name_element is not None:
        branch_name_element.text = new_branch_name

    # Convert back to string
    updated_xml_string = ET.tostring(patient_info, encoding="utf-8").decode("utf-8")

    return updated_xml_string


def transform_patient_info(xml_string, branch_id):
    """Extracts relevant patient info and converts it into the required structure."""

    # Define namespaces
    namespaces = {
        'ns0': "http://schemas.xmlsoap.org/soap/envelope/",
        'ns1': "https://www.hhaexchange.com/apis/hhaws.integration"
    }

    # Parse XML
    root = ET.fromstring(xml_string)

    # Find PatientInfo
    patient_info = root.find(".//ns1:PatientInfo", namespaces)

    # Extract required fields
    patient_id = patient_info.find("ns1:PatientID", namespaces).text if patient_info.find("ns1:PatientID",
                                                                                          namespaces) is not None else ""
    first_name = patient_info.find("ns1:FirstName", namespaces).text if patient_info.find("ns1:FirstName",
                                                                                          namespaces) is not None else ""
    last_name = patient_info.find("ns1:LastName", namespaces).text if patient_info.find("ns1:LastName",
                                                                                        namespaces) is not None else ""
    birth_date = patient_info.find("ns1:BirthDate", namespaces).text if patient_info.find("ns1:BirthDate",
                                                                                          namespaces) is not None else ""
    gender = patient_info.find("ns1:Gender", namespaces).text if patient_info.find("ns1:Gender",
                                                                                   namespaces) is not None else ""

    # Get first Coordinator ID
    coordinators = patient_info.findall(".//ns1:Coordinator", namespaces)
    coordinator_id1 = coordinators[0].find("ns1:ID", namespaces).text if coordinators and coordinators[0].find("ns1:ID",
                                                                                                               namespaces) is not None else "0"
    medicaid_number = patient_info.find("ns1:MedicaidNumber", namespaces).text if patient_info.find(
        "ns1:MedicaidNumber", namespaces) is not None else ""

    # Get primary Address details
    primary_address = None
    for address in patient_info.findall("ns1:Addresses/ns1:Address", namespaces):
        is_primary = address.find("ns1:IsPrimaryAddress", namespaces)
        if is_primary is not None and is_primary.text == "Yes":
            primary_address = address
            break

    address_id = primary_address.find("ns1:AddressID", namespaces).text if primary_address is not None else ""
    zip5 = primary_address.find("ns1:Zip5", namespaces).text if primary_address is not None else ""

    location_id = patient_info.find("ns1:Location/ns1:ID", namespaces).text if patient_info.find("ns1:Location/ns1:ID",
                                                                                                 namespaces) is not None else "0"

    # Get Accepted Services
    disciplines = [d.text for d in patient_info.findall(".//ns1:AcceptedServices/ns1:Discipline", namespaces) if d.text]

    # Construct new XML structure
    new_xml = f"""<PatientInfo>
    <PatientID>{patient_id}</PatientID>
    <FirstName>{first_name}</FirstName>
    <LastName>{last_name}</LastName>
    <BirthDate>{birth_date}</BirthDate>
    <Gender>{gender}</Gender>
    <CoordinatorID1>{coordinator_id1}</CoordinatorID1>
    <MedicaidNumber>{medicaid_number}</MedicaidNumber>
    <AcceptedServices>"""

    # Add disciplines if present
    if disciplines:
        new_xml += "".join([f"\n        <Discipline>{disc}</Discipline>" for disc in disciplines])

    # Close AcceptedServices properly
    new_xml += "\n    </AcceptedServices>"

    new_xml += f"""
    <BranchID>{branch_id}</BranchID>
    <LocationID>{location_id}</LocationID>
    <Addresses>
      <Address>
        <AddressID>{address_id}</AddressID>
        <Zip5>{zip5}</Zip5>
        <IsPrimaryAddress>Yes</IsPrimaryAddress>
      </Address>
    </Addresses>
</PatientInfo>"""

    # Remove extra namespace prefixes (e.g., ns1:)
    new_xml = re.sub(r'ns\d+:', '', new_xml)

    return new_xml


async def update_branch(admission_id, branch_id):
    patient_id = await get_patient_id(admission_id)
    demographics = await get_patient_demographics(patient_id)

    patient_info = transform_patient_info(demographics, branch_id)

    try:

        # Define the XML payload with correct SOAP 1.1 envelope for Update Patient Demographics
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <UpdatePatientDemographics xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              {patient_info}
            </UpdatePatientDemographics>
          </soap:Body>
        </soap:Envelope>"""

        response_content = await retry_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                    '"https://www.hhaexchange.com/apis/hhaws.integration/UpdatePatientDemographics"')
        # Check response content for success (you may want to look for specific elements in `response_content` if needed)
        if "Success" in response_content:  # Replace "Success" with the actual success check from response
            return admission_id, True, None
        else:
            # Extract error message from response (you may need to parse the XML response)
            root = ET.fromstring(response_content)
            error_message_element = root.find('.//ns1:ErrorMessage',
                                              namespaces={'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'})

            error_message = error_message_element.text if error_message_element is not None and error_message_element.text else "No error message provided"
            return admission_id, False, error_message

    except Exception as e:
        # Return error message if an exception occurs
        error_message = str(e)
        return admission_id, False, error_message
