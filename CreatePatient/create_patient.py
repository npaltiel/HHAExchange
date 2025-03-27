import APIkeys
from HHAExchange.get_requests import get_offices, get_notification_methods
import xml.etree.ElementTree as ET
import aiohttp

app_name = APIkeys.app_name
app_secret = APIkeys.app_secret
app_key = APIkeys.app_key


async def create_patient(patient):
    offices = await get_offices()
    office_id = offices['COR']
    first_name = patient['First Name']
    last_name = patient['Last Name']
    birthdate = patient['DOB']
    status_id = 1
    gender = 'Other'
    medicaid = patient['Medicaid Number']
    coordinator_id = 77288
    # employment_type = get_employment_types(caregiver)
    discipline = '<Discipline>HHA</Discipline>'
    address1 = patient['Address 1']
    address2 = patient['Address 2']
    city = patient['City']
    zip_5 = patient['Zip Code']
    state = patient['State']
    notifications_dict = await get_notification_methods()
    notification_id = notifications_dict['Mobile/Text Message']
    phone = '<HomePhone>' + str(patient['Phone Number']) + '</HomePhone>' if patient['Phone Number'] != "" else ""

    # Define the XML payload with correct SOAP 1.1 envelope for Create Caregivers
    soap_payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <CreatePatient xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <PatientInfo>
            <OfficeID>{office_id}</OfficeID>
            <FirstName>{first_name}</FirstName>
            <LastName>{last_name}</LastName>
            <BirthDate>{birthdate}</BirthDate>
            <StatusID>{status_id}</StatusID>
            <Gender>{gender}</Gender>
            <CoordinatorID1>{coordinator_id}</CoordinatorID1>
            <AcceptedServices>
              {discipline}
            </AcceptedServices>
            <Addresses>
              <Address>
                <Address1>{address1}</Address1>
                <Address2>{address2}</Address2>
                <City>{city}</City>
                <Zip5>{zip_5}</Zip5>
                <State>{state}</State>
                <IsPrimaryAddress>Yes</IsPrimaryAddress>
              </Address>
            </Addresses>
          </PatientInfo>
        </CreatePatient>
      </soap:Body>
    </soap:Envelope>"""

    # Define the headers, including content type for XML and SOAPAction
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',  # Set content type to XML for SOAP 1.1
        'SOAPAction': '"https://www.hhaexchange.com/apis/hhaws.integration/CreatePatient"',
        # Correct the SOAPAction header for Search Visits
    }

    # Use the general endpoint URL for the SOAP service
    endpoint_url = 'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx'

    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint_url, data=soap_payload, headers=headers) as response:
            response_text = await response.text()
            print(f"Response Status: {response.status}")
            print(f"Response Content: {response_text}")
            root = ET.fromstring(response_text)
            # Find the CaregiverID\
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'
            }
            patient_id_element = root.find(".//ns1:PatientID", namespaces)
            # Extract the text value
            patient_id = patient_id_element.text if patient_id_element is not None else "Not Found"
            print(f"Extracted PatientID: {patient_id}")
