import APIkeys
import requests
from get_requests import get_offices, get_notification_methods
from datetime import date
import xml.etree.ElementTree as ET
import pandas as pd
import time
import asyncio
import aiohttp

app_name = APIkeys.app_name
app_secret = APIkeys.app_secret
app_key = APIkeys.app_key


async def main():
    def get_employment_types(caregiver):
        segment_start = '<Discipline>'
        segment_end = '</Discipline>\n'
        types = caregiver['Employment Type'].split(', ')

        res = ''
        for type in types:
            res += f"{segment_start}{type}{segment_end}"

        return res.rstrip('\n')

    statuses = {'Inactive': 0, 'Active': 1, 'Hold': 2, 'On Leave': 3, 'Terminated': 4, 'Rejected': 5, 'Empty': 6}
    offices = await get_offices()
    office_id = offices['CDP']
    first_name = 'Test Nochum 2'
    last_name = 'Test Paltiel 2'
    birthdate = '1953-05-07'
    gender = 'Male'
    SSN = '999-45-1234'
    status_id = 1
    # employment_type = get_employment_types(caregiver)
    employment_type = '<Discipline>PA</Discipline>'
    employee_type = 'Applicant'
    application_date = date.today().strftime('%Y-%m-%d')
    zip_5 = '11213'
    hha_pca_registry = ' '
    notifications_dict = await get_notification_methods()
    notification_id = notifications_dict['Mobile/Text Message']
    phone = '917-112-3224'

    # Define the XML payload with correct SOAP 1.1 envelope for Create Caregivers
    soap_payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <CreateCaregiver xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <CaregiverInfo>
            <OfficeID>{office_id}</OfficeID>
            <FirstName>{first_name}</FirstName>
            <LastName>{last_name}</LastName>
            <Gender>{gender}</Gender>
            <BirthDate>{birthdate}</BirthDate>
            <SSN>{SSN}</SSN>
            <StatusID>{status_id}</StatusID>
            <EmploymentTypes>{employment_type}</EmploymentTypes>
            <EmployeeType>{employee_type}</EmployeeType>
            <ApplicationDate>{application_date}</ApplicationDate>
            <HHAPCARegistryNumber>{hha_pca_registry}</HHAPCARegistryNumber>
            <Address>
              <Zip5>{zip_5}</Zip5>
            </Address>
            <NotificationPreferences>
              <MethodID>{notification_id}</MethodID>
              <MobileOrSMS>{phone}</MobileOrSMS>
            </NotificationPreferences>
          </CaregiverInfo>
        </CreateCaregiver>
      </soap:Body>
    </soap:Envelope>"""

    # Define the headers, including content type for XML and SOAPAction
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',  # Set content type to XML for SOAP 1.1
        'SOAPAction': '"https://www.hhaexchange.com/apis/hhaws.integration/CreateCaregiver"',
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
            caregiver_id_element = root.find(".//ns1:CaregiverID", namespaces)
            # Extract the text value
            caregiver_id = caregiver_id_element.text if caregiver_id_element is not None else "Not Found"
            print(f"Extracted CaregiverID: {caregiver_id}")


asyncio.run(main())
