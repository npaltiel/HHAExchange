import APIINFO
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
import asyncio
import aiohttp

app_name = APIINFO.app_name
app_secret = APIINFO.app_secret
app_key = APIINFO.app_key
patient_id = 20860460
start_date = "2024-08-01T00:00:00"
end_date = "2024-08-02T00:00:00"


# Define the XML payload with correct SOAP 1.1 envelope for Search Visits
soap_payload = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <SearchVisits xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
      <Authentication>
        <AppName>{app_name}</AppName>
        <AppSecret>{app_secret}</AppSecret>
        <AppKey>{app_key}</AppKey>
      </Authentication>
      <SearchFilters>
        <StartDate>{start_date}</StartDate>
        <EndDate>{end_date}</EndDate>
      </SearchFilters>
    </SearchVisits>
  </soap:Body>
</soap:Envelope>"""

# Define the headers, including content type for XML and SOAPAction
headers = {
    'Content-Type': 'text/xml; charset=utf-8',  # Set content type to XML for SOAP 1.1
    'SOAPAction': '"https://www.hhaexchange.com/apis/hhaws.integration/SearchVisits"',  # Correct the SOAPAction header for Search Visits
}

# Use the general endpoint URL for the SOAP service
endpoint_url = 'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx'

start_visit_day = time.time()
# Make the POST request to the API endpoint
response = requests.post(endpoint_url, data=soap_payload, headers=headers)
end_visit_day = time.time()

# Check if the request was successful
if response.status_code != 200:
    # Print the error code and response content if there was an error
    print(f"Error {response.status_code}: {response.text}")


# Parse the XML response
root = ET.fromstring(response.text)

# Use the namespace to find all 'VisitID' elements
namespace = {'ns': 'https://www.hhaexchange.com/apis/hhaws.integration'}

# Extract all 'VisitID' elements under the 'Visits' tag
visit_ids = root.findall('.//ns:VisitID', namespace)

# Extract the text from each 'VisitID' element
visit_id_list = [visit_id.text for visit_id in visit_ids]


visit_data_list = []
async def get_visit_info(session, visit_id):
    # Define the XML payload with correct SOAP 1.1 envelope for Search Visits
    soap_payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetVisitInfoV2 xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <VisitInfo>
            <ID>{visit_id}</ID>
          </VisitInfo>
        </GetVisitInfoV2>
      </soap:Body>
    </soap:Envelope>"""

    # Define the headers, including content type for XML and SOAPAction
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',  # Set content type to XML for SOAP 1.1
        'SOAPAction': '"https://www.hhaexchange.com/apis/hhaws.integration/GetVisitInfoV2"',  # Correct the SOAPAction header for Search Visits
    }

    # Use the general endpoint URL for the SOAP service
    endpoint_url = 'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx'

    async with session.post(endpoint_url, data=soap_payload, headers=headers) as response:
        response_text = await response.text()
        namespace = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                     'ns': 'https://www.hhaexchange.com/apis/hhaws.integration'}
        root = ET.fromstring(response_text)
        visit_info = root.find('.//ns:VisitInfo', namespace)
        if visit_info is not None:
            parsed_data = {
                'VisitID': visit_info.find('ns:ID', namespace).text,
                'VisitDate': visit_info.find('ns:VisitDate', namespace).text,
                'PatientID': visit_info.find('ns:Patient/ns:ID', namespace).text,
                'PatientAdmissionNumber': visit_info.find('ns:Patient/ns:AdmissionNumber', namespace).text,
                'PatientFirstName': visit_info.find('ns:Patient/ns:FirstName', namespace).text,
                'PatientLastName': visit_info.find('ns:Patient/ns:LastName', namespace).text,
                'CaregiverID': visit_info.find('ns:Caregiver/ns:ID', namespace).text,
                'CaregiverFirstName': visit_info.find('ns:Caregiver/ns:FirstName', namespace).text,
                'CaregiverLastName': visit_info.find('ns:Caregiver/ns:LastName', namespace).text,
                'CaregiverCode': visit_info.find('ns:Caregiver/ns:CaregiverCode', namespace).text,
                'VisitStartTime': visit_info.find('ns:VisitStartTime', namespace).text,
                'VisitEndTime': visit_info.find('ns:VisitEndTime', namespace).text,
                'IsMissedVisit': visit_info.find('ns:IsMissedVisit', namespace).text,
                'ServiceHours': visit_info.find('ns:Payroll/ns:ServiceHours', namespace).text,
            }
            return parsed_data
        return None

# Main function to handle asynchronous tasks
async def main(visit_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [get_visit_info(session, visit_id) for visit_id in visit_ids]
        results = await asyncio.gather(*tasks)
        # Filter out any None results
        results = [result for result in results if result is not None]
        # Create a DataFrame
        visit_df = pd.DataFrame(results)
        return visit_df


start_time = time.time()
visit_df = asyncio.run(main(visit_id_list))

end_time = time.time()

time_for_day = start_visit_day - end_visit_day
time_for_report = start_time - end_time

