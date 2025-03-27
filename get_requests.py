from APIkeys import app_name, app_secret, app_key
import xml.etree.ElementTree as ET
from asynchronous import async_soap_request
import asyncio


async def get_offices():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetOffices xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
        </GetOffices>
      </soap:Body>
    </soap:Envelope>"""

    response_content = await async_soap_request(
        'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx',
        payload,
        '"https://www.hhaexchange.com/apis/hhaws.integration/GetOffices"'
    )

    namespaces = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'
    }
    # Parse the XML response
    root = ET.fromstring(response_content)

    offices = root.find(".//ns1:GetOfficesResult", namespaces).findall(".//ns1:Office", namespaces)
    office_dict = {office.find("ns1:OfficeCode", namespaces).text: office.find("ns1:OfficeID", namespaces).text for
                   office in offices}

    return office_dict


async def get_notification_methods():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetCaregiverNotificationMethods xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
        </GetCaregiverNotificationMethods>
      </soap:Body>
    </soap:Envelope>"""

    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetCaregiverNotificationMethods"')
    root = ET.fromstring(response_content)
    namespaces = {'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
    notification_methods_dict = {method.find('ns1:CaregiverNotificationMethodName', namespaces).text: method.find(
        'ns1:CaregiverNotificationMethodID', namespaces).text for method in
                                 root.findall('.//ns1:CaregiverNotificationMethod', namespaces)}
    return notification_methods_dict


async def get_caregiver_id(caregiver_code):
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <SearchCaregivers xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <SearchFilters>
                <CaregiverCode>{caregiver_code}</CaregiverCode>
              </SearchFilters>
            </SearchCaregivers>
          </soap:Body>
        </soap:Envelope>"""
    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/SearchCaregivers"')
    root = ET.fromstring(response_content)
    namespace = {'ns1': "https://www.hhaexchange.com/apis/hhaws.integration"}
    caregiver_ids = [elem.text for elem in root.findall('.//ns1:CaregiverID', namespaces=namespace)]
    return int(caregiver_ids[0]) if caregiver_ids else None


async def get_teams():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetCaregiverTeams xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <OfficeID>2365</OfficeID>
        </GetCaregiverTeams>
      </soap:Body>
    </soap:Envelope>"""

    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetCaregiverTeams"')
    root = ET.fromstring(response_content)
    namespaces = {'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
    team_dict = {team.find('ns1:CaregiverTeamName', namespaces).text: team.find('ns1:CaregiverTeamID', namespaces).text
                 for team in root.findall('.//ns1:CaregiverTeam', namespaces)}
    return team_dict


async def get_notification_methods():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetCaregiverNotificationMethods xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
        </GetCaregiverNotificationMethods>
      </soap:Body>
    </soap:Envelope>"""

    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetCaregiverNotificationMethods"')
    root = ET.fromstring(response_content)
    namespaces = {'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
    notification_methods_dict = {method.find('ns1:CaregiverNotificationMethodName', namespaces).text: method.find(
        'ns1:CaregiverNotificationMethodID', namespaces).text for method in
                                 root.findall('.//ns1:CaregiverNotificationMethod', namespaces)}
    return notification_methods_dict


async def get_branches():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetBranches xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <OfficeID>2365</OfficeID>
        </GetBranches>
      </soap:Body>
    </soap:Envelope>"""

    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetBranches"')
    root = ET.fromstring(response_content)
    xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')
    ns = {'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
    branches_dict = {branch.find('ns1:BranchName', ns).text: branch.find('ns1:BranchID', ns).text for branch in
                     root.findall('.//ns1:Branch', ns)}

    return branches_dict


async def get_patient_id(admission_id):
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <SearchPatients xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <SearchFilters>
                <AdmissionID>{admission_id}</AdmissionID>
              </SearchFilters>
            </SearchPatients>
          </soap:Body>
        </soap:Envelope>"""
    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/SearchPatients"')
    root = ET.fromstring(response_content)
    namespace = {'ns1': "https://www.hhaexchange.com/apis/hhaws.integration"}
    patient_ids = [elem.text for elem in root.findall('.//ns1:PatientID', namespaces=namespace)]
    return int(patient_ids[0]) if patient_ids else None


async def get_coordinators():
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <GetCoordinators xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <SearchFilters />
            </GetCoordinators>
          </soap:Body>
        </soap:Envelope>"""
    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetCoordinators"')

    root = ET.fromstring(response_content)
    xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')

    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "hhaws": "https://www.hhaexchange.com/apis/hhaws.integration"
    }
    coordinator_dict = {}

    # Navigate to the Coordinators section
    coordinators = root.find(".//hhaws:GetCoordinatorsResult/hhaws:Coordinators", namespaces)

    if coordinators is not None:
        for coordinator in coordinators.findall("hhaws:Coordinator", namespaces):
            name = coordinator.find("hhaws:Name", namespaces)
            coordinator_id = coordinator.find("hhaws:CoordinatorID", namespaces)

            if name is not None and coordinator_id is not None:
                coordinator_dict[name.text] = int(coordinator_id.text)

    return coordinator_dict


async def get_patient_demographics(patient_id):
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <GetPatientDemographics xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <PatientInfo>
                <ID>{patient_id}</ID>
              </PatientInfo>
            </GetPatientDemographics>
          </soap:Body>
        </soap:Envelope>"""
    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetPatientDemographics"')
    root = ET.fromstring(response_content)
    xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')

    return xml_string


async def get_caregiver_demographics(caregiver_id):
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <GetCaregiverDemographics xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
              <Authentication>
                <AppName>{app_name}</AppName>
                <AppSecret>{app_secret}</AppSecret>
                <AppKey>{app_key}</AppKey>
              </Authentication>
              <CaregiverInfo>
                <ID>{caregiver_id}</ID>
              </CaregiverInfo>
            </GetCaregiverDemographics>
          </soap:Body>
        </soap:Envelope>"""
    response_content = await async_soap_request('https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx', payload,
                                                '"https://www.hhaexchange.com/apis/hhaws.integration/GetCaregiverDemographics"')
    root = ET.fromstring(response_content)
    xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')

    return xml_string
