import os
import re
import xml.etree.ElementTree as ET
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from APIkeys import app_name, app_secret, app_key
from asynchronous import retry_soap_request
from get_requests import get_caregiver_demographics

NS = {'ns1': 'https://www.hhaexchange.com/apis/hhaws.integration'}
ENDPOINT = 'https://app.hhaexchange.com/integration/ent/v1.8/ws.asmx'
UPDATE_ACTION = '"https://www.hhaexchange.com/apis/hhaws.integration/UpdateCaregiverDemographics"'


def _text(element, path):
    node = element.find(path, NS)
    return node.text if node is not None and node.text else ''


def _build_payload(caregiver_id, demographics_xml, new_team_id, add_hcss=False):
    """
    Parse current demographics from GetCaregiverDemographics response,
    inject the new TeamID (and optionally add HCSS discipline), and return
    the full UpdateCaregiverDemographics SOAP payload.
    """
    root = ET.fromstring(demographics_xml)
    info = root.find('.//ns1:CaregiverInfo', NS)

    first_name     = _text(info, 'ns1:FirstName')
    middle_name    = _text(info, 'ns1:MiddleName')
    last_name      = _text(info, 'ns1:LastName')
    gender         = _text(info, 'ns1:Gender')
    birth_date     = _text(info, 'ns1:BirthDate')
    ssn            = _text(info, 'ns1:SSN')
    employee_type  = _text(info, 'ns1:EmployeeType')
    status_id      = _text(info, 'ns1:Status/ns1:ID')
    application_date = _text(info, 'ns1:ApplicationDate')
    terminated_date  = _text(info, 'ns1:TerminatedDate')
    rehire_date      = _text(info, 'ns1:RehireDate')
    registry_number  = _text(info, 'ns1:RegistryNumber')
    zip5             = _text(info, './/ns1:Zip5')
    branch_name      = _text(info, 'ns1:BranchName')

    disciplines = [d.text for d in info.findall('.//ns1:EmploymentTypes/ns1:Discipline', NS) if d.text]

    if add_hcss and 'HCSS' not in disciplines and 'SCM' not in disciplines and 'RN' not in disciplines \
            and 'ACD' not in branch_name:
        disciplines.append('HCSS')

    notif_elem = info.find('ns1:NotificationPreferences', NS)
    if notif_elem is not None:
        clean_notif = ET.Element('NotificationPreferences')
        method_id = notif_elem.find('ns1:Method/ns1:ID', NS)
        ET.SubElement(clean_notif, 'MethodID').text = method_id.text if method_id is not None else ''
        for tag in ('Email', 'MobileOrSMS', 'VoiceMessage'):
            sub = notif_elem.find(f'ns1:{tag}', NS)
            ET.SubElement(clean_notif, tag).text = sub.text if sub is not None else ''
        notification_xml = ET.tostring(clean_notif, encoding='unicode')
    else:
        notification_xml = ''

    rehire_xml     = f'<RehireDate>{rehire_date}</RehireDate>'       if rehire_date     else ''
    terminated_xml = f'<TerminatedDate>{terminated_date}</TerminatedDate>' if terminated_date else ''
    disciplines_xml = ''.join(f'<Discipline>{d}</Discipline>' for d in disciplines)

    payload = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <UpdateCaregiverDemographics xmlns="https://www.hhaexchange.com/apis/hhaws.integration">
          <Authentication>
            <AppName>{app_name}</AppName>
            <AppSecret>{app_secret}</AppSecret>
            <AppKey>{app_key}</AppKey>
          </Authentication>
          <CaregiverInfo>
            <CaregiverID>{caregiver_id}</CaregiverID>
            <FirstName>{first_name}</FirstName>
            <MiddleName>{middle_name}</MiddleName>
            <LastName>{last_name}</LastName>
            <Gender>{gender}</Gender>
            <BirthDate>{birth_date}</BirthDate>
            <SSN>{ssn}</SSN>
            <EmployeeType>{employee_type}</EmployeeType>
            <StatusID>{status_id}</StatusID>
            {rehire_xml}
            {terminated_xml}
            <EmploymentTypes>{disciplines_xml}</EmploymentTypes>
            <ApplicationDate>{application_date}</ApplicationDate>
            <TeamID>{new_team_id}</TeamID>
            <HHAPCARegistryNumber>{registry_number}</HHAPCARegistryNumber>
            <Address><Zip5>{zip5}</Zip5></Address>
            {notification_xml}
          </CaregiverInfo>
        </UpdateCaregiverDemographics>
      </soap:Body>
    </soap:Envelope>"""

    return payload


async def update_team(caregiver_id, caregiver_code, new_team_id, add_hcss=False):
    try:
        demographics_xml = await get_caregiver_demographics(caregiver_id)
        payload = _build_payload(caregiver_id, demographics_xml, new_team_id, add_hcss=add_hcss)

        response = await retry_soap_request(ENDPOINT, payload, UPDATE_ACTION)

        if 'Success' in response:
            return caregiver_code, True, None

        root = ET.fromstring(response)
        err_elem = root.find('.//ns1:ErrorMessage', NS)
        error_msg = err_elem.text if err_elem is not None and err_elem.text else 'No error message provided'
        return caregiver_code, False, error_msg

    except Exception as e:
        return caregiver_code, False, str(e)