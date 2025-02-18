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
