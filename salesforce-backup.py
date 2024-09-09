import os
import re
import sys
import yaml
import shutil
import requests
from datetime import date, datetime
from xml.etree import ElementTree as ET

class Result:
    def __init__(self, xmldoc):
        self.xmldoc = xmldoc

    @property
    def server_url(self):
        # {*} needed to search in all namespaces
        return self.xmldoc.findtext('.//{*}serverUrl')

    @property
    def session_id(self):
        return self.xmldoc.findtext('.//{*}sessionId')

    @property
    def org_id(self):
        return self.xmldoc.findtext('.//{*}organizationId')

class SfError(Exception):
    def __init__(self, resp):
        self.resp = resp

    def __str__(self):
        return str(self.resp.text)

def http():
    return requests.Session()

def headers(login):
    return {
        'Cookie': f"oid={login.org_id}; sid={login.session_id}",
        'X-SFDC-Session': login.session_id
    }

def file_name(url=None):
    datestamp = date.today().strftime('%Y-%m-%d')
    uid_string = ''
    if url:
        match = re.match(r'.*fileName=(.*)\.ZIP.*', url)
        if match:
            uid_string = f"-{match.group(1)}"
    return f"salesforce-{datestamp}{uid_string}.ZIP"

def progress_percentage(current, total):
    return int((current / total) * 100)

def login():
    print("Logging in...")
    path = 'https://login.salesforce.com/services/Soap/u/28.0'

    pwd_token_encoded = sales_force_passwd_and_sec_token.replace('&', '&amp;')

    inital_data = f"""<?xml version="1.0" encoding="utf-8" ?>
<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
      <n1:username>{sales_force_user_name}</n1:username>
      <n1:password>{pwd_token_encoded}</n1:password>
    </n1:login>
  </env:Body>
</env:Envelope>"""

    initial_headers = {
        'Content-Type': 'text/xml; charset=UTF-8',
        'SOAPAction': 'login'
    }

    resp = http().post(path, data=inital_data, headers=initial_headers)

    if resp.status_code == 200:
        xmldoc = ET.fromstring(resp.text)
        return Result(xmldoc)
    else:
        raise SfError(resp)

def download_index(login):
    print("Downloading index...")
    path = '/servlet/servlet.OrgExport'
    resp = http().post(f"https://{sales_force_site}{path}", headers=headers(login))
    return resp.text.strip()

def get_download_size(login, url):
    print("Getting download size...")
    resp = http().head(f"https://{sales_force_site}{url}", headers=headers(login))
    return int(resp.headers.get('Content-Length', 0))

def download_file(login, url, expected_size):
    printing_interval = 10
    interval_type = 'percentage'
    last_printed_value = None
    size = 0
    fn = file_name(url)
    print(f"Downloading {fn}...")
    with open(os.path.join(data_directory, fn), "wb") as f:
        resp = http().get(f"https://{sales_force_site}{url}", headers=headers(login), stream=True)
        resp.raise_for_status()
        for segment in resp.iter_content(chunk_size=8192):
            f.write(segment)
            size += len(segment)
            last_printed_value = print_progress(size, expected_size, printing_interval, last_printed_value, interval_type)
    print(f"\nFinished downloading {fn}!")
    if size != expected_size:
        raise ValueError(f"Size didn't match. Expected: {expected_size} Actual: {size}")

def print_progress(size, expected_size, interval, previous_printed_interval, interval_type='seconds'):
    percent_file_complete = progress_percentage(size, expected_size)
    if interval_type == 'percentage':
        previous_printed_interval = previous_printed_interval or 0
        current_value = percent_file_complete
    elif interval_type == 'seconds':
        previous_printed_interval = previous_printed_interval or datetime.now().timestamp()
        current_value = datetime.now().timestamp()
    next_interval = previous_printed_interval + interval
    if current_value >= next_interval:
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        print(f"{timestamp}: {percent_file_complete}% complete ({size} of {expected_size})")
        return next_interval
    return previous_printed_interval

try:
    config_file = os.getenv('SF_CONFIG_YML') or os.path.join(os.path.dirname(__file__), 'config.yml')
    with open(config_file) as f:
        config_hash = yaml.safe_load(f)
    for name, value in config_hash.items():
        globals()[f"{name}"] = value

    result = login()
    urls = download_index(result).split("\n")
    print("All urls:")
    print(urls)
    print()

    if not os.path.isdir(data_directory):
        os.makedirs(data_directory)

    for url in urls:
        fn = file_name(url)
        file_path = os.path.join(data_directory, fn)
        retry_count = 0
        while retry_count < 5:
            try:
                print(f"Working on: {url}")
                expected_size = get_download_size(result, url)
                print(f"Expected size: {expected_size}")
                fs = os.path.getsize(file_path) if os.path.exists(file_path) else None
                if fs and fs == expected_size:
                    print(f"File {fn} exists and is the right size. Skipping.")
                else:
                    download_file(result, url, expected_size)
                break
            except Exception as e:
                retry_count += 1
                print(f"Error: {e}")
                print("Retrying (retry_count of 5)...")

    print("Done!")
except Exception as e:
    print(e)
