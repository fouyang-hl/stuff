import os

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import httplib2
from apiclient import discovery

def get_gsheet_service():

    #########################################################################
    # If modifying these scopes, delete your previously saved credentials
    # at ~/.credentials/sheets.googleapis.com-python-quickstart.json
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    CLIENT_SECRET_FILE = 'client_secret_gsheet.json'
    PATH2CLIENT_SECRET_FILE = '/Users/kumiko.kashii/Desktop/python_path/' + CLIENT_SECRET_FILE
    APPLICATION_NAME = 'Google Sheets API Python Quickstart'
    #########################################################################

    # Path to the credential file
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    # Get credentials
    store = Storage(credential_path)
    credentials = store.get()

    # Create if invalid
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(PATH2CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store, None)
        print('Storing credentials to ' + credential_path)

    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl, cache_discovery=False)

    return service

def get_gdrive_service():

    #########################################################################
    # If modifying these scopes, delete your previously saved credentials
    # at ~/.credentials/drive-python-quickstart.json
    SCOPES = 'https://www.googleapis.com/auth/drive'
    CLIENT_SECRET_FILE = 'client_secret_gdrive.json'
    PATH2CLIENT_SECRET_FILE = '/Users/kumiko.kashii/Desktop/python_path/' + CLIENT_SECRET_FILE
    APPLICATION_NAME = 'Drive API Python Quickstart'
    #########################################################################

    # Path to the credential file
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    # Get credentials
    store = Storage(credential_path)
    credentials = store.get()

    # Create if invalid
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(PATH2CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store, None)
        print('Storing credentials to ' + credential_path)

    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http, cache_discovery=False)

    return service
