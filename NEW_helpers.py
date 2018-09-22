import os
import requests

from simple_salesforce import SalesforceLogin, Salesforce
from gsheet_gdrive_api import *
from googleapiclient import errors
from apiclient.http import MediaFileUpload

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import re

#################################################################
PAS_WHICH_GSHEET = {2018: '1pK_NC4dtkvx97QHAD09cCLtioF8P2F3pWxVQDWV4yY8',
                    2017: '1E-20h0owGXilIHBrCjpUoY_900wOv83LSKOeBGsUeXw'}
CPUV_GOALS_WHICH_GSHEET = {2018: '1aaiV_sjNCdrdQNXHsoDb9UTmcbKo0CjIRJ4umqKFW_4',
                           2017: '1_9ahzi07esrSELD2LWT4aQ3pGu5mXqip9b_tg7D7A_w'}

CC_TRACKER_GSHEET = {'Drugs.com': '1zXIoBgG2BG7qH-83SkOsvdlJe9tVnmvGCArCbV-eSfw',
                     'GoodRx': '1lqc2zkX88f8xTB2szFuwNgoN_PC0G2y60onJw4ad4sE'}
#################################################################

###################################################################
# Basic
###################################################################

def start_end_month(formatted_date):
    """Return the first date of the month and the last date of the month."""

    start_date = formatted_date.replace(day=1)
    this_month = start_date.month
    this_month_year = start_date.year
    next_month = this_month + 1
    if next_month == 13:
        next_month = 1
        next_month_year = this_month_year + 1
    else:
        next_month_year = this_month_year
    end_date = date(next_month_year, next_month, 1) - timedelta(days=1)
    return (start_date, end_date)

def get_last_modified_local(path):
    """Return the modified date of a file in a local machine, converted to UTC (= PST + 7hr)
    Return None if no such file exists.
    """
    
    if os.path.isfile(path):
        last_modified = os.path.getmtime(path)
        last_modified = datetime.utcfromtimestamp(last_modified)
        return last_modified
    return None

def get_last_modified_gdrive(file_id):
    """Return the modified date of a file in Google Drive, in UTC (=PST + 7hr)."""

    service = get_gdrive_service()

    try:
        file = service.files().get(fileId=file_id, fields='modifiedTime').execute()
        last_modified = datetime.strptime(file['modifiedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return last_modified
    except errors.HttpError as error:
        print('An error occurred: %s' % error)

def check_and_make_dir(path):
    """Make a director if it doesn't exist."""

    if not os.path.exists(path):
        os.makedirs(path)

###################################################################
# Salesforce (a.k.a. DAS), PAS, CPUV Goals, RevShare
###################################################################

def get_salesforce_login_info():
    """Return username, password, and security token of a Salesforce login account.
    The values need to be updated if anything changes.
    """

    username = 'kumiko.kashii@healthline.com'
    password = 'data007team'
    security_token = '1nT42QawFDRq1iR3p5agqy8d1'
    return (username, password, security_token)

def make_das(use_scheduled_units=False, export=False):
    """Return DAS report as a dataframe.

    If use_scheduled_units is True, then it uses Scheduled Units in Salesforce. If False, then it uses
    Actual Units where available, and Scheduled Units elsewhere.

    If export is True, then it saves the dataframe as a csv.
    """

    username, password, security_token = get_salesforce_login_info()

    #1. Export Ad Ops DAS Reporting from Salesforce (Converted Report ID: 00O61000003rUoxEAE, Non-Converted Report ID: 00O61000003KY4AEAW)
    (session_id, instance) = SalesforceLogin(username=username, password=password, security_token=security_token)
    query_url = 'https://' + instance + '/00O61000003rUox?export=1&enc=UTF-8&xf=csv'
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + session_id,
               'X-PrettyPrint': '1'}
    s = requests.Session()
    response = s.get(query_url, headers=headers, cookies={'sid': session_id})

    f = open('sf_das.csv', 'wb')
    f.write(response.content)
    f.close()

    #2. Clean up
    sf_das = pd.read_csv('sf_das.csv', encoding='utf-8')
    sf_das = sf_das.fillna('N/A')

    for col in sf_das.columns.tolist():
        if ' (converted)' in col:
            sf_das = sf_das.rename(columns={col: col.replace(' (converted)', '')})

    sf_das.loc[sf_das['Actual Units'] == 'N/A', 'Actual Units'] = sf_das.loc[sf_das['Actual Units'] == 'N/A', 'Scheduled Units']
    sf_das.loc[sf_das['Actual Amount'] == 'N/A', 'Actual Amount'] = sf_das.loc[sf_das['Actual Amount'] == 'N/A', 'Contracted Amount']

    for col in ['Sales Price', 'Base Rate', 'Baked-In Production Rate', 'Total Price', 'Total Units',
                'Scheduled Units', 'Actual Units', 'Contracted Amount', 'Actual Amount']:
        sf_das.loc[sf_das[col] == 'N/A', col] = 0

    #3. Pivot to create monthly Actual Units columns
    index_list = ['BBR', 'Campaign Name', 'Flight Type', 'Brand: Brand Name', 'Account Name: Account Name', 'Agency: Account Name',
                  'IO Number', 'Start Date', 'End Date', 'Approval Date', 'Stage', 'Billing Details', 'Customer Billing ID', 'Billing Profile Name',
                  'Opportunity Owner: Full Name',
                  '2nd Opportunity Owner: Full Name', 'Client Services User: Full Name', 'Campaign Manager: Full Name', 'Advertiser Vertical',
                  'Product: Product Name', 'Budget Category', 'Media Product', 'Media Product Family', 'Advertiser Vertical Family',
                  'Contracted Sites', 'Contracted Devices', 'Line Item Number', 'OLI', 'Billable Reporting Source',
                  'Viewability Source', 'Viewability', 'Blocking System', 'Line Description', 'Contracted Sizes', 'Price Calculation Type',
                  'Sales Price', 'Base Rate', 'Baked-In Production Rate', 'Total Price', 'Total Units']

    if use_scheduled_units:
        das = pd.pivot_table(sf_das, index=index_list, columns=['Active Month'], values='Scheduled Units', fill_value=0, aggfunc=np.sum)
    else:
        das = pd.pivot_table(sf_das, index=index_list, columns=['Active Month'], values='Actual Units', fill_value=0, aggfunc=np.sum)
    das = das.reset_index()

    #4. Convert dates to date type
    for col in ['Start Date', 'End Date', 'Approval Date']:
        das.loc[das[col] != 'N/A', col] = das[das[col] != 'N/A'].apply(lambda row: datetime.strptime(row[col], '%m/%d/%Y').date(), axis=1)

    #5. Rename index portion of header
    rename_dict = {'Brand: Brand Name': 'Brand',
                   'Account Name: Account Name': 'Account Name',
                   'Agency: Account Name': 'Agency',
                   'Opportunity Owner: Full Name': 'Opportunity Owner',
                   '2nd Opportunity Owner: Full Name': '2nd Opportunity Owner',
                   'Client Services User: Full Name': 'Account Manager',
                   'Campaign Manager: Full Name': 'Campaign Manager',
                   'Product: Product Name': 'Product',
                   'Billing Profile Name': 'Customer Billing Name'}
    das = das.rename(columns=rename_dict)

    renamed_index_list = []
    for index in index_list:
        if index in rename_dict:
            renamed_index_list.append(rename_dict[index])
        else:
            renamed_index_list.append(index)

    #6. Reorder months
    months_list = sf_das['Active Month'].drop_duplicates().values.tolist()
    months_flipped_list = []
    for month in months_list:
        if re.search('([0-9]+)/', month):
            mo = re.search('([0-9]+)/', month).group(1)
        else:
            continue
        if len(mo) == 1:
            mo = '0' + mo
        yr = re.search('/([0-9]+)', month).group(1)
        months_flipped_list.append(yr + '/' + mo)
    months_flipped_list.sort()

    months_ordered_list = []
    for month_flipped in months_flipped_list:
        mo = re.search('/([0-9]+)', month_flipped).group(1)
        if mo[0] == '0':
            mo = mo[1]
        yr = re.search('([0-9]+)/', month_flipped).group(1)
        months_ordered_list.append(mo + '/' + yr)

    #7. Output
    das = das[renamed_index_list + months_ordered_list]
    das = das.sort_values(['BBR', 'Campaign Name', 'Line Item Number', 'Line Description'])
    das = das[das['Price Calculation Type'] != 'N/A']

    if export:
        das.to_csv('das.csv', index=False, encoding='utf-8')
    #os.remove('sf_das.csv')
    return das

def get_pas(year, sheet):
    """Return Partner Allocation Sheet as a dataframe."""

    spreadsheetId = PAS_WHICH_GSHEET[year]

    service = get_gsheet_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=sheet, valueRenderOption='UNFORMATTED_VALUE').execute()
    values = result.get('values', [])
    pas_df = pd.DataFrame(values[1:])
    pas_df.columns = pas_df.iloc[0]
    pas_df = pas_df[1:].reset_index(drop=True)

    col_rename_dict = {}
    for col in pas_df.columns.tolist():
        if not isinstance(col, str):
            new_col = date(1900, 1, 1) + timedelta(days=int(col) - 2)
            new_col = str(new_col.month) + '/' + str(new_col.year)
            col_rename_dict[col] = new_col
    pas_df = pas_df.rename(columns=col_rename_dict)

    # Convert date columns from Int type to Date type
    for date_col in ['Start Date', 'End Date']:
        pas_df[date_col] = [(date(1900, 1, 1) + timedelta(days=d - 2)) for d in pas_df[date_col]]

    # Empty cells are in the string type. Convert them to nan (float)
    def empty_str_to_nan(cell):
        if cell == '':
            return None
        else:
            return cell

    pas_df = pas_df.applymap(empty_str_to_nan)

    return pas_df

def get_cpuv_goals(year, sheet):
    """Return CPUV Goals Sheet as a dataframe."""

    spreadsheetId = CPUV_GOALS_WHICH_GSHEET[year]

    service = get_gsheet_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=sheet, valueRenderOption='UNFORMATTED_VALUE').execute()
    values = result.get('values', [])
    uv_goals_df = pd.DataFrame(values[1:])
    uv_goals_df.columns = uv_goals_df.iloc[0]
    uv_goals_df = uv_goals_df[1:].reset_index(drop=True)
    uv_goals_df = uv_goals_df[[isinstance(cell, str) for cell in uv_goals_df['OLI']]]

    # Empty cells are in the string type. Convert them to numeric (float).
    for site_goal in ['HL Goal', 'Drugs Goal', 'GoodRx Goal', 'MNT Goal', 'BCO Goal', 'LS Goal', 'EmpowHer Goal']:
        if site_goal in uv_goals_df.columns.tolist():
            uv_goals_df[site_goal] = pd.to_numeric(uv_goals_df[site_goal])

    # Convert date columns from Int type to Date type
    for date_col in ['Start Date', 'End Date']:
        uv_goals_df[date_col] = [(date(1900, 1, 1) + timedelta(days=d - 2)) for d in uv_goals_df[date_col]]

    return uv_goals_df

def get_revshare_dict():
    """Return partner revenue share dictionary.
    This is what we pay each site. For example, HL pays Drugs.com 60% of gross revenue.
    """

    revshare_dict = {'Black Health Matters': 0.5,
                     'Dr.Gourmet': 0.51,
                     'Drugs.com': 0.6,
                     'eHow': 0.5,
                     'eMedTV': 0.5,
                     'EmpowHer': 0.46,
                     'GoodRx': 0.5,
                     'HL': 1,
                     'Livestrong': 0.4,
                     'Medical News Today': 0.0,
                     'Patient Info': 0.4,
                     'SkinSight': 0.5,
                     'Breastcancer.org': 0.5}
    return revshare_dict

###################################################################
# Salesforce queries
###################################################################

def get_expedited_invoice_opportunities():
    """Return a dataframe of Salesforce opportunities where the Expedited Invoice field is True."""
 
    username, password, security_token = get_salesforce_login_info()
    sf = Salesforce(username=username, password=password, security_token=security_token)
    query = "SELECT BBR__c, Name, Expedited_Invoicing_Requested__c FROM Opportunity"
    result = sf.query_all(query)
    records = result['records']

    values = []
    for record in records:
        bbr = record['BBR__c']
        oppt_name = record['Name']
        exp_inv = record['Expedited_Invoicing_Requested__c']
        values.append([bbr, oppt_name, exp_inv])

    header = ['BBR', 'Opportunity Name', 'Expedited Invoice']
    df = pd.DataFrame(values, columns=header)
    df = df[df['Expedited Invoice']]  # Only select True

    return df

###################################################################
# DAS-related
###################################################################

def bbr2brand(df, bbr, das):
    """Return a datafame with a BBR column added to the input df.
    The input df requires a Brand column.
    """

    das_bbr_brand = das[['BBR', 'Brand']].drop_duplicates()
    df = pd.merge(df, das_bbr_brand, how='left', left_on=bbr, right_on='BBR')
    df = df.drop('BBR', axis=1)
    return df

def bbr2cm(df, bbr, das):
    """Return a dataframe with a Camapign Manager column added to the input df.
    The input df requires a BBR column.
    
    Currently exclude Campaign Manager == 'SEM'.
    When 'SEM' is to be added, it's necessary to join on ['BBR', 'Line Description'] or 'OLI'.
    """

    das_bbr_cm = das[das['Campaign Manager'] != 'SEM'][['BBR', 'Campaign Manager']].drop_duplicates()
    df = pd.merge(df, das_bbr_cm, how='left', left_on=bbr, right_on='BBR')
    df = df.drop('BBR', axis=1)
    return df

def bbr2camp(df, bbr, das):
    """Return a datafame with a Campaign Name column added to the input df.
    The input df requires a BBR column.
    """
    
    das_bbr_camp = das[['BBR', 'Campaign Name']].drop_duplicates()
    df = pd.merge(df, das_bbr_camp, how='left', left_on=bbr, right_on='BBR')
    df = df.drop('BBR', axis=1)
    return df

def das_filtered(das, das_month):
    """Return a dataframe of this month's DAS excluding SEM campaigns. Only CPM & CPUV.
    das_month is a string in the form of 'mm/yyyy'.
    """

    df = das[das[das_month] > 0]
    df = df[(df['Price Calculation Type'] == 'CPM') |
            (df['Price Calculation Type'] == 'CPUV')]
    df = df[df['Campaign Manager'] != 'SEM']
    return df

############################################
# Other tables
# - Exlude List (DFP creatives under BBR that are not billable)
# - Drugs IO Naming (mapping from internal camp/line desc. to ones in drugs.com IO)
############################################

def get_exclude_list():
    """Return a dataframe of the Google Sheet exclude list.
    These are non-billable creatives under Orders in DFP tht contain 'BBR' in the Order name.
    """

    #################################################################
    spreadsheetId = '10RD_2cF0jytoCBT-2bui1pRBiP9B4UPdB0VWzxsGeg4'
    #################################################################

    service = get_gsheet_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range='non-billable', valueRenderOption='UNFORMATTED_VALUE').execute()
    values = result.get('values', [])
    df = pd.DataFrame(values)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    return df

def get_partner_io_naming(site, sheet):

    #################################################################
    if site == 'Drugs':
        spreadsheetId = '1Mx3F6K1jnf01ra2sjutmia7rMULLNlEacCcHjo98-j0'
    elif site == 'GoodRx':
        spreadsheetId = '1nCwy9nCzLcDqbhXHMNg8opT3zVtIWuBvpnkbFwjvbBQ'
    #################################################################

    # If sheet doesn't exist, create
    if gsheet_get_sheet_id_by_name(sheet, spreadsheetId) is None:
        year = int(sheet[:4])
        mo = int(sheet[4:])

        if mo == 1:
            year -= 1
            mo = 12
        else:
            mo -= 1

        last_month_sheet = str(year) + str(mo).zfill(2)
        gsheet_copy_sheet(last_month_sheet, sheet, spreadsheetId)

    service = get_gsheet_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=sheet, valueRenderOption='UNFORMATTED_VALUE').execute()
    values = result.get('values', [])
    df = pd.DataFrame(values)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # Replace empty string with None
    df = df.applymap(lambda cell: None if cell == '' else cell)

    return df

def get_drugs_io_naming(sheet):
    """Return a dataframe of the Google Sheet drugs.com IO naming table.
    Each of the internal (Campaign Name, Line Description) has a (Campaign, Placement) for drugs.com IO.
    sheet is a string in the form of 'yyyymm'.
    """

    return get_partner_io_naming('Drugs', sheet)

def get_goodrx_io_naming(sheet):
    return get_partner_io_naming('GoodRx', sheet)

############################################
# Google Drive-related
############################################

def save_in_gdrive(file_name, folder_id, mimetype):
    """Save a file in a specified Google Drive folder with a specified mimetype.
    Return the Google Drive file id of a created file.
    """

    service = get_gdrive_service()

    file_metadata = {'name': file_name,
                     'parents': [folder_id]}
    media = MediaFileUpload(file_name,
                            mimetype=mimetype)
    file = service.files().create(body=file_metadata,
                                  media_body=media,
                                  fields='id').execute()
    return file.get('id')

def save_csv_as_gsheet_in_gdrive(file_name, folder_id, path2csv):
    """Save a csv in a specified Google Drive folder as a Google Sheet file.
    Return the Google Drive file id of a created file.
    """

    service = get_gdrive_service()

    file_metadata = {'name': file_name,
                     'parents': [folder_id],
                     'mimeType': 'application/vnd.google-apps.spreadsheet'}
    media = MediaFileUpload(path2csv,
                            mimetype='text/csv',
                            resumable=True)
    file = service.files().create(body=file_metadata,
                                  media_body=media,
                                  fields='id').execute()
    return file.get('id')

def save_excel_as_gsheet_in_gdrive(file_name, folder_id, path2excel):
    """Save an excel in a specified Google Drive folder as a Google Sheet file.
    Return the Google Drive file id of a created file.
    """

    service = get_gdrive_service()

    file_metadata = {'name': file_name,
                     'parents': [folder_id],
                     'mimeType': 'application/vnd.google-apps.spreadsheet'}
    media = MediaFileUpload(path2excel,
                            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            resumable=True)
    file = service.files().create(body=file_metadata,
                                  media_body=media,
                                  fields='id').execute()
    return file.get('id')

def delete_in_gdrive(file_id):
    """Delete a file with a specified file id in Google Drive."""

    service = get_gdrive_service()

    try:
        service.files().delete(fileId=file_id).execute()
    except errors.HttpError as error:
        print('An error occurred: %s' % error)

    return None

def gdrive_get_file_id_by_name(name, folder_id):
    """Return the file id of a file with a specified name in a specified Google Drive folder."""

    service = get_gdrive_service()
    id = None

    page_token = None
    while True:
        response = service.files().list(q="'" + folder_id + "' in parents" + " and " +
                                          "name = '" + name + "'",
                                        spaces='drive',
                                        fields='nextPageToken, files(id)',
                                        pageToken=page_token).execute()

        for file in response.get('files', []):
            id = file.get('id')

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return id

def gdrive_get_most_recent_file_id(folder_id):
    """Return the file id of a file with the most recent modified timestamp in a specified Google Drive folder."""

    service = get_gdrive_service()

    most_recent_file_id = None
    most_recent_file_modified = datetime(1999, 1, 2, 1, 2, 3)

    page_token = None
    while True:
        response = service.files().list(q="'" + folder_id + "' in parents" + " and " +
                                          "trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, modifiedTime)',
                                        pageToken=page_token).execute()

        for file in response.get('files', []):
            last_modified_time = file.get('modifiedTime')
            last_modified_time = datetime.strptime(last_modified_time, '%Y-%m-%dT%H:%M:%S.%fZ')
            if last_modified_time > most_recent_file_modified:
                most_recent_file_id = file.get('id')
                most_recent_file_modified = last_modified_time

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return most_recent_file_id

def gdrive_get_file_info_list(folder_id):
    """Return a list dictionaries, each of which contains information about a file in a specified Google Drive folder.
    Each dictionary consists of 'id' (file id), 'name' (file name), and 'last_modified' (last modified timestamp in UTC).
    """

    service = get_gdrive_service()

    folder_content = []
    page_token = None
    while True:
        response = service.files().list(q="'" + folder_id + "' in parents" + " and " +
                                          "trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name, modifiedTime)',
                                        pageToken=page_token).execute()

        for file in response.get('files', []):
            file_info = {'id': file.get('id'),
                         'name': file.get('name'),
                         'last_modified': datetime.strptime(file['modifiedTime'],
                                                            '%Y-%m-%dT%H:%M:%S.%fZ')}
            folder_content.append(file_info)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return folder_content

def gdrive_copy_file(file_id, new_file_name):
    """Make a copy of a specified file in the same Google Drive folder, and rename it to new_file_name."""

    service = get_gdrive_service()

    try:
        return service.files().copy(fileId=file_id, body={'name': new_file_name}).execute()
    except errors.HttpError as error:
        print('An error occurred: %s' % error)

    return None

############################################
# Google Sheet-related
############################################

def gsheet_get_sheet_id_by_name(name, ss_id):
    """Return a sheet id of a sheet with a specified name in a specified Google Sheet spreadsheet."""

    service = get_gsheet_service()

    ss_metadata = service.spreadsheets().get(spreadsheetId=ss_id).execute()

    for sheet_metadata in ss_metadata['sheets']:
        if sheet_metadata['properties']['title'] == name:
            id = sheet_metadata['properties']['sheetId']
            return id

    return None

def gsheet_create_sheet(name, ss_id):
    """Create a new sheet with a specified name in a specified Google Sheet spreadsheet.
    Return the sheet id of a created sheet.
    """

    service = get_gsheet_service()

    request_body = {'requests': [{'addSheet': {'properties': {'title': name}}}]}
    result = service.spreadsheets().batchUpdate(spreadsheetId=ss_id, body=request_body).execute()
    
    sheet_id = result['replies'][0]['addSheet']['properties']['sheetId']
    return sheet_id

def gsheet_delete_sheet(name, ss_id):
    """Delete a sheet with a specified name in a specified Google Sheet spreadsheet."""

    service = get_gsheet_service()

    s_id = gsheet_get_sheet_id_by_name(name, ss_id)
    if s_id is not None:
        request_body = {'requests': [{'deleteSheet': {'sheetId': s_id}}]}
        result = service.spreadsheets().batchUpdate(spreadsheetId=ss_id, body=request_body).execute()

    return None

def gsheet_move_sheet(name, ss_id, new_index=0):
    """Move a sheet with a specified name in a specified Google Sheet spreadsheet, to a specified index."""

    service = get_gsheet_service()

    s_id = gsheet_get_sheet_id_by_name(name, ss_id)
    if s_id is not None:
        request_body = {'requests': [{'updateSheetProperties': {'properties': {'sheetId': s_id,
                                                                               'index': new_index},
                                                                'fields': 'index'}}]}
        result = service.spreadsheets().batchUpdate(spreadsheetId=ss_id, body=request_body).execute()

    return None

def gsheet_rename_sheet(name, new_name, ss_id):
    """Rename a sheet name"""
    service = get_gsheet_service()

    sheet_id = gsheet_get_sheet_id_by_name(name, ss_id)
    request = [{'updateSheetProperties': {'properties': {'sheetId': sheet_id,
                                                         'title': new_name},
                                          'fields': 'title'}}]
    result = service.spreadsheets().batchUpdate(spreadsheetId=ss_id,
                                                body={'requests': request}).execute()
    return None

def gsheet_copy_sheet(copy_sheet_name, as_sheet_name, ss_id):
    """Copy a sheet with a specified name within a specified Google Sheet spreadsheet."""

    service = get_gsheet_service()

    copy_sheet_id = gsheet_get_sheet_id_by_name(copy_sheet_name, ss_id)
    requst_body = {'destination_spreadsheet_id': ss_id}
    result = service.spreadsheets().sheets().copyTo(spreadsheetId=ss_id, sheetId=copy_sheet_id, body=requst_body).execute()

    gsheet_rename_sheet('Copy of ' + copy_sheet_name, as_sheet_name, ss_id)
    gsheet_move_sheet(as_sheet_name, ss_id)

    return None


