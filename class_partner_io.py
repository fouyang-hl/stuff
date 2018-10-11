
import calendar
from datetime import date, datetime
import copy
import time

import pandas as pd
import numpy as np
import openpyxl as opx

from NEW_helpers import *
from gsheet_gdrive_api import *
from byme_helpers import update_partner_io_naming, get_booked_future_months

pd.options.mode.chained_assignment = None  # default='warn'


class PartnerIO:
    # Static #######################################################
    ss_id = {'Drugs': '1738A3UqxhauAfNpAWHD2WWOR3pUT9duOYNVeNZKisP8',  # Real
             'GoodRx': '1NTrPFCNNTwkXqY68gk9vu_pdLux1f0rxjkWb6yMzRMs'} # Real
             #'Drugs': '1fuO56JaK9b59Q9GdQt9YxxhOhylYEKoF8je9zPCcAvc'}  # Test
             #'GoodRx': '1zfAlJtYUuBaHRMky4BAyRxi3RJ-5RrvO5_oFhP4Rnao'}  # Test}

    revshare = {'Drugs': 0.6, 'GoodRx': 0.5}
    drugs_cc_rate = 0.5

    # Sheet names
    log_sheet_name = 'log'
    booked_sheet_name = 'Booked'

    # Column names
    col_dates = 'Dates'
    col_camp = 'Campaign Name'
    col_pl = 'Placement'
    col_device = 'Devices (D, T, M)'
    col_size = 'Size'
    col_goal = 'Impressions/Uvs'
    col_rate = 'CPM/CPUV'
    col_rev = 'Total Revenue'
    col_disc = 'Expected Discrepancy/Unbillability'
    col_aim = 'Aim Towards Impressions'
    col_drugs_specific = 'Drugs.com-specific'
    col_notlive_rev = 'Not Live Revenue'

    # Section names
    sec_cpm = 'CPM'
    sec_cpuv_cc = 'CPUV Competitive Conquesting'
    sec_cpuv_ms = 'CPUV Microsite'

    check_diff = {sec_cpm: [col_dates, col_goal, col_rate, col_disc],
                  sec_cpuv_cc: [col_dates, col_goal, col_rate],
                  sec_cpuv_ms: [col_dates, col_goal, col_rate]}

    # Colors
    color_light_green = {'red': .851, 'green': .918, 'blue': .827, 'alpha': 1}
    color_dark_green = {'red': .576, 'green': .769, 'blue': .49, 'alpha': 1}
    color_greenish_blue = {'red': .463, 'green': .647, 'blue': .686, 'alpha': 1}
    color_orange = {'red': 1, 'green': .949, 'blue': .8, 'alpha': 1}
    color_purple = {'red': .851, 'green': .824, 'blue': .914, 'alpha': 1}
    color_red = {'red': .957, 'green': .8, 'blue': .8, 'alpha': 1}
    color_white = {'red': 1, 'green': 1, 'blue': 1, 'alpha': 1}

    disc_color = {1: color_light_green,
                  2: color_dark_green,
                  3: color_greenish_blue}

    col2color = {col_dates: color_orange,
                 col_goal: color_purple,
                 col_rate: color_red,
                 col_disc: None}

    def get_sheet_name(mo_year):
        mo, year = mo_year
        return ' '.join([calendar.month_name[mo], str(year)])

    def update(site, mo_year, ith_disc_update=1, cpm=True, cpuv=True, record_log=True):
        io = PartnerIO(site)
        io.update_month(mo_year, ith_disc_update, cpm, cpuv, record_log)

    def make(site, mo_year, base_on_sheet=None):
        io = PartnerIO(site)
        io.make_month(mo_year, base_on_sheet)

    def update_goodrx_booked():
        io = PartnerIO('GoodRx')
        io.update_booked()

    # End Static ###################################################

    def __init__(self, site):
        self.service = get_gsheet_service()
        self.site = site
        self.ss_id = PartnerIO.ss_id[site]
        self.notlive_col = True  # True if (site == 'Drugs') else False
        self.revshare = PartnerIO.revshare[site]
        self.per_section = {}

    def set_log_attr(self, record_log):
        self.record_log = record_log
        if not record_log:
            return

        self.log_sheet_id = gsheet_get_sheet_id_by_name(PartnerIO.log_sheet_name, self.ss_id)
        if self.log_sheet_id is None:
            self.record_log = False
            print('* %s %s tab not found :(' % (self.site, self.log_sheet_name))
            return

        self.log_header = self.get_sheet_values(self.log_sheet_name)[0]

    def update_month(self, mo_year, ith_disc_update=1, cpm=True, cpuv=True,
                     record_log=True, remove_disappear=False):
        self.set_monthly_attr(mo_year)
        self.set_col2color(ith_disc_update)
        self.set_up2date_goals(cpm, cpuv)  # Creates 'up2date' key in per_section[section] dict
        self.set_log_attr(record_log)  # Creates self.record_log

        for section in PartnerIO.check_diff:
            if section not in self.per_section:
                continue
            if self.per_section[section]['up2date'] is None:
                continue
            self.update_per_section(section, remove_disappear)

        if not remove_disappear:
            self.warn_disappear()

    def set_col2color(self, ith_disc_update):
        self.col2color = copy.deepcopy(PartnerIO.col2color)
        self.col2color[PartnerIO.col_disc] = PartnerIO.disc_color[ith_disc_update]

    def set_monthly_attr(self, mo_year):
        self.mo, self.year = mo_year
        self.month_start_date = date(self.year, self.mo, 1)
        self.month_end_date = start_end_month(self.month_start_date)[1]
        self.str_entire_month_dates = self.get_str_monthly_dates(self.month_start_date, self.month_end_date)

        self.sheet_name = PartnerIO.get_sheet_name(mo_year)
        self.sheet_id = gsheet_get_sheet_id_by_name(self.sheet_name, self.ss_id)

        self.naming = get_partner_io_naming(self.site, str(self.year) + str(self.mo).zfill(2))

    def format_goals_helper(self, df, header, col):
        df = df[[not isinstance(sg, str) for sg in df[col['site_goal']]]]
        df = df[df[col['site_goal']] > 0]
        df = df[header].rename(columns={PartnerIO.col_camp: 'Internal Campaign Name'})
        df = pd.merge(df, self.naming, how='left', on=['Internal Campaign Name', 'Line Description'])
        df['Site Rate'] = df[col['base_rate']] * self.revshare
        df['Start Date'] = [(d.date() if not isinstance(d, date) else d) for d in df['Start Date']]
        df['End Date'] = [(d.date() if not isinstance(d, date) else d) for d in df['End Date']]

        if len(df[pd.isnull(df[PartnerIO.col_camp])]) > 0:
            print('Update %s IO Naming Google Sheet first.' % (self.site))
            quit()

        return df

    def set_up2date_goals(self, cpm=True, cpuv=True):
        if cpm:
            pas = get_pas(self.year, calendar.month_name[self.mo][: 3])
            if self.site == 'Drugs':
                self.pas_col_disc = 'MTD Disc'
            else:
                self.pas_col_disc = 'Overall MTD Disc'
            header = [PartnerIO.col_camp, 'Start Date', 'End Date', 'Line Description', 'Contracted Sizes',
                      self.pas_col_disc, 'Sales Price', self.site]
            col = {'site_goal': self.site, 'base_rate': 'Sales Price'}
            cpm = self.format_goals_helper(pas, header, col)

            #### Aug fix ####
            #cpm.loc[cpm['Campaign Name'].str.contains('Xiidra'), 'End Date'] = date(2018, 8, 1)
            #################

            if self.site != 'Drugs':  # Drugs has its own disc column in PAS
                cpm.loc[cpm[self.pas_col_disc] <= 0.05, self.pas_col_disc] = ''

            if PartnerIO.sec_cpm not in self.per_section:
                self.per_section[PartnerIO.sec_cpm] = {}
            if len(pas) > 0:
                self.per_section[PartnerIO.sec_cpm]['up2date'] = cpm
            else:
                self.per_section[PartnerIO.sec_cpm]['up2date'] = None

        if cpuv:
            cpuv_goals = get_cpuv_goals(self.year, calendar.month_name[self.mo][: 3])
            site_goal_col = self.site + ' Goal'
            header = [PartnerIO.col_camp, 'Start Date', 'End Date', 'Line Description', 'Base Rate', site_goal_col]
            col = {'site_goal': site_goal_col, 'base_rate': 'Base Rate'}
            cpuv_goals = self.format_goals_helper(cpuv_goals, header, col)

            cc = cpuv_goals[cpuv_goals['Line Description'].str.contains('Competitive Conquesting', case=False) | cpuv_goals['Line Description'].str.contains('Brand Championing', case=False)]
            ms = cpuv_goals[-cpuv_goals['Line Description'].str.contains('Competitive Conquesting', case=False) & -cpuv_goals['Line Description'].str.contains('Brand Championing', case=False)]

            # Adjustments
            if self.site == 'Drugs':
                cc['Site Rate'] = PartnerIO.drugs_cc_rate
            if self.site == 'GoodRx':
                cc.loc[cc['Internal Campaign Name'] == 'Neulasta BC Sept - Dec 2018', ('Site Rate', site_goal_col)] = (712, 1)

            if PartnerIO.sec_cpuv_cc not in self.per_section:
                self.per_section[PartnerIO.sec_cpuv_cc] = {}
            if len(cc) > 0:
                self.per_section[PartnerIO.sec_cpuv_cc]['up2date'] = cc
            else:
                self.per_section[PartnerIO.sec_cpuv_cc]['up2date'] = None

            if PartnerIO.sec_cpuv_ms not in self.per_section:
                self.per_section[PartnerIO.sec_cpuv_ms] = {}
            if len(ms) > 0:
                self.per_section[PartnerIO.sec_cpuv_ms]['up2date'] = ms
            else:
                self.per_section[PartnerIO.sec_cpuv_ms]['up2date'] = None

    def print_update_message(self, section, remove_disappear):
        message = 'Updating %s %s... ' % (self.site, section)
        message += '%d changes' % (len(self.per_section[section]['change']))
        message += ', %d adds' % (len(self.per_section[section]['add']))
        if remove_disappear:
            message += ', %d removes' % (len(self.per_section[section]['disappear']))
        print(message)

    def update_per_section(self, section, remove_disappear=False):
        print(self.site + ', ' + section)
        self.set_current_io(section)
        self.set_combined(section)
        self.set_disappear_change_add(section)
        self.print_update_message(section, remove_disappear)

        self.upload_change(section)
        self.insert_add(section)
        self.sort(section)
        if remove_disappear:
            self.remove_disappear(section)
        self.update_total()

    def remove_disappear(self, section):
        self.set_current_io(section)
        self.set_combined(section)
        self.set_disappear_change_add(section)

        df = self.per_section[section]['disappear']
        if len(df) < 1:
            return

        requests = []
        i_list = df['Row Index'].tolist()
        for i in range(len(i_list)):
            i_row = i_list[i]
            requests.append({'deleteDimension': {'range': {'sheetId': self.sheet_id,
                                                           'dimension': 'ROWS',
                                                           'startIndex': i_row - i,
                                                           'endIndex': i_row + 1 - i}}})

        print(requests)
        result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': requests}).execute()

    def init_per_section_dict(self):
        self.values = self.get_sheet_values(self.sheet_name)

        # Collect info
        section_order = -1
        section_dict = {}
        for i in range(len(self.values)):
            row = self.values[i]
            if len(row) < 1:
                continue

            if row[0] == PartnerIO.col_dates:  # Assumes col A is dates
                self.i_header = i
                self.header = copy.deepcopy(row)

            elif row[0] in PartnerIO.check_diff:
                section_order += 1
                name = row[0]
                i_start = i + 1
                section_dict[section_order] = {'name': name, 'i_start': i_start}

                # Update previous section
                if section_order > 0:
                    section_dict[section_order - 1]['i_end'] = i - 1

            elif row[0] == 'Total':
                self.i_total = i
                section_dict[max(section_dict)]['i_end'] = i - 1

        # Set attributes
        for _, section in section_dict.items():
            name = section['name']
            i_start = section['i_start']
            i_end = section['i_end']
            if name in self.per_section:
                self.per_section[name]['i_start'] = i_start
                self.per_section[name]['i_end'] = i_end
            else:
                self.per_section[name] = {'i_start': i_start, 'i_end': i_end}

    def set_current_io(self, section):
        self.init_per_section_dict()
        i_start = self.per_section[section]['i_start']
        i_end = self.per_section[section]['i_end']

        header = ['Row Index'] + self.header
        len_header = len(header)
        values = copy.deepcopy(self.values[i_start: i_end + 1])
        for i in range(len(values)):
            values[i].insert(0, i + i_start)  # Add Row Index
            row = values[i]
            if len(row) < len_header:
                repeat_none = len_header - len(row)
                values[i] = row + list([None] * repeat_none)
            elif len(row) > len_header:  # When Drugs.com add notes in extra columns
                values[i] = values[i][: len_header]

        self.per_section[section]['current'] = pd.DataFrame(values, columns=header)

    def set_combined(self, section):
        up2date = self.per_section[section]['up2date']
        current = self.per_section[section]['current']
        up2date[PartnerIO.col_dates] = up2date.apply(lambda row: self.get_str_monthly_dates(row['Start Date'], row['End Date']), axis=1)
        # Format
        if section == PartnerIO.sec_cpm:
            up2date[PartnerIO.col_device] = 'TBFilled'
            rename_dict = {'Contracted Sizes': PartnerIO.col_size, self.site: PartnerIO.col_goal, 'Site Rate': PartnerIO.col_rate,
                           self.pas_col_disc: PartnerIO.col_disc}
        else:
            up2date[PartnerIO.col_device] = ''
            up2date[PartnerIO.col_size] = '1x1 (CC)' if section == PartnerIO.sec_cpuv_cc else '1x1'
            up2date[PartnerIO.col_disc] = ''
            rename_dict = {(self.site + ' Goal'): PartnerIO.col_goal, 'Site Rate': PartnerIO.col_rate}

        max_col = [PartnerIO.col_dates, PartnerIO.col_camp, PartnerIO.col_pl, PartnerIO.col_device, PartnerIO.col_size, PartnerIO.col_goal,
                   PartnerIO.col_rate, PartnerIO.col_disc]
        col = list(set(self.header).intersection(max_col))

        up2date = up2date.rename(columns=rename_dict)[col]
        up2date['in new'] = 1
        current = current[['Row Index'] + col]
        current['in old'] = 1
        self.per_section[section]['combined'] = pd.merge(up2date, current, how='outer',
                                                         on=[PartnerIO.col_camp, PartnerIO.col_pl], suffixes=('_new', '_old'))

    def set_disappear_change_add(self, section):
        df = self.per_section[section]['combined'].copy()
        df = df.fillna('')

        # Disappear (In current IO but not in PAS or CPUV Goals Sheet)
        self.per_section[section]['disappear'] = df[(df['in old'] == 1) & (df['in new'] != 1)]

        # Add (In PAS or CPUV Goals Sheet but not in current IO)
        self.per_section[section]['add'] = df[(df['in new'] == 1) & (df['in old'] != 1)]

        # Change (Exists in both but values are different)
        df = df[(df['in new'] == 1) & (df['in old'] == 1)]

        diffs = []
        for col in PartnerIO.check_diff[section]:
            col_type = df[col + '_new'].dtype.name
            if 'float' in col_type:
                df[col + '_old'] = [round(value, 3) if value != '' else '' for value in df[col + '_old']]
                df[col + '_new'] = [round(value, 3) if value != '' else '' for value in df[col + '_new']]
            if 'int' in col_type:
                df[col + '_old'] = [int(value) if value != '' else '' for value in df[col + '_old']]
                df[col + '_new'] = [int(value) if value != '' else '' for value in df[col + '_new']]
            df_col_diff = df[df[col + '_old'] != df[col + '_new']]
            df_col_diff['update ' + col] = 1
            diffs.append(df_col_diff)

        self.per_section[section]['change'] = pd.concat(diffs)

    def warn_disappear(self):
        df_list = []
        for section in PartnerIO.check_diff:
            if 'disappear' not in self.per_section[section]:
                continue
            disappear_df = self.per_section[section]['disappear']
            disappear_df['section'] = section
            df_list.append(disappear_df)

        df = pd.concat(df_list)
        if len(df) < 1:
            return

        message = '*' * 100 + '\n'
        message += 'In %s IO but not in PAS or CPUV Goals Sheet\n' % (self.site)
        message += '*' * 100 + '\n'
        for _, row in df.iterrows():
            message += '%s, %s, %s\n' % (row['section'], row[PartnerIO.col_camp], row[PartnerIO.col_pl])
        message += '*' * 100
        print(message)

    # Helpers for upload_change method ################
    def add_to_change_data(self, section, cell, value):
        self.per_section[section]['change data'].append({'range': self.sheet_name + '!' + cell,
                                                         'majorDimension': 'ROWS',
                                                         'values': [[value]]})

    def add_to_change_color(self, section, color, row_index, col_index):
        self.per_section[section]['change color'].append({'updateCells': {'rows': [{'values': [{"userEnteredFormat": {'backgroundColor': color}}]}],
                                                                          'fields': 'userEnteredFormat.backgroundColor',
                                                                          'range': {'sheetId': self.sheet_id,
                                                                                    'startRowIndex': row_index,
                                                                                    'endRowIndex': row_index + 1,
                                                                                    'startColumnIndex': col_index,
                                                                                    'endColumnIndex': col_index + 1}}})

    # End Helpers for upload_change method #############

    def upload_change(self, section):
        df = self.per_section[section]['change']
        if len(df) < 1:
            return
        self.per_section[section]['change data'] = []
        self.per_section[section]['change color'] = []

        for col in PartnerIO.check_diff[section]:
            col_index = self.header.index(col)
            col_letter = self.get_col_letter(col)
            color = self.col2color[col]
            for _, row in df[df['update ' + col] == 1].iterrows():
                row_index = row['Row Index']
                self.add_to_change_data(section, cell=col_letter + str(int(row_index + 1)), value=row[col + '_new'])
                self.add_to_change_color(section, color, int(row_index), col_index)

        result_upload_change_values = self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.ss_id,
            body={'valueInputOption': 'USER_ENTERED', 'data': self.per_section[section]['change data']}).execute()
        result_upload_change_color = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': self.per_section[section]['change color']}).execute()

        if self.record_log:
            self.log_change(section)

    # Helpers for insert_add method #############
    def insert_n_add_rows_at_top(self, section):
        self.insert_rows(sheet_id=self.sheet_id,
                         i_start=self.per_section[section]['i_start'],
                         n_rows=len(self.per_section[section]['add']))

    def add_to_add_data(self, section, row_index, row):
        str_row_n = str(row_index + 1)
        last_col_letter = self.get_col_letter(self.header[-1])
        cell = 'A' + str_row_n + ':' + last_col_letter + str_row_n

        row_values = []
        for col in self.header:
            if col in [PartnerIO.col_camp, PartnerIO.col_pl]:
                value = row[col]
            elif col + '_new' in row.index:
                value = row[col + '_new']
            else:
                value = ''
            row_values.append(value)

        # Formulas
        i_rev = self.header.index(PartnerIO.col_rev)
        i_aim = self.header.index(PartnerIO.col_aim)

        col_letter_goal = self.get_col_letter(PartnerIO.col_goal)
        col_letter_rate = self.get_col_letter(PartnerIO.col_rate)
        col_letter_disc = self.get_col_letter(PartnerIO.col_disc)
        col_letter_notlive_rev = self.get_col_letter(PartnerIO.col_notlive_rev)

        if section == PartnerIO.sec_cpm:
            if self.notlive_col:
                row_values[i_rev] = '=IF(' + col_letter_notlive_rev + str_row_n + '="", ' + col_letter_goal + str_row_n + '/1000*' + col_letter_rate + str_row_n + ', "")'
            else:
                row_values[i_rev] = '=' + col_letter_goal + str_row_n + '/1000*' + col_letter_rate + str_row_n
            row_values[i_aim] = '=IF(' + col_letter_disc + str_row_n + '<>"", ' + col_letter_goal + str_row_n + '/(1-' + col_letter_disc + str_row_n + '), "")'
        else:
            if self.notlive_col:
                row_values[i_rev] = '=IF(' + col_letter_notlive_rev + str_row_n + '="", ' + col_letter_goal + str_row_n + '*' + col_letter_rate + str_row_n + ', "")'
            else:
                row_values[i_rev] = '=' + col_letter_goal + str_row_n + '*' + col_letter_rate + str_row_n
            row_values[i_aim] = ''

        if self.site == 'Drugs':  # Drugs.com only
            i_drugs_specific = self.header.index(PartnerIO.col_drugs_specific)
            col_letter_pl = self.get_col_letter(PartnerIO.col_pl)
            row_values[i_drugs_specific] = '=IF(ISNUMBER(FIND("Drugs.com-specific", ' + col_letter_pl + str_row_n + ')), "Yes", "")'

        self.per_section[section]['add data'].append({'range': self.sheet_name + '!' + cell,
                                                      'majorDimension': 'ROWS',
                                                      'values': [row_values]})

    def add_to_add_color(self, section, row_index, row):
        colors = []
        for col in self.header:
            if col == PartnerIO.col_camp:
                color = PartnerIO.color_light_green
            elif col == PartnerIO.col_dates:
                if row[col + '_new'] == self.str_entire_month_dates:
                    color = PartnerIO.color_white
                else:
                    color = PartnerIO.color_orange
            else:
                color = color = PartnerIO.color_white
            colors.append({'userEnteredFormat': {'backgroundColor': color}})

        self.per_section[section]['add color'].append({'updateCells': {'rows': [{'values': colors}],
                                                                       'fields': 'userEnteredFormat.backgroundColor',
                                                                       'range': {'sheetId': self.sheet_id,
                                                                                 'startRowIndex': row_index,
                                                                                 'endRowIndex': row_index + 1,
                                                                                 'startColumnIndex': 0,
                                                                                 'endColumnIndex': len(self.header)}}})
    # End Helpers for insert_add method ##########

    def insert_add(self, section):
        df = self.per_section[section]['add']
        if len(df) < 1:
            return
        self.per_section[section]['add data'] = []
        self.per_section[section]['add color'] = []

        for i, row in df.reset_index(drop=True).iterrows():
            row_index = int(self.per_section[section]['i_start'] + i)
            self.add_to_add_data(section, row_index, row)
            self.add_to_add_color(section, row_index, row)

        self.insert_n_add_rows_at_top(section)
        result_upload_add_values = self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.ss_id,
            body={'valueInputOption': 'USER_ENTERED', 'data': self.per_section[section]['add data']}).execute()
        result_upload_add_color = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': self.per_section[section]['add color']}).execute()

        if self.record_log:
            self.log_add(section)

    def sort(self, section):
        self.init_per_section_dict()
        request = [{'sortRange': {'range': {'sheetId': self.sheet_id,
                                            'startRowIndex': self.per_section[section]['i_start'],
                                            'endRowIndex': self.per_section[section]['i_end'] + 1,
                                            'startColumnIndex': 0,
                                            'endColumnIndex': len(self.header)},
                                  'sortSpecs': [{'dimensionIndex': self.header.index(PartnerIO.col_camp), 'sortOrder': 'ASCENDING'},
                                                {'dimensionIndex': self.header.index(PartnerIO.col_pl), 'sortOrder': 'ASCENDING'}]}}]
        result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
                                                         body={'requests': request}).execute()

    def update_total(self):
        self.init_per_section_dict()

        str_total_row_n = str(int(self.i_total + 1))
        str_start_row_n = str(int(self.i_header + 3))
        str_end_row_n = str(int(self.i_total))

        data = []
        subtotal_cols = [PartnerIO.col_goal, PartnerIO.col_rev]
        if self.notlive_col:
            subtotal_cols.append(PartnerIO.col_notlive_rev)

        for col in subtotal_cols:
            col_letter = self.get_col_letter(col)
            data.append({'range': self.sheet_name + '!' + col_letter + str_total_row_n,
                         'majorDimension': 'ROWS',
                         'values': [['=SUM(' + col_letter + str_start_row_n + ':' + col_letter + str_end_row_n + ')']]})

        result = self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.ss_id,
                                                                  body={'valueInputOption': 'USER_ENTERED', 'data': data}).execute()

    # Log ###################################################
    def log_change(self, section):
        df = self.per_section[section]['change'].copy()

        diffs = []
        for col in PartnerIO.check_diff[section]:
            col_diff = df[df['update ' + col] == 1]
            if len(col_diff) < 1:
                continue
            for h_col in self.log_header:
                if h_col == 'Revised Date':
                    col_diff[h_col] = datetime.now().strftime('%m/%d/%Y')
                elif h_col == 'Revised Field':
                    col_diff[h_col] = col
                elif h_col in [PartnerIO.col_camp, PartnerIO.col_pl]:
                    continue
                elif h_col == 'To':
                    col_diff[h_col] = col_diff[col + '_new']
                elif h_col == 'From':
                    col_diff[h_col] = col_diff[col + '_old']
            diffs.append(col_diff)

        self.upload_log(pd.concat(diffs)[self.log_header].values.tolist())

    def log_add(self, section):
        df = self.per_section[section]['add'].copy()

        for col in self.log_header:
            if col == 'Revised Date':
                df[col] = datetime.now().strftime('%m/%d/%Y')
            elif col == 'Revised Field':
                df[col] = 'ADDED'
            elif col in [PartnerIO.col_camp, PartnerIO.col_pl]:
                continue
            else:
                df[col] = ''

        self.upload_log(df[self.log_header].values.tolist())

    def upload_log(self, values):
        n_rows = len(values)

        range = self.log_sheet_name + '!A2:'
        range += opx.utils.get_column_letter(len(self.log_header))
        range += str(2 + n_rows)

        self.insert_rows(self.log_sheet_id, 1, n_rows)

        values_result = self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.ss_id,
            body={'valueInputOption': 'USER_ENTERED',
                  'data': {'range': range, 'majorDimension': 'ROWS', 'values': values}}).execute()

        color_result_ = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': [{'updateCells': {'rows': [{'values': [{"userEnteredFormat": {'backgroundColor': PartnerIO.color_white}}] * len(self.log_header)}] * n_rows,
                                                'fields': 'userEnteredFormat.backgroundColor',
                                                'range': {'sheetId': self.log_sheet_id,
                                                          'startRowIndex': 1,
                                                          'endRowIndex': 1 + n_rows,
                                                          'startColumnIndex': 0,
                                                          'endColumnIndex': len(self.log_header)}}}]}).execute()

    # End Log ################################################

    def make_month(self, mo_year, base_on_sheet=None):
        self.prep_month(mo_year, base_on_sheet)
        self.update_month(mo_year, record_log=False, remove_disappear=True)
        self.reset_all_color()

    def prep_month(self, mo_year, base_on_sheet):
        # Copy a sheet. Rename it to new month.
        mo, year = mo_year
        sheet_name = PartnerIO.get_sheet_name(mo_year)

        if base_on_sheet is None:
            prev_mo = 12 if (mo == 1) else (mo - 1)
            prev_year = (year - 1) if (mo == 1) else year
            prev_sheet_name = PartnerIO.get_sheet_name((prev_mo, prev_year))
        else:
            prev_sheet_name = base_on_sheet

        gsheet_copy_sheet(prev_sheet_name, sheet_name, self.ss_id)

    def reset_all_color(self):
        self.init_per_section_dict()
        for section in self.per_section:
            if 'i_start' in self.per_section[section]:
                self.reset_color(section)

    def reset_color(self, section):
        i_start = self.per_section[section]['i_start']
        i_end = self.per_section[section]['i_end']
        i_col_dates = self.header.index(PartnerIO.col_dates)

        colors = []
        for i in range(i_start, i_end + 1):
            row_color = []
            for j in range(len(self.header)):
                color = PartnerIO.color_white
                if j == i_col_dates:
                    if self.values[i][j] != self.str_entire_month_dates:
                        color = PartnerIO.color_orange
                row_color.append({'userEnteredFormat': {'backgroundColor': color}})
            colors.append({'values': row_color})

        result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': [{'updateCells': {'rows': colors,
                                                'fields': 'userEnteredFormat.backgroundColor',
                                                'range': {'sheetId': self.sheet_id,
                                                          'startRowIndex': i_start,
                                                          'endRowIndex': i_end + 1,
                                                          'startColumnIndex': 0,
                                                          'endColumnIndex': len(self.header)}}}]}).execute()

    # Helpers ################################################
    def get_str_monthly_dates(self, start, end):
        monthly_start = max(start, self.month_start_date)
        monthly_end = min(end, self.month_end_date)
        dates = str(monthly_start.month) + '/' + str(monthly_start.day) + '/' + str(monthly_start.year)[-2:] + '-' \
                + str(monthly_end.month) + '/' + str(monthly_end.day) + '/' + str(monthly_end.year)[-2:]
        return dates

    def get_col_letter(self, col):
        return opx.utils.get_column_letter(self.header.index(col) + 1)

    def get_sheet_values(self, sheet_name):
        response = self.service.spreadsheets().values().get(spreadsheetId=self.ss_id, range=sheet_name,
                                                            majorDimension='ROWS', valueRenderOption='UNFORMATTED_VALUE').execute()
        return response['values']

    def insert_rows(self, sheet_id, i_start, n_rows):
        result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
            body={'requests': [{'insertDimension': {'range': {'sheetId': sheet_id,
                                                              'dimension': 'ROWS',
                                                              'startIndex': i_start,
                                                              'endIndex': i_start + n_rows},
                                                    'inheritFromBefore': False}}]}).execute()

    #############################################################

    #############################################################

    def update_booked(self):
        # Update naming sheet
        update_partner_io_naming(self.site, 'Booked')

        # What's booked
        col_rename_dict = {'Price Calculation Type': 'Type',
                           'Campaign Name': 'Internal Campaign Name',
                           'Base Rate': 'CPM/CPUV'}
        booked_up2date = get_booked_future_months(self.site).rename(columns=col_rename_dict)

        # What's in IO
        list_booked_on_sheet = self.get_sheet_values(PartnerIO.booked_sheet_name)
        booked_on_sheet = pd.DataFrame(list_booked_on_sheet[1:], columns=list_booked_on_sheet[0])

        # Date formatting
        booked_on_sheet['Month/Year'] = [(date(1900, 1, 1) + timedelta(days=int(my) - 2)) for my in booked_on_sheet['Month/Year']]
        booked_on_sheet['Month/Year'] = [(str(my.month) + '/' + str(my.year)) for my in booked_on_sheet['Month/Year']]

        # Add io naming columns
        naming = get_partner_io_naming(self.site, 'Booked').rename(columns={'Price Calculation Type': 'Type'})
        join_on = ['Type', 'Internal Campaign Name', 'Line Description']
        new_booked = pd.merge(booked_up2date, naming, how='left', on=join_on)

        # Add Added On date if new
        join_col = ['Type', 'Campaign Name', 'Placement', 'Month/Year']
        new_booked = pd.merge(new_booked, booked_on_sheet[['Added On'] + join_col], how='left', on=join_col)
        today_in_int = (datetime.now().date() - date(1900, 1, 1)).days + 2
        new_booked.loc[pd.isnull(new_booked['Added On']), 'Added On'] = today_in_int

        # Clean up
        col = ['Added On', 'Type', 'Campaign Name', 'Placement', 'Month/Year', 'Booked', 'CPM/CPUV']
        new_booked = new_booked[col]

        # Upload
        booked_sheet_id = gsheet_get_sheet_id_by_name(PartnerIO.booked_sheet_name, self.ss_id)
        result = self.service.spreadsheets().values().clear(spreadsheetId=self.ss_id, range=PartnerIO.booked_sheet_name,
                                                            body={}).execute()

        values = [new_booked.columns.tolist()] + new_booked.values.tolist()
        result = self.service.spreadsheets().values().update(spreadsheetId=self.ss_id, range=PartnerIO.booked_sheet_name,
                                                             valueInputOption='USER_ENTERED',
                                                             body={'values': values}).execute()
