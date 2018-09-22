from byme_helpers import *
from NEW_helpers import make_das
from class_partner_io import *

#############################################
run = {'drugs cpuv hit goal': False,  # Non-exclusive TS: Cialis, Mydayis, Otezla PsA (Metho), Taltz (Remicade)  # Paused already: Trintellix
       'update pas': True,

       'update partner io naming': False,
       'update partner ios': False,

       'new month partner ios': False,
       'compare partner goals for next month': False,
       'over 10% volume shift': False,

       'make das': False,

       'get ytd drugs revenue': False,
       'get 2016 site reports': False}

#############################################
if run['drugs cpuv hit goal']:
    path_drugs_cpuv = '//sfo-nas02/shared/departments/BA/Tim\'s Reports/UV reports/Drugs/Drugs_Microsite_UV_Tracker - February 2018 V2.xlsx'
    drugs_cpuv_hit_goal(path_drugs_cpuv)

if run['update pas']:
    update_pas(mo_year=(9, 2018), use_sheet='Sep')

if run['update partner io naming']:
    for site in ['Drugs', 'GoodRx']:  # 'GoodRx'
        update_partner_io_naming(site, mo_year=(9, 2018))

if run['update partner ios']:
    for site in ['Drugs', 'GoodRx']:
        PartnerIO.update(site, mo_year=(9, 2018), ith_disc_update=1)
    #PartnerIO.update_goodrx_booked()

#############################################
if run['new month partner ios']:
    for site in ['Drugs']:  # , 'GoodRx'
        PartnerIO.make(site=site, mo_year=(9, 2018))

if run['compare partner goals for next month']:
    before_sheet = 'Aug'
    now_sheet = 'Sep'
    csv_name = before_sheet + '2' + now_sheet + '_GoalChanges.csv'
    df_pas = compare_pas(year=2018, before_pas_sheet=before_sheet, now_pas_sheet=now_sheet)
    df_cpuv_goals = compare_cpuv_goals(year=2018, before_cpuv_goals_sheet=before_sheet, now_cpuv_goals_sheet=now_sheet)
    df = pd.concat([df_pas, df_cpuv_goals]).sort_values(['Campaign Name', 'Line Item Number', 'Line Description'])
    df.to_csv(csv_name, index=False, encoding='utf-8')

if run['over 10% volume shift']:
    before_pas_sheet = 'Nov'
    now_pas_sheet = 'Dec'
    csv_name = before_pas_sheet + '2' + now_pas_sheet + '_volume_shift.csv'
    df = vol_share_shift_over10p(year=2017, before_pas_sheet=before_pas_sheet, now_pas_sheet=now_pas_sheet)
    df.to_csv(csv_name, index=False, encoding='utf-8')

#############################################
if run['make das']:
    make_das(use_scheduled_units=False, export=True)

#############################################
if run['get ytd drugs revenue']:
    path_dict = {
        1: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/01-2018/FINAL_January_2018_Site_Report_03302018.xlsx',
        2: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/02-2018/FINAL_Febuary_2018_Site_Report_03152018.xlsx',
        3: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/03-2018/FINAL_March_2018_Site_Report_04232018.xlsx',
        4: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/04-2018/FINAL_April_2018_Site_Report_06272018.xlsx',
        5: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/05-2018/FINAL_May_2018_Site_Report_06272018.xlsx',
        6: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/06-2018/FINAL_June_2018_Site_Report_08242018.xlsx',
        7: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/07-2018/FINAL_July_2018_Site_Report_08162018.xlsx',
        8: '//sfo-nas02/shared/departments/AdOps/Ad Ops/Billing/2018 Billing/08-2018/FINAL_August_2018_Site_Report_09192018.xlsx'
    }

    combined_sr, drugs = get_ytd_drugs_revenue(path_dict)
    combined_sr.to_csv('YTD_site_report.csv', index=False)
    drugs.to_csv('YTD_drugs_rev.csv', index=False)

if run['get 2016 site reports']:
    path_dict = {
        1: 'L:/AdOps/Ad Ops/Billing/2016 Billing/01-2016/January_2016_Site_Report_02222016_reconciled to recorded.xlsx',
        2: 'L:/AdOps/Ad Ops/Billing/2016 Billing/02-2016/CHANGED_February_2016_Site_Report_04082016.xlsx',
        3: 'L:/AdOps/Ad Ops/Billing/2016 Billing/03-2016/REVISED_March_2016_Site_Report_05122016.xlsx',
        4: 'L:/AdOps/Ad Ops/Billing/2016 Billing/04-2016/UPDATED_April_2016_Site_Report_06242016.xlsx',
        5: 'L:/AdOps/Ad Ops/Billing/2016 Billing/05-2016/Final_May_2016_Site_Report_06302016.xlsx',
        6: 'L:/AdOps/Ad Ops/Billing/2016 Billing/06-2016/Final_June_2016_Site_Report_08082016.xlsx',
        7: 'L:/AdOps/Ad Ops/Billing/2016 Billing/07-2016/REVISED_v2_July_2016_Site_Report_11222016.xlsx',
        8: 'L:/AdOps/Ad Ops/Billing/2016 Billing/08-2016/FINAL_August_2016_Site_Report_09272016.xlsx',
        9: 'L:/AdOps/Ad Ops/Billing/2016 Billing/09-2016/FINAL_September_2016_Site_Report_10172016.xlsx',
        10: 'L:/AdOps/Ad Ops/Billing/2016 Billing/10-2016/FINAL_October_2016_Site_Report_12142016.xlsx',
        11: 'L:/AdOps/Ad Ops/Billing/2016 Billing/11-2016/Final_November_2016_Site_Report_12222016.xlsx',
        12: 'L:/AdOps/Ad Ops/Billing/2016 Billing/12-2016/Final_December_2016_Site_Report_01312017.xlsx'
    }

    combined_sr = get_ytd_drugs_revenue(path_dict, just_sr=True)
    combined_sr.to_csv('2016_Site_Reports.csv', index=False)