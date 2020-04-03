import datetime
import itertools
import multiprocessing
import os
import glob
from datetime import datetime
from string import ascii_uppercase

import pandas as pd

#CLIENT_NAME = '0-30=010E01, 31-60=010E02, 61-90=010PR1, 91-120=010LT1, 121-210=010LT2, 211-300=010LT2A, 301-360=010LT2B, 361-9999=010LT3'
client_prefix = '013'
date_format = '%Y-%m-%d %H:%M:%S'

dt_today = pd.to_datetime('today')
id_column = 'patientid'
FILTER_BY_ID = False
FILTER_ID = 1197

def aging_bucket(row):
    """
    BASED ON AGE BETWEEN POST DATE AND CURRENT DATE OUTPUT 0-30, 31-60, 61-90, 91-120, 121-180, 181-360, 361-9999
    :param row:
    :return:
    """
    age = abs((datetime.now() - datetime.strptime(str(row['postdate']), date_format)).days)
    if age in range(0, 31):
        return '0-30'
    if age in range(31, 61):
        return '31-60'
    if age in range(61, 91):
        return '61-90'
    if age in range(91, 121):
        return '91-120'
    if age in range(121, 181):
        return '121-180'
    if age in range(181, 361):
        return '181-360'

    return '361-9999'


def get_client_name(row):
    """
    DOS ROWS OUPUT 016AR1, TOTALS OWS OUTPUT 016COR
    :param row:
    :return:
    """

    if row['Action Code'] == 'CORRESPONDENCE ACCOUNT':
        return '016COR'

    return '016AR1'

def collection_status(row):
    """
    :param row:
    :return:
    """
    days = abs((datetime.now() - datetime.strptime(str(row['postdate']), date_format)).days)
    # BASED ON AGE, 0-30=STATEMENT 1, 31-60=STATEMENT 2, 61+=LETTER 26
    if days in range(0, 31):
        return 'STATEMENT 1'

    if days in range(31, 61):
        return 'STATEMENT 2'

    if days in range(61, 10000):
        return 'ACCOUNT REVIEW 1'

    return None


def internal_account_status(row):
    """
    :param row:
    :return:
    """
    age = abs((datetime.now() - datetime.strptime(str(row['B']), date_format)).days)

    if age in range(0, 61):
        return 'Statement'
    return 'Account Review'


def continuous_alphabets():
    """
    :return:
    """
    for size in itertools.count(1):
        for s in itertools.product(ascii_uppercase, repeat=size):
            yield "".join(s)


def transform_data_by_id(df):
    """
    :param df:
    :return:
    """
    #print('in data transformation')
    #print(df.to_string())
    df_output = pd.DataFrame()

    df_output['icd10claimdiagdescr01'] = df['icd10claimdiagdescr01']
    df_output['icd10claimdiagdescr02'] =  df['icd10claimdiagdescr02']
    df_output['icd10claimdiagdescr03'] = df['icd10claimdiagdescr03']
    df_output['svc dept bill name'] = df['svc dept bill name']
    df_output['patient address'] = df.apply(lambda row: '{} {} {} {} {} '.format(row['patient address1'], row['patient address2'], row['patient city'], row['patient state'], row['patient zip']), axis=1,result_type='expand')
    df_output['patient address1'] = df['patient address1']
    df_output['patient address2'] = df['patient address2']
    df_output['patient city'] = df['patient city']
    df_output['patientdob'] = df['patientdob'].dt.strftime('%m/%d/%Y')
    df_output['patient email'] = df['patient email']
    df_output['ptnt emrgncy cntct name'] = df['ptnt emrgncy cntct name']
    df_output['ptnt emrgncy cntct ph'] = df['ptnt emrgncy cntct ph']
    df_output['ptnt emrgncy cntct rltnshp'] = df['ptnt emrgncy cntct rltnshp']
    df_output['firstapptdate'] =df['firstapptdate'].dt.strftime('%m/%d/%Y')
    df_output['patient firstname'] = df['patient firstname']
    df_output['guarantor addr'] = df['guarantor addr']
    df_output['guarantor addr2'] = df['guarantor addr2']
    df_output['patientguarantordob'] = df['patientguarantordob']
    df_output['guarantor city'] = df['guarantor city']
    df_output['guarantor email'] = df['guarantor email']
    df_output['guarantor frstnm'] = df['guarantor frstnm']
    df_output['guarantor lastnm'] = df['guarantor lastnm']
    df_output['guarantor middle initial'] = df['guarantor middle initial']
    df_output['guarantor phone'] = df['guarantor phone']
    df_output['ptnt grntr rltnshp'] = df['ptnt grntr rltnshp']
    df_output['guarantor state'] = df['guarantor state']
    df_output['guarantor zip'] = df['guarantor zip']
    df_output['patient homephone'] = df['patient homephone']
    df_output['patientid'] = df['patientid']
    df_output['patient lastname'] = df['patient lastname']
    df_output['patientlastseend'] = df['patientlastseend'].dt.strftime('%m/%d/%Y')
    df_output['patient middleinitial'] = df['patient middleinitial']
    df_output['patient mobile no'] = df['patient mobile no']
    df_output['patientnextappt'] = df['patientnextappt']
    df_output['ptnt nxtkin name'] = df['ptnt nxtkin name']
    df_output['ptnt nxtkin ph'] = df['ptnt nxtkin ph']
    df_output['ptnt nxtkin rltn'] = df['ptnt nxtkin rltn']
    df_output['patientsex'] = df['patientsex']
    df_output['patient state'] = df['patient state']
    df_output['patient workphone'] = df['patient workphone']
    df_output['proccode-descr'] = df['proccode-descr']
    df_output['guarantor ssn'] = df.apply(lambda row: '{}{}{}'.format(row['patientid'], '00', '116'), axis=1, result_type='expand')
    df_output['patient ssn'] = df.apply(lambda row: '{}{}{}'.format(row['patientid'], '00', '016'), axis=1, result_type='expand')
    df_output['rndrng prvdrfullnme'] = df['rndrng prvdrfullnme']
    df_output['sup prvdrfullnme'] = df['sup prvdrfullnme']
    df_output['custom trans code'] = df['custom trans code']
    df_output['invid'] = df['invid']
    df_output['postdate'] = df['postdate'].dt.strftime('%m/%d/%Y')
    df_output['transreasonid'] = df['transreasonid']
    df_output['srvdate'] = df['srvdate'].dt.strftime('%m/%d/%Y')
    df_output['employer phone'] = df['employer phone']
    df_output['patient zip'] = df['patient zip']
    df_output['claimid'] = df['claimid']
    df_output['Discount Threshold'] = '10% W/O Mgt Approval'
    df_output['Client Billing System'] = 'Athena Health'
    df_output['Collection Status'] = 'LETTER 26'
    df_output['Client Billing System User/Pass'] = 'See Management'
    df_output['Accepted Payment Forms'] = 'Credit, Debit, e-Check, Mail-In'
    df_output['Financial Class'] = 'Patient Responsibility'
    df_output['Client Billing System URL'] = 'https://athenanet.athenahealth.com/1/55/login.esp'
    df_output['Next Collection Action'] = 'Legal'
    df_output['Responsibility Date'] = df['postdate'].dt.strftime('%m/%d/%Y')
    df_output['Client Name'] = '016AR1'
    df_output['3rd Party Correspondence'] = 'Innovare-Virtual Post Mail'
    df_output['Script'] = df.apply(lambda row: collection_status(row), axis=1, result_type='expand')
    df_output['Early Out Correspondence'] = 'Managed By Client'
    df_output['Client Payment Mailing Address'] = '340 S Lemon Ave #1102, Walnut, CA 91789'
    df_output['Client Payment System'] = 'Repay'
    df_output['Callback Number'] = '479-337-7339'
    df_output['Minimum Payment'] = '$10.00'
    df_output['Internal Account Status'] = df.apply(lambda row: collection_status(row), axis=1, result_type='expand')

    df_output['Client Phone'] = '479-337-7339'
    df_output['Billing Provider'] = df['svc dept bill name']
    df_output['Claim Received Date'] = df['postdate'].dt.strftime('%m/%d/%Y')
    df_output['Client Billing Contact'] = 'Erin Burger - Biling Supervisor'
    df_output['Client Payment System URL'] = 'https://innovareprm.repay.io'
    df_output['Client Website'] = 'https://voldvision.com/fayetteville-office'

    df_output['Aging Bucket'] = df.apply(lambda row: aging_bucket(row), axis=1, result_type='expand')
    df_output['Customer Service Email'] = 'support@innovareprm.com'
    df_output['Specialty'] = 'Ophthalmology'

    # CREATE CUSTOM NUMBER BEGINNING WITH PATID THEN "-" SERVDATE (XX.XX.XXXX) "-" VVP
    df_output['Custom Account Number'] = df.apply(
        lambda row: '{}-{}-{}'.format(row['patientid'], datetime.strftime(row['srvdate'], "%m.%d.%Y"), 'VVP'), axis=1,result_type='expand')

    # CREATE 9 DIGIT NUMBER BEGINNING WITH PATID (COLUMN AD) AND ENDING WITH 116 AND FILL IN BLANKS WITH 0'S
    df_output['gaurantor id'] = df.apply(lambda row: '{}{}{}'.format(row['patientid'], '00', '116'), axis=1, result_type='expand')
    df_output['charge off date'] = df['postdate'].dt.strftime('%m/%d/%Y')
    df_output['originated date'] = df['srvdate'].dt.strftime('%m/%d/%Y')
    df_output['patient address'] = df.apply(
        lambda row: '{} {} {} {} {} '.format(row['patient address1'], row['patient address2'],
                                             row['patient city'], row['patient state'], row['patient zip']),
        axis=1, result_type='expand')

    df_output['Phone Number1'] = df['patient mobile no'].astype(str)
    df_output['Phone Number2'] = df['patient homephone'].astype(str)
    df_output['Phone Number3'] = df['patient workphone'].astype(str)
    df_output['Phone Number4'] = df['ptnt emrgncy cntct ph'].astype(str)
    df_output['creditor'] = 'Vold Vision'
    df_output['Action Code'] = 'INFO ACCOUNT'

    # baddebt	collect	recovery
    df_output['SUM_OF_3_COL'] = df.apply(lambda row: (row['baddebt'] + row['collect'] + row['recovery']), axis=1, result_type='expand')

    df_output.drop_duplicates(inplace=True)

    columns = list(df_output.columns.values.tolist())
    #print(columns)

    sum_column = 'SUM_OF_3_COL'
    columns.remove(sum_column)

    #print('Calculating aggreate of sum')
    #columns = ['icd10claimdiagdescr01', 'icd10claimdiagdescr02', 'icd10claimdiagdescr03', 'svc dept bill name', 'patient address', 'patient address1', 'patient address2', 'patient city', 'patient state', 'patient zip', 'patientdob', 'patient firstname', 'patient lastname', 'guarantor addr', 'guarantor addr2', 'guarantor city', 'guarantor email', 'guarantor frstnm', 'guarantor lastnm', 'guarantor phone', 'ptnt grntr rltnshp', 'guarantor state', 'guarantor zip', 'patient homephone', 'patientid', 'patient middleinitial', 'patient mobile no', 'guarantor ssn', 'patient ssn', 'Ordering Physician', 'invid', 'postdate', 'srvdate', 'Discount Threshold', 'Client Billing System', 'Collection Status', 'Client Billing System User/Pass', 'Accepted Payment Forms', 'Financial Class', 'Client Billing System URL', 'Responsibility Date', 'Client Name', '3rd Party Correspondence', 'Script', 'Early Out Correspondence', 'Client Payment Mailing Address', 'Client Payment System', 'Callback Number', 'Minimum Payment', 'Internal Account Status', 'Client Phone', 'Billing Provider', 'Claim Received Date', 'Client Billing Contact', 'Client Payment System URL', 'Client Website', 'Aging Bucket', 'Customer Service Email', 'Specialty', 'Custom Account Number', 'charge off date', 'originated date', 'Phone Number1', 'Phone Number2', 'Phone Number3', 'Phone Number4', 'creditor']
    #['invid', 'postdate', 'srvdate']

    df_dos = df_output.groupby(columns, as_index=False)[sum_column].sum()
    #print(df_dos.count())
    #print(df_dos.to_string())

    df_dos['original claim amount (DOS Rows)'] = float(0)
    df_dos['Balance (DOS Rows)'] = float(0)
    df_dos['original claim amount (Totals Row)'] = float(0)
    df_dos['Balance (Totals Row)'] = 0
    df_dos['10% discount'] = float(0)
    df_dos['15% discount'] = float(0)
    df_dos['20% discount'] = float(0)
    df_dos['25% discount'] = float(0)
    df_dos['30% discount'] = float(0)
    df_dos['35% discount'] = float(0)
    df_dos['40% discount'] = float(0)
    df_dos['45% discount'] = float(0)
    df_dos['50% discount'] = float(0)


    df_total = df_output.groupby([id_column], as_index=False)[sum_column].sum()
    #print(df_total.count())
    #print(df_total.to_string())

    pd_series = []
    for index, row in df_total.iterrows():
        dos_row = df_dos[df_dos[id_column] == row[id_column]].tail(1)
        dos_row['original claim amount (Totals Rows)'] = row[sum_column]
        dos_row['Balance (Totals Rows)'] = row[sum_column]
        dos_row['Action Code'] = 'CORRESPONDENCE ACCOUNT'
        pd_series.append(dos_row)

    df_dos = df_dos.append(pd.concat(pd_series, axis=1), ignore_index=True)

    df_dos['original claim amount (DOS Rows)'] = df_dos[sum_column]
    df_dos['Balance (DOS Rows)'] = df_dos[sum_column]

    df_dos.drop([sum_column], axis=1, inplace=True)

    phone_list = []
    for index, row in df_dos.iterrows():
        if row['Action Code'] == 'CORRESPONDENCE ACCOUNT':
            df_dos.at[index, 'original claim amount (DOS Rows)'] = 0
            df_dos.at[index, 'Balance (DOS Rows)'] = 0
            df_dos.at[index, 'Client Name'] = '016COR'
            amount = row['original claim amount (Totals Row)']
        else:
            amount = row['original claim amount (DOS Rows)']

        df_dos.at[index, '10% discount'] = amount * 0.90
        df_dos.at[index, '15% discount'] = amount * 0.85
        df_dos.at[index, '20% discount'] = amount * 0.80
        df_dos.at[index, '25% discount'] = amount * 0.75
        df_dos.at[index, '30% discount'] = amount * 0.70
        df_dos.at[index, '35% discount'] = amount * 0.65
        df_dos.at[index, '40% discount'] = amount * 0.60
        df_dos.at[index, '45% discount'] = amount * 0.55
        df_dos.at[index, '50% discount'] = amount * 0.50


        # phone numbers - Phone Number1 Phone Number2  Phone Number3 Phone Number4
        phone_set = set()
        for i in range(1, 5):
            _value = row['Phone Number{}'.format(i)]
            if not _value or str(_value).strip() == '':
                continue
            phone_set.add(_value)
            df_dos.at[index, 'Phone Number{}'.format(i)] = ''

        #print('phone_set: {}'.format(phone_set))

        for i, val in enumerate(phone_set):
            if val not in phone_list:
                df_dos.at[index, 'Phone Number{}'.format(i+1)] = str(val)

        phone_list.extend(list(phone_set))

    #print(df_dos.count())
    #print(df_dos.to_string())

    return df_dos



def load_excel_file(excel_file):
    """
    :param excel_file:
    :return:
    """

    #print('in load file')
    xl = pd.ExcelFile(excel_file)
    sheet_name = xl.sheet_names[0]
    #print('sheet name {}'.format(xl.sheet_names))
    df = pd.read_excel(excel_file, sheet_name=sheet_name, keep_default_na=False)
    #print(df.count())

    return df, sheet_name
    # return df[:10], sheet_name


def transform_data(df):
    """
    :param df:
    :return:
    """

    df.drop_duplicates(inplace=True)

    column_list = df.columns.values.tolist()
    #print(len(column_list))
    #print(df.head().to_string())

    if FILTER_BY_ID:
        df = df[df[id_column] == FILTER_ID]
    df = df.reset_index(drop=True)

    id_list = df[id_column].unique()

    df_list = []
    for id in id_list:
        df_list.append(df[df[id_column] == id])

    cpu_count = multiprocessing.cpu_count()
    df_result = []
    with multiprocessing.Pool(cpu_count-1) as pool:
        df_result.append(pool.map(transform_data_by_id, df_list))

    df_output = pd.DataFrame()
    for df_tmp in df_result:
        df_output = df_output.append(df_tmp)


    return df_output


def write_output_to_file(df, sheet_name, output_file):
    """
    :param df:
    :param sheet_name:
    :param output_file:
    :return:
    """

    #print('Data transforamtion completed. Writing to file :{}'.format(output_file))

    writer = pd.ExcelWriter(output_file)
    df.to_excel(writer, sheet_name, index=False)
    writer.save()

    #print('Data transformed to :{}'.format(output_file))



def main(input_excel_file, output_excel_file):
    """

    :param input_args:
    :return:
    """

    #print('Loading data from input_excel_file: {}'.format(input_excel_file))

    # call function to read excel file
    df, sheet_name = load_excel_file(input_excel_file)
    #print(df.head().to_string())

    # transform data
    df = transform_data(df)
    write_output_to_file(df, sheet_name, output_excel_file)

    #print('Data written to: {}'.format(output_excel_file))

if __name__ == '__main__':
    #print(input_args)
    files = glob.glob("./*.xlsx")
    input_excel_file = files[0]
    output_excel_file = '{}-Output.xlsx'.format(os.path.splitext(files[0])[0])
    print('Input file: {}'.format(input_excel_file))
    main(input_excel_file, output_excel_file)
