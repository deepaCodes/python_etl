import datetime
import itertools
import multiprocessing
import os
import glob
from datetime import datetime
from string import ascii_uppercase

import pandas as pd

#CLIENT_NAME = '0-30=010E01, 31-60=010E02, 61-90=010PR1, 91-120=010LT1, 121-210=010LT2, 211-300=010LT2A, 301-360=010LT2B, 361-9999=010LT3'
date_format = '%Y-%m-%d %H:%M:%S'

dt_today = pd.to_datetime('today')
id_column = 'Patient ID'
FILTER_BY_ID = False
FILTER_ID = 12163


def aging_bucket(row):
    """
    :param row:
    :return:
    """
    age = abs((datetime.now() - datetime.strptime(str(row['B']), date_format)).days)
    if age in range(0, 31):
        return '0-30'
    if age in range(31, 61):
        return '31-60'
    if age in range(61, 91):
        return '61-90'
    if age in range(91, 121):
        return '91-120'
    if age in range(121, 361):
        return '121-360'

    return '361-9999'


def get_client_name(row):
    """
    :param row:
    :return:
    """
    age = abs((datetime.now() - datetime.strptime(str(row['B']), date_format)).days)

    if age in range(0, 31):
        return '010E01'
    if age in range(31, 61):
        return '010E02'
    if age in range(61, 91):
        return '010PR1'
    if age in range(91, 121):
        return '010LT1'
    if age in range(121, 210):
        return '010LT2'
    if age in range(211, 301):
        return '010LT2A'
    if age in range(301, 361):
        return '010LT2B'

    return '010LT3'


def collection_status(row):
    """
    :param row:
    :return:
    """
    days = abs((datetime.now() - datetime.strptime(str(row['B']), date_format)).days)
    # BASED ON AGE, 0-30=STATEMENT 1, 31-60=STATEMENT 2, 61+=LETTER 26
    if days in range(0, 31):
        return 'STATEMENT 1'

    if days in range(31, 61):
        return 'STATEMENT 2'

    if days in range(61, 10000):
        return 'LETTER 26'

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

    df_output['icd10claimdiagdescr01'] = df.apply(lambda row: '{}{}'.format(row['R'], row['S']), axis=1, result_type='expand')
    df_output['icd10claimdiagdescr02'] = df.apply(lambda row: '{} {}'.format(row['T'], row['U']), axis=1, result_type='expand')
    df_output['icd10claimdiagdescr03'] = df.apply(lambda row: '{} {}'.format(row['V'], row['W']), axis=1, result_type='expand')
    df_output['svc dept bill name'] = df['C']
    df_output['patient address'] = df.apply(lambda row: '{} {} {} {} {} '.format(row['H'], row['I'], row['J'], row['K'], row['L']), axis=1,result_type='expand')
    df_output['patient address1'] = df['H']
    df_output['patient address2'] = df['I']
    df_output['patient city'] = df['J']
    df_output['patient state'] = df['K']
    df_output['patient zip'] = df['L']
    df_output['patientdob'] = df['P'].dt.strftime('%m/%d/%Y')
    df_output['patient firstname'] = df['F']
    df_output['patient lastname'] = df['E']
    df_output['guarantor addr'] = df['AT']
    df_output['guarantor addr2'] = df['AU']
    df_output['guarantor city'] = df['AV']
    df_output['guarantor email'] = df['BA']
    df_output['guarantor frstnm'] = df['AS']
    df_output['guarantor lastnm'] = df['AR']
    df_output['guarantor phone'] = df['AY']
    df_output['ptnt grntr rltnshp'] = df['AQ']
    df_output['guarantor state'] = df['AW']
    df_output['guarantor zip'] = df['AX']
    df_output['patient homephone'] = df['M']
    df_output['patientid'] = df['O']
    df_output['patient middleinitial'] = df['G']
    df_output['patient mobile no'] = df['N']
    #df_output['proccode-descr'] = df.apply(lambda row: '{}-{}'.format(row['AJ'], row['AB']), axis=1, result_type='expand')
    df_output['guarantor ssn'] = df.apply(lambda row: '{}{}{}'.format(row['O'], '0', '110'), axis=1, result_type='expand')
    df_output['patient ssn'] = df.apply(lambda row: '{}{}{}'.format(row['O'], '0', '010'), axis=1, result_type='expand')
    df_output['Ordering Physician'] = df['AL']
    df_output['invid'] = df['A']
    df_output['postdate'] = df['B'].dt.strftime('%m/%d/%Y')
    df_output['srvdate'] = df['AC'].dt.strftime('%m/%d/%Y')
    df_output['Invoice Detail Balance'] = df['AI']
    df_output['Discount Threshold'] = '30% W/O Mgt Approval'
    df_output['Client Billing System'] = 'Brightree'
    # add if else
    df_output['Collection Status'] = df.apply(lambda row: collection_status(row), axis=1, result_type='expand')
    df_output['Client Billing System User/Pass'] = 'See Management'
    df_output['Accepted Payment Forms'] = 'See Management'
    df_output['Financial Class'] = 'Credit, Debit, e-Check, Mail-In'
    df_output['Client Billing System URL'] = 'https://login.brightree.net/'
    df_output['Responsibility Date'] = df['B'].dt.strftime('%m/%d/%Y')
    df_output['Client Name'] = df.apply(lambda row: get_client_name(row), axis=1, result_type='expand')
    df_output['3rd Party Correspondence'] = 'Innovare-Virtual Post Mail'
    df_output['Script'] = '3rd Party Collections'
    df_output['Early Out Correspondence'] = 'Managed By Client'
    df_output['Client Payment Mailing Address'] = '340 S Lemon Ave #1102 Walnut, CA 91789'
    df_output['Client Payment System'] = 'Repay'
    df_output['Callback Number'] = '4052001666'
    df_output['Minimum Payment'] = '$10'
    df_output['Internal Account Status'] = df.apply(lambda row: internal_account_status(row), axis=1, result_type='expand')
    df_output['Client Phone'] = '4052001666'
    df_output['Billing Provider'] = df['C']
    df_output['Claim Received Date'] = dt_today.strftime('%m/%d/%Y')
    df_output['Client Billing Contact'] = 'Sinthya Cruz-Billing Manager'
    df_output['Client Payment System URL'] = 'https://innovareprm.repay.io'
    df_output['Client Website'] = 'N/A'
    df_output['Aging Bucket'] = df.apply(lambda row: aging_bucket(row), axis=1, result_type='expand')
    df_output['Customer Service Email'] = 'support@innovareprm.com'
    df_output['Specialty'] = 'Sleep Medicine and Supplies'
    df_output['Custom Account Number'] = df.apply(
        lambda row: '{}-{}-{}'.format(row['O'], datetime.strftime(row['AC'], "%m.%d.%Y"), 'EML'), axis=1,result_type='expand')
    df_output['charge off date'] = df['B'].dt.strftime('%m/%d/%Y')
    df_output['originated date'] = df['AC'].dt.strftime('%m/%d/%Y')
    df_output['patient address'] = df.apply(
        lambda row: '{} {} {} {} {} '.format(row['H'], row['I'], row['J'], row['K'], row['L']), axis=1,result_type='expand')
    df_output['Phone Number1'] = df['M'].astype(str)
    df_output['Phone Number2'] = df['N'].astype(str)
    df_output['Phone Number3'] = df['AY'].astype(str)
    df_output['Phone Number4'] = df['AZ'].astype(str)
    df_output['creditor'] = 'Echelon Medical'
    # Action Code  NON-PAYMENT ROWS=INFO ACCOUNT, PAYMENT ROWS=CORRESPONDENCE ACCOUNT ??
    df_output['Action Code'] = 'INFO ACCOUNT'

    df_output.drop_duplicates(inplace=True)

    columns = list(df_output.columns.values.tolist())
    #print(columns)

    columns.remove('Invoice Detail Balance')

    #print('Calculating aggreate of sum')
    #columns = ['icd10claimdiagdescr01', 'icd10claimdiagdescr02', 'icd10claimdiagdescr03', 'svc dept bill name', 'patient address', 'patient address1', 'patient address2', 'patient city', 'patient state', 'patient zip', 'patientdob', 'patient firstname', 'patient lastname', 'guarantor addr', 'guarantor addr2', 'guarantor city', 'guarantor email', 'guarantor frstnm', 'guarantor lastnm', 'guarantor phone', 'ptnt grntr rltnshp', 'guarantor state', 'guarantor zip', 'patient homephone', 'patientid', 'patient middleinitial', 'patient mobile no', 'guarantor ssn', 'patient ssn', 'Ordering Physician', 'invid', 'postdate', 'srvdate', 'Discount Threshold', 'Client Billing System', 'Collection Status', 'Client Billing System User/Pass', 'Accepted Payment Forms', 'Financial Class', 'Client Billing System URL', 'Responsibility Date', 'Client Name', '3rd Party Correspondence', 'Script', 'Early Out Correspondence', 'Client Payment Mailing Address', 'Client Payment System', 'Callback Number', 'Minimum Payment', 'Internal Account Status', 'Client Phone', 'Billing Provider', 'Claim Received Date', 'Client Billing Contact', 'Client Payment System URL', 'Client Website', 'Aging Bucket', 'Customer Service Email', 'Specialty', 'Custom Account Number', 'charge off date', 'originated date', 'Phone Number1', 'Phone Number2', 'Phone Number3', 'Phone Number4', 'creditor']
    #['invid', 'postdate', 'srvdate']

    df_dos = df_output.groupby(columns, as_index=False)['Invoice Detail Balance'].sum()
    #print(df_dos.count())
    #print(df_dos.to_string())

    # original claim amount (DOS Rows) SUM OF BALANCES PER DATE OF SERVICE PER PATIENT ID#???
    # Balance (DOS Rows) SUM OF BALANCES PER DATE OF SERVICE PER PATIENT ID#??
    # 10% discount	15% discount	20% discount	25% discount	30% discount ??
    # original claim amount (Totals Row)	Balance (Totals Row)
    df_dos['original claim amount (DOS Rows)'] = float(0)
    df_dos['Balance (DOS Rows)'] = float(0)
    df_dos['original claim amount (Totals Row)'] = float(0)
    df_dos['Balance (Totals Row)'] = 0
    df_dos['10% discount'] = float(0)
    df_dos['15% discount'] = float(0)
    df_dos['20% discount'] = float(0)
    df_dos['25% discount'] = float(0)
    df_dos['30% discount'] = float(0)

    df_total = df_output.groupby(['patient ssn'], as_index=False)['Invoice Detail Balance'].sum()
    #print(df_total.count())
    #print(df_total.to_string())

    pd_series = []
    for index, row in df_total.iterrows():
        dos_row = df_dos[df_dos['patient ssn'] == row['patient ssn']].tail(1)
        dos_row['original claim amount (Totals Row)'] = row['Invoice Detail Balance']
        dos_row['Balance (Totals Row)'] = row['Invoice Detail Balance']
        dos_row['Action Code'] = 'CORRESPONDENCE ACCOUNT'
        #print(dos_row.to_string())
        pd_series.append(dos_row)

    df_dos = df_dos.append(pd.concat(pd_series, axis=1), ignore_index=True)
    #df_dos.rename(columns={'Invoice Detail Balance': 'original claim amount (DOS Rows)'}, inplace=True)
    df_dos['original claim amount (DOS Rows)'] = df_dos['Invoice Detail Balance']
    df_dos['Balance (DOS Rows)'] = df_dos['original claim amount (DOS Rows)']

    df_dos.drop(['Invoice Detail Balance'], axis=1, inplace=True)

    phone_list = []
    for index, row in df_dos.iterrows():
        if row['Action Code'] == 'CORRESPONDENCE ACCOUNT':
            df_dos.at[index, 'original claim amount (DOS Rows)'] = 0
            df_dos.at[index, 'Balance (DOS Rows)'] = 0
            amount = row['original claim amount (Totals Row)']
        else:
            amount = row['original claim amount (DOS Rows)']

        df_dos.at[index, '10% discount'] = amount * 0.90
        df_dos.at[index, '15% discount'] = amount * 0.85
        df_dos.at[index, '20% discount'] = amount * 0.80
        df_dos.at[index, '25% discount'] = amount * 0.75
        df_dos.at[index, '30% discount'] = amount * 0.70

        df_dos['Invoice Detail Charge'] = ''
        df_dos['Invoice Detail Allow'] = ''
        df_dos['Invoice Detail Payments'] = ''
        df_dos['Invoice Detail Adjustments'] = ''
        df_dos['Invoice Detail Balance'] = ''

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

    alpha_list = list(itertools.islice(continuous_alphabets(), len(column_list)))
    #print(len(alpha_list))
    # create a dictionary mapping alpha_name with column_name
    columns_dict = dict(zip(column_list, alpha_list))
    id_column_new = columns_dict.get(id_column)
    #print('new id column: {}'.format(id_column_new))

    # print(columns_dict)
    df.rename(columns=columns_dict, inplace=True)

    if FILTER_BY_ID:
        df = df[df[id_column_new] == FILTER_ID]
    df = df.reset_index(drop=True)

    id_list = df[id_column_new].unique()
    #print('Total unique {}: {}'.format(id_column, len(id_list)))

    #print('Data transform start time: {}'.format(datetime.now()))

    df_list = []
    for id in id_list:
        #print('aggregating {}: {}'.format(id_column, id))
        df_list.append(df[df[id_column_new] == id])

    cpu_count = multiprocessing.cpu_count()
    #print('cpu_count: {}'.format(cpu_count))

    df_result = []
    with multiprocessing.Pool(cpu_count-1) as pool:
        df_result.append(pool.map(transform_data_by_id, df_list))

    df_output = pd.DataFrame()
    for df_tmp in df_result:
        df_output = df_output.append(df_tmp)

    #print(df_output.count())

    #print('Data transform end time: {}'.format(datetime.now()))

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
