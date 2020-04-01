import pandas as pd
from pandas.tests.test_strings import assert_series_or_index_equal


def load_data():
    file_name = 'E:\python\data\excel/EML_AGING FINAL_20191105_EML-Test Input.xlsx'
    df = pd.read_excel(file_name, index_col=0)
    #print(df.count())

    df.drop_duplicates(inplace=True)
    #print(df.count())

    df = df[df['Patient ID'] == 12693]
    df = df.reset_index(drop=False)
    #print(df.count())
    print(df.to_string())

    column_list = list(df.columns.values.tolist())
    print(len(column_list))

    column_list.remove('Invoice Detail Balance')
    print(len(column_list))
    print(column_list)


    df_sum = df.groupby(['Patient ID', 'Invoice Number', 'Invoice Key', 'Invoice Date Opened'], as_index=False)['Invoice Detail Balance'].sum()
    print(df_sum.count())
    print(df_sum.to_string())

    df_new = df.drop(columns=['Invoice Detail Balance'])
    df_new.drop_duplicates(inplace=True)
    print(df_new.to_string())


def main():
    print('main')
    load_data()


if __name__ == '__main__':
    main()
