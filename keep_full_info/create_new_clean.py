import pandas as pd
import time
import random
import sys
import os

start = time.time()

def main():
    """ given the unique pids created in make_unique_pids,
        remove rows in the clean data which have the same pids,
        and save it... again :/"""

    clean_data = _setup_files()[0]
    unique_pids_data = _setup_files()[1]
    output_data = _setup_files()[2]

    clean_data = _make_df_from_data(clean_data)
    clean_data.to_csv('out_data/test_from_df.csv')
    unique_pids_data = _make_df_from_data(unique_pids_data)
    # Because I don't know how to do df compare, use a list :/...
    unique_pids_list = unique_pids_data['pidp'].values.tolist()

    for i, row in enumerate(clean_data.itertuples()):
        if(row[1] in unique_pids_list):
            clean_data.drop(i, inplace=True)

    clean_data['start'] = clean_data['start'].astype(str)
    clean_data['end'] = clean_data['end'].astype(str)
    #clean_data['div'] = clean_data['div'].astype(str)
    #clean_data['wid'] = clean_data['wid'].astype(str)
    clean_data.to_stata(output_data)
    clean_data.to_csv('out_data/test.csv')

def _make_df_from_data(data):
    reader = pd.read_stata(data, chunksize=100000, convert_categoricals=False)
    df = pd.DataFrame()
    for itm in reader:
        df = df.append(itm)
    return df

def _setup_files():
    files = []
    try:
        files.append(sys.argv[1])
    except Exception as e:
        print('You need to add your clean data file')
        print("""Your command should look like this:
        python keep_full_info/create_new_clean.py source_data/clean.dta source_data/unique_pids.dta out_data/the_output.dta
        """)
        sys.exit(2)
    try:
        files.append(sys.argv[2])
    except Exception as e:
        print('You need to add your unique_pids data file')
        print("""Your command should look like this:
        python keep_full_info/create_new_clean.py source_data/clean.dta source_data/unique_pids.dta out_data/the_output.dta
        """)
        sys.exit(2)

    try:
        files.append(sys.argv[3])
    except Exception as e:
        print('You need to add your target output data file')
        print("""Your command should look like this:
        python keep_full_info/create_new_clean.py source_data/clean.dta out_data/unique_pids.dta out_data/the_output.dta
        """)
        sys.exit(2)


    return files


if __name__ == '__main__':
    main()
