import pandas as pd
import time
import random
import sys
import os

start = time.time()

def unique_pids():
    """ given a raw data set, find out what pids in raw do not
        exist in the clean data set and save the result as a dta"""
    raw_data = _setup_files()[0]
    clean_data = _setup_files()[1]
    output_data = _setup_files()[2]

    raw_df = _make_df_from_data(raw_data)
    raw_pids = []
    for i, row in enumerate(raw_df.itertuples()):  # enumeration means the row begins 0
        raw_pids.append(row[1])

    clean_df = _make_df_from_data(clean_data)
    clean_pids = []
    for i, row in enumerate(clean_df.itertuples()):  # enumeration means the row begins 0
        clean_pids.append(row[1])

    interesting_pids = [i for i in raw_pids if i not in clean_pids]
    # make unique
    the_pids = list(set(interesting_pids))
    df = pd.DataFrame({'pidp':the_pids})
    df.to_stata(output_data)
    df.to_csv('out_data/test.csv')

def _make_df_from_data(data):
    reader = pd.read_stata(data, chunksize=100000)
    df = pd.DataFrame()
    for itm in reader:
        df = df.append(itm)
    return df

def _setup_files():
    files = []
    try:
        files.append(sys.argv[1])
    except Exception as e:
        print('You need to add your raw data file')
        print("""Your command should look like this:
        python keep_full_info/make_unique_pids.py source_data/raw.dta source_data/clean.dta out_data/the_output.dta
        """)
        sys.exit(2)
    try:
        files.append(sys.argv[2])
    except Exception as e:
        print('You need to add your clear data file')
        print("""Your command should look like this:
        python keep_full_info/make_unique_pids.py source_data/raw.dta source_data/clean.dta out_data/the_output.dta
        """)
        sys.exit(2)

    try:
        files.append(sys.argv[3])
    except Exception as e:
        print('You need to add your output data file')
        print("""Your command should look like this:
        python keep_full_info/make_unique_pids.py source_data/raw.dta source_data/clean.dta out_data/the_output.dta
        """)
        sys.exit(2)


    return files


if __name__ == '__main__':
    unique_pids()
