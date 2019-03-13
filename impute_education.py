import pandas as pd
import time
import random
import utils
import sys
import os
import pickle
import gc
import math


start = time.time()

def load_large_dta(fname):

    reader = pd.read_stata(fname, iterator=True)
    df = pd.DataFrame()
    try:
        chunk = reader.get_chunk(100*1000)
        while len(chunk) > 0:
            df = df.append(chunk, ignore_index=True)
            chunk = reader.get_chunk(100*1000)
            print('.'),
            sys.stdout.flush()
    except (StopIteration, KeyboardInterrupt):
        pass

    print('\nloaded {} rows'.format(len(df)))

    return df


def setup_files():
    files = []
    try:
        files.append(sys.argv[1])
    except Exception as e:
        print('You need to add your source data file')
        print("""Your command should look like this:
python impute_education.py source_data/my_source.dta
        """)
        sys.exit(2)

    return files

data_file = setup_files()[0]


""" Set up a data frame to work with """
#reader = pd.read_stata(data_file, chunksize=100000)
df = load_large_dta(data_file)

# make the new variables we'll need
# df['highest_qual'] = ''
# df['qual'] = ''


# Find a pidp with grades - 2051
# for i, row in enumerate(df.itertuples()):  # enumeration means the row begins 0
#     if type(row[3]) == str and row[3] != 'no qual':
#         print(row[1])
#         exit()


#df = df.head(78)
new_df = pd.DataFrame()

for i, row in enumerate(df.itertuples()):  # enumeration means the row begins 0
    #print('Processing {} ...'.format(i))
    locator = i
    pidp = row[1]

    # Just take an observation with values we can use for now
    if pidp != 2051:
        continue

    try:
        next_pidp = df.iloc[i+1][0]
    except IndexError:
        next_pidp = 0
        continue

    high_qual = row[3]

    # we want to skip some rows as we'll be repeating actions
    if next_pidp and pidp == next_pidp:
        continue

    print(pidp)
    # - Get the current pidp
    # - lookup every observation with this pidp as that is what we will be
    #   Running the logic on
    tmp_data = df.loc[df['pidp'] == pidp]
    print(tmp_data)

    # appending to a df
    # new_df = new_df.append({
    #     df.columns[0]:row[1],
    #     df.columns[1]:row[2],
    #     df.columns[2]:row[3],
    #     df.columns[3]:row[4]
    # }, ignore_index=True)

    # if type(high_qual) == str and high_qual != 'no qual':
    #     print(high_qual)
# print(df)
# print(new_df.head())