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
#         print(row[3])

# exit()

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

    # - Get the current pidp
    # - lookup every observation with this pidp as that is what we will be
    #   Running the logic on
    tmp_data = df.loc[df['pidp'] == pidp]
    # Loop through a frame by pidp and assign correct values
    # for _i, _row in enumerate(tmp_data.itertuples()):

    #     tmp_data.at[i,'high_qual'] = 'hi'
    # #print(tmp_data)
    for _i, _row in tmp_data.iterrows():
        pass
        #tmp_data.at[_i,'high_qual'] = 'Overridden'
        # First row will start at age 16

        # Assumptions
        # Average age for qualifications
        # - no_qual is the default and will replace NaN, it means no_qual
        #   Someone with any qualification will never get no_qual assigned.
        # - GCSE 16
        # - A-Level 18
        # - other higher qual = 20

        """ EG
        If we find someone has a first_degree value at age 50,
        this means at 50 they were observed with a degree.
        We assume that the age they got that degree would be 21
        BUT, if we creep back in the data and find they have high_qual A-levels
        at age 48, then we know they got there degree at 49.
        so:
        45 NaN
        46 NaN
        47 NaN
        48 a_levels
        49 first_degree
        49 first_degree

        Given the above, we now need to add valuse going back from alevls to be like
        24 NaN
        25 NaN
        26 GCSE    << Note GCSE pops up, se we fill in 25,24 etc going back to age 16.
        ...
        46 a_levels
        47 a_levels
        48 a_levels
        49 first_degree
        49 first_degree

        """


        # logic here for amending/overriding values
        # in pidp 2051, the high_qual is first_degree, age achieved 21.
        # So we need to fill in high_qual values going back to when they were 21 or
        #  until another high_qual is found EG GCSE.
        # Before that should be left as NaN


    print(tmp_data)

    # new_df = new_df.append(tmp_data)


    # appending to a df
    # new_df = new_df.append({
    #     df.columns[0]:row[1],
    #     df.columns[1]:row[2],
    #     df.columns[2]:row[3],
    #     df.columns[3]:row[4]
    # }, ignore_index=True)

    # if type(high_qual) == str and high_qual != 'no qual':
    #     print(high_qual)
#print(tmp_data)
# print(df)
print(new_df)