import pandas as pd
import time
import random
import utils
import sys
import os
import pickle
import gc
import math
import numpy as np
from datetime import datetime


startTime = datetime.now()


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
df = load_large_dta(data_file)

# TODO make the new variables we'll need
# df['highest_qual'] = ''
# df['qual'] = ''

#df = df.head(78)
new_df = pd.DataFrame()

qual_map = {
    'no qual': 16,
    'GCSE etc.': 16,
    'A level etc.': 18,
    'other qual': 18,
    'other higher qual': 20,
    'degree': 21,
    'teaching or nursing': 22, # TODO - split values 20
}

# Teaching and GCSE 894887
# Just teaching and nursing 314859
# GCSE, Teching, degree 760267
# A level and teching 897615
# Alevel teaching other higher qual 274208645
# GCSE, teaching/nursing 4465571
# Alevel, teaching and degree 4747771
# no qual Teaching/nursing 4908251?
# no qual, teaching, other high qual 69967249
# Alevel, teaching, other 136967645
# GCSE teching other higher qual 275129369
# Alevel teaching degree 342377965
# Alevel, teaching NaN,  other higher qual
# Just alevel = 91807
# just gcse = 223727
# just other higher qual = 34122, 156407, 170082
# Just degree 112887
# just teaching or nursing 301247
# Other high qual and degree 943851
# A level and teching 897615
# no qual onlt 4767
# just other qual 400527
# 224729809 teaching post 2013
# 8167 - other higher qual first with degree later
# to check 9527

working_pid = 172727

""" Cleaning up """
# Merges higher degree and first degree
# enumeration means the row begins 0

# for _i, _row in enumerate(df.itertuples(), start=1):
#     try:
#         pidp = _row[1]

#         if working_pid and pidp != working_pid:
#             continue

#         print(
#             f"Merging first and higher degree into 'degree'... {len(df.index) - _row[0]}", end="", flush=True)
#         print('\r', end='')
#         if df.at[_i, 'high_qual'] == 'higher degree' or df.at[_i, 'high_qual'] == 'first degree':
#             df.at[_i, 'high_qual'] = 'degree'
#     except KeyError as e:
#         print(e)
#         pass

""" Helper functions"""
def get_pid_by_qual(data_frame, qual):
    # enumeration means the row begins 0
    for i, row in enumerate(data_frame.itertuples()):
        pidp = row[1]

        try:
            next_pidp = data_frame.iloc[i+1][0]
        except IndexError:
            next_pidp = 0
            continue

        high_qual = row[3]
        # we want to skip some rows as we'll be repeating actions
        if next_pidp and pidp == next_pidp:
            continue

        _tmp_data = data_frame.loc[data_frame['pidp'] == pidp]

        quals = []
        for _i, _row in _tmp_data.iterrows():
            if _tmp_data.at[_i, 'high_qual'] and type(_tmp_data.at[_i, 'high_qual']) == str:
                quals.append(_tmp_data.at[_i, 'high_qual'])

        quals = list(set(quals))

        if len(quals) == len(qual):
            for c in qual:
                if c in quals:
                    print(pidp)

# get_pid_by_qual(df, ['teaching or nursing'])
# exit()

def single_grade_backfill(tmp_data, quals_to_ignore=[], qual_map_override=qual_map, first_wave_qual=None, backfill_age=None):
    backfill_year = 0
    #print('Backfilling by single grade...')
    qm = qual_map_override.copy()
    # remove what we know we don't want to fill
    for i in quals_to_ignore:
        qm[i] = 0

    for _i, _row in tmp_data.iterrows():
        try:
            if type(tmp_data.at[_i, 'high_qual']) != str and type(tmp_data.at[_i+1, 'high_qual']) == str:
                if first_wave_qual:
                    backfill = first_wave_qual
                else:
                    backfill = tmp_data.at[_i+1, 'high_qual']


                # get backfill year val so we don't accidently fill forwad
                backfill_year = tmp_data.at[_i+1, 'year']
                # now get rid of everything in the qual map above this age
                for i in qm:
                    if int(qm[i]) > int(qm[backfill]):
                        #print(i)
                        qm[i] = 0
        except KeyError as e:
            print(f'{e}... continuing')
    for _i, _row in tmp_data.iterrows():
        if type(tmp_data.at[_i, 'high_qual']) != str and tmp_data.at[_i, 'year'] < backfill_year:
            # find the qual map for this age
            for qual, age in qm.items():    # for name, age in dictionary.iteritems():  (for Python 2.x)
                if backfill_age:
                    if age <= int(tmp_data.at[_i, 'age']) and age != 0 and int(tmp_data.at[_i, 'age']) < backfill_age:
                        tmp_data.at[_i, 'high_qual'] = qual
                else:
                    if age <= int(tmp_data.at[_i, 'age']) and age != 0:
                        tmp_data.at[_i, 'high_qual'] = qual
    return tmp_data


""" Backfilling values """
for i, row in enumerate(df.itertuples()):  # enumeration means the row begins 0
    print('Processing {} ...'.format(i))
    locator = i
    pidp = row[1]

    # Just take an observation with values we can use for now
    if working_pid and pidp != working_pid:

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
    # Get all the possible quals
    quals = []
    backfill_years = []
    backfill_quals = []
    for _i, _row in tmp_data.iterrows():
        if tmp_data.at[_i, 'high_qual'] and type(tmp_data.at[_i, 'high_qual']) == str:
            quals.append(tmp_data.at[_i, 'high_qual'])
            backfill_years.append(tmp_data.at[_i, 'year'])
            backfill_quals.append(tmp_data.at[_i, 'high_qual'])
        pass
        #tmp_data.at[_i,'high_qual'] = 'Overridden'
    quals = list(set(quals))

    """routes"""
    # Single qualifications
    if quals and len(quals) < 2:
        if quals[0] == 'no qual':
            #print('Just other higher qual')
            tmp_data = single_grade_backfill(tmp_data, [
                'GCSE etc.',
                'A level etc.',
                'other higher qual',
                'other qual',
                'degree',
                'teaching/nursing'
            ])

        elif quals[0] == 'GCSE etc.':
            #print('Just other higher qual')
            tmp_data = single_grade_backfill(tmp_data)

        elif quals[0] == 'other qual':
            tmp_data = single_grade_backfill(tmp_data, ['A level etc.'])

        elif quals[0] == 'other higher qual':
            #print('Just other higher qual')
            tmp_data = single_grade_backfill(tmp_data, ['A level etc.', 'other qual'])
        elif quals[0] == 'A level etc.':
            #print('Just A level')
            tmp_data = single_grade_backfill(tmp_data, ['other qual'])

        elif quals[0] == 'degree':
            print('Just degree')
            tmp_data = single_grade_backfill(
                tmp_data, ['other higher qual', 'teaching/nursing', 'other qual'])

        elif quals[0] == 'teaching or nursing':
            print('Just Teaching/Nursing')
            # Backfill_years [0] will be the first wave year observation
            if backfill_years[0] < 2013:
                qual_map = {
                    'no qual': 16,
                    'GCSE etc.': 16,
                    'A level etc.': 18,
                    'other qual': 18,
                    'other higher qual': 20,
                    'degree': 21,
                    'teaching or nursing': 20,
                }
                tmp_data = single_grade_backfill(tmp_data, ['other higher qual','other qual','degree'], qual_map)
            else:
                pass

    # Combination of qualifications
    elif quals:
        print('More than 1 qual')
        if set(['other higher qual', 'degree']).issubset(quals) or set(['other higher qual', 'teaching or nursing']).issubset(quals):
            if backfill_years[0] < 2013 and backfill_quals[0] == 'teaching or nursing':
                qual_map = {
                    'no qual': 16,
                    'GCSE etc.': 16,
                    'A level etc.': 18,
                    'other qual': 18,
                    'other higher qual': 20,
                    'degree': 21,
                    'teaching or nursing': 20,
                }
                tmp_data = single_grade_backfill(tmp_data, ['other higher qual','other qual','degree'], qual_map)
            elif backfill_quals[0] == 'other higher qual':
                qual_map = {
                    'no qual': 16,
                    'GCSE etc.': 16,
                    'A level etc.': 18,
                    'other qual': 0,
                    'other higher qual': 20,
                    'degree': 0,
                    'teaching or nursing': 0,
                }
                tmp_data = single_grade_backfill(tmp_data, ['other qual'], qual_map)
            else:
                print(tmp_data)
                tmp_data = single_grade_backfill(tmp_data)
        else:
            # EG Alevel and teaching would be caught here
            for _i, _row in tmp_data.iterrows():
                try:
                    if type(tmp_data.at[_i, 'high_qual']) != str and type(tmp_data.at[_i+1, 'high_qual']) == str:
                        backfill = tmp_data.at[_i+1, 'high_qual']
                        backfill_age = tmp_data.at[_i+1, 'age']
                        # check the first wave qual value, work out what we need to ignore in the method
                        if backfill == 'GCSE etc.':
                            #print('Just other higher qual')
                            tmp_data = single_grade_backfill(tmp_data)

                        elif backfill == 'other qual':
                            print(tmp_data)
                            tmp_data = single_grade_backfill(tmp_data,[], qual_map, backfill_quals[0], backfill_age)
                            print(tmp_data)
                            exit()

                        elif backfill == 'other higher qual':
                            #print('Just other higher qual')
                            tmp_data = single_grade_backfill(
                                tmp_data, ['A level etc.'])

                        elif backfill == 'A level etc.':
                            print('A level')
                            tmp_data = single_grade_backfill(tmp_data, ['other qual'])

                        elif backfill == 'degree':
                            #print('Just degree')
                            tmp_data = single_grade_backfill(
                                tmp_data, ['other higher qual', 'teaching or nursing', 'other qual'])

                        elif backfill == 'teaching or nursing':
                            #print('Just Teaching/Nursing')
                            if backfill_years[0] < 2013:
                                qual_map = {
                                    'no qual': 16,
                                    'GCSE etc.': 16,
                                    'A level etc.': 18,
                                    'other qual': 18,
                                    'other higher qual': 20,
                                    'degree': 21,
                                    'teaching or nursing': 20,
                                }
                                tmp_data = single_grade_backfill(tmp_data, ['other higher qual','other qual','degree'], qual_map)
                            else:
                                tmp_data = single_grade_backfill(tmp_data, ['other higher qual', 'other_qual'])

                        elif backfill == 'no qual':
                            print(tmp_data)
                            tmp_data = single_grade_backfill(tmp_data, [
                                'GCSE etc.',
                                'A level etc.',
                                'other higher qual',
                                'other qual',
                                'degree',
                                'teaching/nursing'
                            ])
                            print(tmp_data)
                            exit()

                        else:
                            print(tmp_data)
                            print('NOT CAUGHT')
                            exit()
                            tmp_data = single_grade_backfill(tmp_data)


                except KeyError as e:
                    print(f'{e}... continuing')

    """forward filling"""
    # get the latest wave
    forward_fill = False
    for _i, _row in tmp_data.iterrows():
        try:
            if type(tmp_data.at[_i, 'high_qual']) != str and type(tmp_data.at[_i-1, 'high_qual']) == str:
                forward_fill = tmp_data.at[_i-1, 'high_qual']
                year = tmp_data.at[_i-1, 'year']
        except KeyError as e:
            print(f'{e}... continuing')
        # Now fill every high_qual with {forward_fill} with any row above the {year}
        if forward_fill and year:
            for _i, _row in tmp_data.iterrows():
                if type(tmp_data.at[_i, 'high_qual']) != str and tmp_data.at[_i, 'year'] > year:
                    tmp_data.at[_i, 'high_qual'] = forward_fill

    new_df = new_df.append(tmp_data)

new_df.to_csv('out_data/impute_education.csv')
print(datetime.now() - startTime)