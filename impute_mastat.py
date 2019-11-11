import pandas as pd
import time
import random
import utils
import sys
import os


start = time.time()


def setup_files():
    files = []
    try:
        files.append(sys.argv[1])
    except Exception as e:
        print("You need to add your source data file")
        print(
            """Your command should look like this:
python impute_mastat.py source_data/my_source.dta out_data/my_output.dta
        """
        )
        sys.exit(2)
    try:
        files.append(sys.argv[2])
    except Exception as e:
        print("You need to add your output data file")
        print(
            """Your command should look like this:
python impute_mastat.py source_data/my_source.dta out_data/my_output.dta
        """
        )
        sys.exit(2)

    return files


data_file = setup_files()[0]
output_file = setup_files()[1]

# 'source_data/for_imputation.dta'

""" Set up a data frame to work with """
reader = pd.read_stata(data_file, chunksize=100000)
df = pd.DataFrame()

for itm in reader:
    df = df.append(itm)

df["mastat_missing"] = ""

# For testing just take the first 40
# df = df.head()
percentages_report = []


for i, row in enumerate(df.itertuples()):  # enumeration means the row begins 0
    print("Processing {} ...".format(i))

    locator = i
    # TODO: Split out prev next to methods
    # If this is slow we can just use cols[9] and [10] which ash has populated
    # with the next and prev values we are interested in.
    # print('This row counter {}'.format(row[12]))
    # print('Prev row counter {}'.format(df.iloc[i-1][11]))
    # try:
    #     print('Next row counter {}'.format(df.iloc[i+1][11]))
    # except IndexError:
    #     print('End of set')
    #     pass
    # if row[12] == 2: # if _09 <<Testing
    # if row[8] == 0: # if _09
    if row[4] == 1:  # if _09
        # Go through whole dataset and get all observations
        # that meet this rows last, married and counter, eg never,never and 4
        # from those records get the mastat values.
        _last = row[5]
        _next = row[6]
        _counter = row[7]
        _satisfy_frame = pd.DataFrame()
        # _satisfy_frame = _satisfy_frame.append(
        #     df.loc[
        #         # For testing just use the counter and set the head to 50
        #         (df['last'] == _last) & (df['next'] == _next) & (df['counter'] == _counter) & (df['year'] == 2009) |
        #         (df['last'] == _last) & (df['next'] == _next) & (
        #             df['counter'] == _counter)
        #         #df['counter'] == _counter
        #     ]
        # )
        prev_counter = df.iloc[i - 1][6]
        condition_1 = df.loc[
            (df["last"] == _last)
            & (df["next"] == _next)
            & (df["counter_pre"] == prev_counter)
            & (df["year"] == 2009)
            & (df["_09"] == 0)
        ]

        condition_2 = df.loc[
            (df["last"] == _last)
            & (df["next"] == _next)
            & (df["counter_pre"] == prev_counter)
            & (df["_09"] == 0)
        ]

        if not condition_1.empty:
            _satisfy_frame = _satisfy_frame.append(condition_1)
            print("condition 1 used")
        elif not condition_2.empty:
            _satisfy_frame = _satisfy_frame.append(condition_2)
            print("condition 2 used")
        else:
            _satisfy_frame = _satisfy_frame.append(df.iloc[i - 1])
            # print('condition 3 used')
            # use the prev rowdf.iloc[i-1]
        counts = _satisfy_frame["mastat"].value_counts().to_dict()
        total = 0
        percentages = []
        if counts:
            for total_item in counts:
                total = total + counts[total_item]

            for item in counts:
                # rounded_val = utils.round_to_100(int(counts[item] / total * 100)
                percentages.append([item, counts[item] / total * 100])

        # Rounding
        percentages = utils.round_percentages(percentages)

        random_number = random.randint(1, 100)
        # make an interval list
        intervals = []
        last = 0
        for i, val in enumerate(percentages):
            if i == 0:
                intervals.append([0, val[1], val[0]])
                last = val[1]
            else:
                intervals.append([last + 1, val[1] + last, val[0]])
                last = last + val[1]
        for interval in intervals:
            if interval[0] <= random_number <= interval[1]:
                df.at[locator, "mastat_missing"] = interval[2]

df["mastat_missing"] = df["mastat_missing"].astype(str)  # convert type
df.to_stata(output_file)
end = time.time()

print("This took: {} seconds".format(int(end - start)))

# TODO -
# Refactor to be run with source data as file argument
# whole script .dta, not csv
