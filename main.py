import pandas as pd
import time
import random

start = time.time()


"""
Sanitize the datas
TODOS:
- Random seeding
- Rounding rules
"""
data_file = 'data/for_imputation.dta'

""" Set up a data frame to work with """
reader=pd.read_stata(data_file, chunksize=100000)
df = pd.DataFrame()
_df_out = pd.DataFrame()

for itm in reader:
    df=df.append(itm)

# Just slice the data for now
df = df

for i,row in enumerate(df.itertuples()):  # enumeration means the row begins 0
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

    if row[8] == 1: # if _09
        # Go through whole dataset and get all observations
        # that meet this rows last, married and counter, eg never,never and 4
        # from those records get the mastat values.
        _last = row[9]
        _next = row[10]
        _counter = row[12]
        _satisfy_frame = pd.DataFrame()
        _satisfy_frame = _satisfy_frame.append(
            df.loc[
                    (df['last'] == _last) & (df['next'] == _next) & (df['counter'] == _counter)
                    #df['counter'] == _counter
                ]
        )
        counts = _satisfy_frame['mastat'].value_counts().to_dict()
        total = 0
        percentages = []
        if counts:
            for total_item in counts:
                total = total + counts[total_item]

            for item in counts:
                percentages.append([item, int(counts[item] / total * 100)])

        random_number = random.randint(1,100)
        # make an interval list
        intervals = []
        last = 0
        for i,val in enumerate(percentages):
            if i == 0:
                intervals.append([0, val[1], val[0]])
                last = val[1]
            else:
                intervals.append([last + 1, val[1] + last, val[0]])
                last = last + val[1]

        for i in intervals:
            if i[0] <= random_number <= i[1]:
                x = i
                # print('Yes')
                # print(random_number)
                # print(i)

        # Next is replace and populate


            #     print(item)
            #     print(counts[item])
            #     print()
            # print(total)



end = time.time()
print('This took: {} seconds'.format(int(end - start)))


"""
for each row:

        # loop over the whole data again and check for the condition
        for each row:
            if this_condition_is_met:
                # get the mastat vale and store it in a dataframe eg:
                mastats.append(this_row['mastat'])

        # from mastat dataframe, calculate some percentages
        # EG count 1's, devide by total * 100 to give us a % value A (1)
        # Same for B and C - like this:
        # A = x%
        # B = Y%
        # C = z%

        # TODO: Rounding rule

        # create the intervals from the % calculated above
        # _A = 0, X%
        # _B = X+1, X+Y
        # _C = X+Y+1, 100

        # Example of the above would look something like
        # _a = 0, 60
        # _b = (60+1),(60+5)
        # _c = (60+5)+1, 100
        # generate random number between 1 and 100
        # From the random number work out what it falls into, EG if the
        # random number generated is 55.... you guessed it, it's _a ;)
        # this_row['mastat'] == A

        df_out.to_csv("testing.csv")
"""
