# Ashleys imputation
Basically data cleaning... nothing much to see in the readme so here is something to look at:

![image](https://i.imgur.com/3Yv8Fet.png)

### Pseudo code

```
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
```
