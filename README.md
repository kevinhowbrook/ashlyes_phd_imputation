# Ashleys imputation
Basically data cleaning... nothing much to see in the readme so here is something to look at:

## Running on Windows
Helpful commands:

List files

``` ls ```

Change directory

```cd [name_of_dir]```

### Running the imputation script

```python3 imputate_mastat.py source_data/[source.dta] out_data/[out.dta]```

When you want to add a new source file, open your directory in file explorer and your dta file to the source_data file. You will then need to modify the new dta file's permissions:

```sudo chmod -R 777 source_data```

This will ask for you password.

### Running the keep_full_info scripts.
The first to run will be:
```python3 keep_full_info/make_unique_pids.py source_data/raw.dta source_data/clean.dta out_data/unique_pids.dta```
This will creata a dta file of unique pids where you specify, eg out_data/unique_pids.dta

Then using the above dta, run the next script:
```
python3 keep_full_info/create_new_clean.py source_data/clean_mar.dta out_data/unique_pids.dta out_data/new_cleaned.dta
```

![image](https://i.imgur.com/3Yv8Fet.png)


## Impute education logic


Assumptions
Average age for qualifications
- no_qual is the default and will replace NaN, it means no_qual Someone with any qualification will never get no_qual assigned.
- no qual - 16
- gcse - 16
- a level - 18
- other higher - 20
- first degree - 21
- high degree - 26

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
