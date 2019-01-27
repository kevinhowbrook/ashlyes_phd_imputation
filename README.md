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
```python keep_full_info/make_unique_pids.py source_data/raw.dta source_data/clean.dta out_data/unique_pids.dta```
This will creata a dta file of unique pids where you specify, eg out_data/unique_pids.dta

Then using the above dta, run the next script:
```
python keep_full_info/create_new_clean.py source_data/clean_mar.dta out_data/unique_pids.dta out_data/new_cleaned.dta
```

![image](https://i.imgur.com/3Yv8Fet.png)