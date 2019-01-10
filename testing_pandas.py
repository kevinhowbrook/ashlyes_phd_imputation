import pandas as pd

df = pd.DataFrame(
    index=['cobra', 'viper', 'sidewinder'],
    columns=['max_speed', 'shield'])

print(df.count())
print(df.loc['cobra'])