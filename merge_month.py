import pandas as pd
import glob

dataframe = []
# regex
for file in glob.glob("2020_04_*_combined.csv"):
    dataframe.append(pd.read_csv(file))

concat_df = pd.concat(dataframe, axis=0)
concat_df.to_csv('2020_04_combined.csv', index=False)
