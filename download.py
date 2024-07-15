# eric15342335
import pandas as pd
import os, glob, requests

df_tags = []
filtered_one = []

# you only need to edit the `year` variable and the `for days in range(1, 32):` loop
# the latter is for months with 31 days, you need to change the range to 30 or 28/29 for February
_year = "2021"
months = "12".zfill(2)
num_of_days = 1 + 31  # 1 + 31 because the range function is exclusive, so it will only go up to 30

session = requests.Session()
for days in range(1, num_of_days):
    days = str(days).zfill(2)   # the file name of the csv files/folders are 2022_12_01_something, 2022_12_02_something, etc
    # so need fill the zero if the day is less than 10
    date = f"{_year}_{months}_{days}"
    folder_name = date[0:7] # the folder name is 2022_12, so extract the first 7 characters of the date

    if _year == "2022":
        """name of repo for 2021 is COVID19_Tweets_Dataset_2021
        2022 is COVID19_Tweets_Dataset
        2020 is COVID19_Tweets_Dataset_2020
        so need treat differently"""
        year = ""
    else:
        year = "_" + _year

    for name in ["Hashtag", "Sentiment"]:
        for hour in range(24):
            hour = str(hour).zfill(2)
            url = f"https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset{year}/main/Summary_{name}/{folder_name}/{date}_{hour}_Summary_{name}.csv"
            print(hour, url)
            if not os.path.exists(f'{date}_{hour}_Summary_{name}.csv'): # if the file does not exist, download it
                response = session.get(url, timeout=10)
                if response.status_code == 200:
                    with open(f'{date}_{hour}_Summary_{name}.csv', 'wb') as f:
                        f.write(response.content)
                else:
                    print("Error", response.status_code)


    # Load the first CSV table with ID and Tag
    for f in glob.glob("*_Summary_Hashtag.csv"):
        df_tags = pd.read_csv(f)
        filtered_one += [df_tags[df_tags["Hastag"].str.contains('covid', case=False)
                                | df_tags["Hastag"].str.contains('omicorn', case=False)
                                | df_tags["Hastag"].str.contains('corona', case=False)
                                | df_tags["Hastag"].str.contains('lockdown', case=False)
                                | df_tags["Hastag"].str.contains('stayhome', case=False)
                                | df_tags["Hastag"].str.contains('wearamask', case=False)
                                ].drop_duplicates(subset=['Tweet_ID'])]

    merged_df = pd.DataFrame()
    if len(filtered_one) >= 2:
        merged_df = pd.concat([filtered_one.pop(0), filtered_one.pop(0)], axis=0)
    elif len(filtered_one) == 1:
        merged_df = filtered_one[0]
    else:
        print("No hashtag file found")
        for f in glob.glob(f"{date}?*.csv"):
            os.remove(f)
        continue

    if not merged_df.empty:
        for loop in filtered_one:
            merged_df = pd.concat([merged_df, loop], axis=0)

    df_tags = []
    # Load the first CSV table with ID and Tag
    for f in glob.glob(f"{date}?*_Summary_Sentiment.csv"):
        df_tags += [pd.read_csv(f)]

    merged_df_2 = pd.DataFrame()
    if len(df_tags) >= 2:
        merged_df_2 = pd.concat([df_tags.pop(0), df_tags.pop(0)], axis=0)
        for loop in df_tags:
            merged_df_2 = pd.concat([merged_df_2, loop], axis=0)
    elif len(df_tags) == 1:
        merged_df_2 = df_tags[0]
    else:
        print("No sentiment file found")
        for f in glob.glob(f"{date}?*.csv"):
            os.remove(f)
        continue

    if not merged_df_2.empty:
        combined = pd.merge(merged_df, merged_df_2, on='Tweet_ID')
        combined.to_csv(f'result/{date}_combined.csv', index=False)

    for f in glob.glob(f"{date}?*.csv"):
        os.remove(f)
