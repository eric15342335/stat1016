# eric15342335
# async version of requests
import pandas as pd
import os, glob, grequests, pyrfc6266

df_tags = []
filtered_one = []

# you only need to edit the `year` variable and the `for days in range(1, 32):` loop
# the latter is for months with 31 days, you need to change the range to 30 or 28/29 for February
_year = "2022"
months = "12".zfill(2)
num_of_days = 1 + 30  # 1 + 31 because the range function is exclusive, so it will only go up to 30

for days in range(31, 32):
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

    if _year == "2020":
        branch = "master"
    else:
        branch = "main"

    url = []
    for name in ["Details", "Sentiment"]:
        for hour in range(24):
            hour = str(hour).zfill(2)
            if not os.path.exists(f'{date}_{hour}_Summary_{name}.csv'): # if the file does not exist, download it
                url += [f"https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset{year}/{branch}/Summary_{name}/{folder_name}/{date}_{hour}_Summary_{name}.csv"]
            #print(hour, url)
    
    print("Downloading", date, "Summary Details and Sentiment")
    rs = (grequests.get(u, stream=False) for u in url)

    
    results = grequests.map(rs, size=len(url))
    for response in results:
        if response.status_code == 200:
            with open(pyrfc6266.requests_response_to_filename(response), 'wb') as f:
                f.write(response.content)
                print("Downloaded", pyrfc6266.requests_response_to_filename(response))
                response.close()
        else:
            print(response.status_code, response.url)

    df_details = []
    # Load the first CSV table with ID and Tag
    for f in glob.glob("*_Summary_Details.csv"):
        df_details.append(pd.read_csv(f))

    sentiment = []
    for f in glob.glob("*_Summary_Sentiment.csv"):
        sentiment.append(pd.read_csv(f))

    merged_df = pd.DataFrame()
    if len(df_details) >= 2:
        merged_df = pd.concat([df_details.pop(0), df_details.pop(0)], axis=0)
    elif len(df_details) == 1:
        merged_df = df_details[0]
    else:
        print("No hashtag file found")
        for f in glob.glob(f"{date}?*.csv"):
            os.remove(f)
        continue

    if not merged_df.empty:
        for loop in df_details:
            merged_df = pd.concat([merged_df, loop], axis=0)

    merged_df_2 = pd.DataFrame()
    if len(sentiment) >= 2:
        merged_df_2 = pd.concat([sentiment.pop(0), sentiment.pop(0)], axis=0)
        for loop in sentiment:
            merged_df_2 = pd.concat([merged_df_2, loop], axis=0)
    elif len(sentiment) == 1:
        merged_df_2 = sentiment[0]
    else:
        print("No sentiment file found")
        for f in glob.glob(f"{date}?*.csv"):
            os.remove(f)
        continue

    if not merged_df_2.empty:
        combined = pd.merge(merged_df, merged_df_2, on='Tweet_ID')
        combined.to_csv(f'result/{date}_details_combined.csv', index=False)