# eric15342335
# async version of requests
import pandas as pd
import os, glob, grequests, pyrfc6266

df_tags = []
filtered_one = []

# you only need to edit the `year` variable and the `for days in range(1, 32):` loop
# the latter is for months with 31 days, you need to change the range to 30 or 28/29 for February
_year = "2022"
for months in [3]:
    months = str(months).zfill(2)
    if months in ["01", "03", "05", "07", "08", "10", "12"]:
        num_of_days = 1 + 31
    elif months in ["04", "06", "09", "11"]:
        num_of_days = 1 + 30
    elif months == "02":
        if _year == "2020":
            num_of_days = 1 + 29
        else:
            num_of_days = 1 + 28

    try:
        os.mkdir(f"{_year}_{months}_daily")
    except FileExistsError:
        pass

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

        if _year == "2020":
            branch = "master"
        else:
            branch = "main"

        url = []
        for name in ["Hashtag", "Sentiment"]:
            for hour in range(24):
                hour = str(hour).zfill(2)
                if not os.path.exists(f'{date}_{hour}_Summary_{name}.csv'): # if the file does not exist, download it
                    url += [f"https://raw.githubusercontent.com/lopezbec/COVID19_Tweets_Dataset{year}/"
                            f"{branch}/Summary_{name}/{folder_name}/{date}_{hour}_Summary_{name}.csv"]
                #print(hour, url)
        
        print("Downloading", date, "Summary Hashtag and Sentiment")
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

        if _year != "2020":
            tag_name = "Hastag"
        else:   # Researcher data mistake typo :(((((
            tag_name = "Hashtag"

        for f in glob.glob("*_Summary_Hashtag.csv"):
            df_tags = pd.read_csv(f, encoding= 'unicode_escape')
            tag_name = df_tags.columns[1]
            print(f"Filtering {f}, {tag_name}")
            filtered_one += [df_tags[df_tags[tag_name].str.contains('covid', case=False)
                                    | df_tags[tag_name].str.contains('omicorn', case=False)
                                    | df_tags[tag_name].str.contains('corona', case=False)
                                    | df_tags[tag_name].str.contains('lockdown', case=False)
                                    | df_tags[tag_name].str.contains('stayhome', case=False)
                                    | df_tags[tag_name].str.contains('wearamask', case=False)
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

        filtered_one = []
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
            combined.to_csv(f"{folder_name}_daily/{date}_combined.csv", index=False)
        merged_df = pd.DataFrame()
        merged_df_2 = pd.DataFrame()


        """
        dataframe = []
        # regex
        for file in glob.glob(f"{_year}_{months}_*_combined.csv"):
            dataframe.append(pd.read_csv(file))

        concat_df = pd.concat(dataframe, axis=0)
        concat_df.to_csv(f'result/{_year}_{months}_combined.csv', index=False)"""
        for f in glob.glob(f"*Summary?*.csv"):
            os.remove(f)
