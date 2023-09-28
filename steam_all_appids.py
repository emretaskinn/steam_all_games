# Before usage export GOOGLE_APPLICATION_CREDENTIALS environment variable
# as path to your JSON google credentials file
import time
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from flask import Flask, Response
from tqdm import tqdm
import json

# customisations - ensure tables show all columns
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 50)

steam_games_table = "nth-wording-258215.steam_all_games.steam_all_games_updated"


def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the GCS bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents, "text/csv")

    print(
        f"{destination_blob_name} with contents {contents} uploaded to {bucket_name}."
    )


def get_request(url, parameters=None):
    """Return json-formatted response of a get request using optional parameters.

    Parameters
    ----------
    url : string
    parameters : {'parameter': 'value'}
        parameters to pass as part of get request

    Returns
    -------
    json_data
        json-formatted response (dict-like)
    """
    try:
        response = requests.get(url=url, params=parameters)
    except requests.exceptions.SSLError as s:
        print('SSL Error:', s)

        for i in range(5, 0, -1):
            print('\rWaiting... ({})'.format(i), end='')
            time.sleep(1)
        print('\rRetrying.' + ' ' * 10)

        # recusively try again
        return get_request(url, parameters)

    except requests.exceptions.ReadTimeout as rt:
        print('Read Timeout Error: ', rt)

        for i in range(5, 0, -1):
            print('\rWaiting... ({})'.format(i), end='')
            time.sleep(1)
        print('\rRetrying.' + ' ' * 10)

        return get_request(url, parameters)

    except requests.exceptions.ConnectionError as ce:
        print('Connection Error: ', ce)

        for i in range(5, 0, -1):
            print('\rWaiting... ({})'.format(i), end='')
            time.sleep(1)
        print('\rRetrying.' + ' ' * 10)

        return get_request(url, parameters)

    if response:
        # check if response has a legit json, otherwise return None
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print('JSON Decoder Error:', e)
            return None
    else:
        # response is none usually means too many requests. Wait and try again
        print('No response, waiting 10 seconds...')
        time.sleep(10)
        print('Retrying.')
        return get_request(url, parameters)


def get_existing_steam_data_from_bq(table_id):
    client = bigquery.Client()

    sql = f"""
        SELECT steam_appid
        FROM `{table_id}`
    """

    df = client.query(sql).to_dataframe()
    return df

# def get_nongame_from_bq(): #next step excluding type: nongame data


def get_appids():
    '''
    gets all app ids through Steam WEB API and
    returns dataframes for all existing, new and all APP IDs
    '''

    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.request("GET", url)
    json_results = response.json()
    all_appids = pd.DataFrame(json_results['applist'])
    all_appids = pd.json_normalize(all_appids['apps'])
    all_appids.sort_values(by='appid',
                           ascending=True,
                           inplace=True,
                           ignore_index=True)
    all_appids = all_appids['appid']

    # decided to skip ste of getting the existing data from bq
    '''existing_appids = get_existing_steam_data_from_bq()
    existing_appids.sort_values(by='steam_appid',
                                ascending=True,
                                inplace=True,
                                ignore_index=True)
    existing_appids = existing_appids['steam_appid']'''

    # new_appids = pd.concat([all_appids, existing_appids])
    # new_appids = pd.DataFrame()
    new_appids = pd.concat([all_appids])
    new_appids.drop_duplicates(keep=False, inplace=True)

    columns = ['appid']
    new_appids = pd.DataFrame(new_appids, columns=columns)
    new_appids.reset_index(drop=True, inplace=True)

    # return new_appids, all_appids, existing_appids
    return new_appids, all_appids


def steam_app_crawler(start, finish, id_list):
    '''
    Returns a dataframe that contains
    all information for new App IDs through Steam App Details API
    '''
    steam_games = pd.DataFrame()
    df2 = pd.DataFrame()
    other_entries = pd.DataFrame()

    for i in range(start, finish):
        if i < len(id_list):
            appid = str(id_list[i])
            url = "http://store.steampowered.com/api/appdetails?appids=" + appid
            parameters = {"request": 'all'}

            # request 'all' from steam spy and parse into dataframe
            time.sleep(0.8)
            json_data = get_request(url, parameters=parameters)
            if json_data is not None:
                if (json_data[appid]['success'] is True and
                        json_data[appid]['data']['type'] == 'game'):
                    df2 = pd.DataFrame.from_dict(json_data, orient='index')
                    df2 = pd.json_normalize(df2['data'])
                    steam_games = pd.concat([steam_games, df2])
                elif (json_data[appid]['success'] is True):
                    df = pd.DataFrame.from_dict(json_data, orient='index')
                    df = pd.json_normalize(df['data'])
                    # pick just a name and id
                    df = df[['name', 'steam_appid']]
                    other_entries = pd.concat([other_entries, df])

    return steam_games, other_entries


# returns true when data successfully fetched and saved
def iterator(start, finish, step, id_list, bar):
    bar.update(1)
    if finish < len(id_list):
        steam_games, other_entries = steam_app_crawler(start, finish, id_list)
        file_name = f"steam_games({start}-{finish}).csv"
        steam_games.to_csv(file_name, mode='a', sep='\t')
        other_entries.to_csv("other_entries.csv", mode='a', sep='\t')
        iterator(finish, finish+step, step, id_list, bar)
    else:
        steam_games, other_entries = steam_app_crawler(start, finish, id_list)
        file_name = f"steam_games({start}-{finish}).csv"
        steam_games.to_csv(file_name, mode='a', sep='\t')
        other_entries.to_csv("other_entries.csv", mode='a', sep='\t')

    return True


def get_non_game_data():
    '''
    returns Non-Game Steam APP IDs in order to exclude them for future crawls
    '''
    steam_games_appids = steam_games['steam_appid']
    new_appids = new_appids['appid']
    non_game_appids = pd.concat([steam_games_appids, new_appids])
    non_game_appids.drop_duplicates(keep=False, inplace=True)

    columns = ['appid']
    non_game_appids = pd.DataFrame(non_game_appids, columns=columns)
    non_game_appids.reset_index(drop=True, inplace=True)

    return non_game_appids


def steam_data_prep():
    '''
    Returns dataframe that contains need info from Steam API
    '''
    steam_all_games_master = steam_games[['name', 'steam_appid', 'required_age',
                                          'is_free', 'developers',
                                          'publishers',
                                          'categories', 'genres',
                                          'price_overview.initial',
                                          'price_overview.final',
                                          'platforms.windows',
                                          'platforms.mac',
                                          'platforms.linux',
                                          'release_date.coming_soon',
                                          'release_date.date']]

    # dropping outlier value
    steam_all_games_master.drop(steam_all_games_master.loc[steam_all_games_master['required_age']=='16+'].index, inplace=True)
    # converting object to int for 'required age'
    steam_all_games_master['required_age'] = pd.to_numeric(steam_all_games_master['required_age'])


    steam_all_games_master['price_overview.initial'] = steam_all_games_master['price_overview.initial']/100
    steam_all_games_master['price_overview.final'] = steam_all_games_master['price_overview.final']/100
    steam_all_games_master = steam_all_games_master.reset_index(drop=True)


    steam_all_games_master['genres'] = steam_all_games_master['genres'].fillna('Empty')

    # Creating Is Indie field
    for i in range(0, len(steam_all_games_master['genres'])):
        if 'Indie' in steam_all_games_master.iloc[i].genres:
            steam_all_games_master.at[i, 'is Indie'] = True
        else:
            steam_all_games_master.at[i, 'is Indie'] = False
    return steam_all_games_master


# Takes data and prepares it for bigquery table
def wrangle(df):
    df = df[['name', 'steam_appid', 'required_age',
             'is_free', 'developers',
             'publishers',
             'categories', 'genres',
             'price_overview.initial',
             'price_overview.final',
             'platforms.windows',
             'platforms.mac',
             'platforms.linux',
             'release_date.coming_soon',
             'release_date.date']]

    df = df.rename(columns={"price_overview.initial": "price_overview_initial",
                            "price_overview.final": "price_overview_final",
                            "platforms.windows": "platforms_windows",
                            "platforms.mac": "platforms_mac",
                            "platforms.linux": "platforms_linux",
                            "release_date.coming_soon": "release_date_coming_soon",
                            "release_date.date": "release_date_date"})

    df.drop(df.loc[df['required_age'] == '16+'].index, inplace=True)
    df.drop(df.loc[df['required_age'] == '18+'].index, inplace=True)
    df.drop(df.loc[df['required_age'] == '１８'].index, inplace=True)
    # df['required_age'] = df['required_age'].fillna('')
    df = df[df['required_age'].notna()]
    # Pay attention to this one, it works for one set but skips everything for others :/
    # df.drop(df.loc[df['required_age'].str.contains('')].index, inplace=True)
    # df.drop(df.loc[('+' in df['required_age'])].index, inplace=True)
    # df.drop(df.loc[('' in df['required_age'])].index, inplace=True)
    df['required_age'] = pd.to_numeric(df['required_age'])
    df['price_overview_initial'] = df['price_overview_initial']/100
    df['price_overview_final'] = df['price_overview_final']/100
    df = df.reset_index(drop=True)
    df = df.replace('', '"Empty"')
    df['genres'] = df['genres'].fillna('[{"id": 0, "description": "Empty"}]')
    df['categories'] = df['categories'].fillna('[{"id": 0, "description": "Empty"}]')

    df['genres'] = df['genres'].astype(str).str.replace("'", '"', regex=False)
    df['genres'] = df['genres'].apply(json.loads)

    df['categories'] = df['categories'].astype(str).str.replace("'", '"', regex=False)
    df['categories'] = df['categories'].apply(json.loads)

    for i in range(0, len(df['genres'])):
        genres = []
        categories = []
        if 'Indie' in df.iloc[i].genres:
            df.at[i, 'is_indie'] = True
        else:
            df.at[i, 'is_indie'] = False

        for j in (df['genres'][i]):
            genres.append(j['description'])
        df.at[i,  'Genres'] = ','.join(genres)

        for j in (df['categories'][i]):
            categories.append(j['description'])
        df.at[i, 'Categories'] = ','.join(categories)

    df = df.drop('genres', axis=1)
    df = df.drop('categories', axis=1)
    df = df.rename(columns={"Genres": "genres",
                            "Categories": "categories"})

    # df.to_csv(f'{file_name}_updated.csv', mode='a', sep='\t')
    return df


def push_to_bq(data_frame, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=[
            # bigquery.SchemaField("int64_field_0", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("steam_appid", "INTEGER"),
            bigquery.SchemaField("required_age", "INTEGER"),
            bigquery.SchemaField("is_free", "BOOLEAN"),
            bigquery.SchemaField("developers", "STRING"),
            bigquery.SchemaField("publishers", "STRING"),
            bigquery.SchemaField("categories", "STRING"),
            bigquery.SchemaField("genres", "STRING"),
            bigquery.SchemaField("price_overview_initial", "FLOAT"),
            bigquery.SchemaField("price_overview_final", "FLOAT"),
            bigquery.SchemaField("platforms_windows", "BOOLEAN"),
            bigquery.SchemaField("platforms_mac", "BOOLEAN"),
            bigquery.SchemaField("platforms_linux", "BOOLEAN"),
            bigquery.SchemaField("release_date_coming_soon", "BOOLEAN"),
            bigquery.SchemaField("release_date_date", "STRING"),
            bigquery.SchemaField("is_indie", "BOOLEAN"),
        ],
        # skip_leading_rows=0,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_dataframe(
        data_frame, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


if __name__ == "__main__":
    new_appids, all_appids = get_appids()
    # non_game_appids = get_non_game_data()
    batch_head = 0
    batch_tail = 20
    iterator_step = 20
    n = int(len(new_appids)/20)
    bar = tqdm(total=n)
    # iterator(batch_head, batch_tail, iterator_step, new_appids['appid'], bar)
