import csv
import requests
import datetime
import os
import pandas as pd
import sqlite3
import random
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

CSV_FILES = ["tx_curr_data.csv", "merged_data.csv", "final_merged_data.csv"]

def remove_previous_files():
    for file in CSV_FILES:
        if os.path.exists(file):
            os.remove(file)
        else:
            print(f"The file {file} does not exist")

def generate_periods():
    months = [f"{i:02}" for i in range(1, 13)]  # list comprehension to generate months
    years = [str(i) for i in range(2019, datetime.datetime.now().year + 1)]
    initial_period_list = [y + m for y in years for m in months]
    return [x for x in initial_period_list if x >= '2023' and x < f"{datetime.datetime.now().year}{datetime.datetime.now().month:02}"]

def retrieve_tx_curr_data(dhis2auth):
    
    base_url = 'https://dhis2.echomoz.org/api/29/analytics/dataValueSet.json'
    # Break down dimensions_dx into multiple lines
    dimensions_dx = (
        'dkZHx1STjIG;ODw4ILhNQjc;DPelk9N6lkO;cIUIr28czEp;G0dQJXrmivL;'
        'WLg2OTJuuAL;KbF7faPyL3O;bedEKj2MvAp;yhgoaX6I5SI;M6radndNMcI;'
        'jVQfAT1i6s9;WMZn1kseuAQ;wn0nkF3Qy4Z;Y9AnAmH63gR;JDUIlfZsvNR;'
        'uSgb62avNGS;T3IjjSH3Qoi;oaTFQGtXVUE;prfHdt6Iwfm;G6qCaD1HZB5;'
        'DpFwGPrnM5b;UjfH29cP2nO;Pr5xXF4u6Po;TuecmWZs13b;QgbgKIU8ha6;'
        'bD6uStxG20G;EhvHDjy4K3i;Wn0Dl0HNkmL;g3dx0K9C4v9;xlIpRdZDuVD;'
        'JRZFbJX8n8e;AGVOZPlKVoY;RzZd4yEq6QU;pS0OjaYEg1J;wW1hykhFepQ;'
        'sbcw1HgmdFz;ZXDkDfHug0l;isBiAEvhk18;gRe79HOGjEy;oat5DroeXt5;'
        'xPnsEjFx1BM;pMpiFnDRQvV;RHG3dzhAULq;Ewz8cNU1YMb;CQQ6OvfEwX4;'
        'bWK3Qvg7oeJ;Fe1x5nNpZ20;c3emJS6Ivsx;wFCYshbrI19;Z5o7he63Us4;'
        'HxXEZp5pM1t;lqwRTBbmf7r;oNH46nmDgPO;TLQBZazjlyP;TiVspxdO2Nv;'
        'QvMMyrUY6Sg;Gfb7iLhjkvk;eV1Jk45moHc;VkobvrJtE8K;z6WZZFeeWt7;'
        'AxecPb6xcCB;RjhuJBWMRmz;oMDsrhbICit;wYCXwqqlmIX'
    )

    dimensions_ou = 'OU_GROUP-fwkewapqBD3;zQUKoh5WmJt'

    # periods = generate_periods()
    periods = '202309'

    # Create a string with all periods separated by a semicolon
    dimensions_pe = ";".join(periods)
    
    display_property = 'NAME'
    hierarchy_meta = 'true'

    # Construct the URL
    url = '{}?dimension=dx:{}&dimension=ou:{}&dimension=pe:{}&displayProperty={}&hierarchyMeta={}'.format(
        base_url, dimensions_dx, dimensions_ou, dimensions_pe, display_property, hierarchy_meta)
    
    response = requests.get(url, auth=dhis2auth)
    txCurr = response.json()["dataValues"]
    if response.status_code == 200:
        txCurr = response.json()["dataValues"]

        # Convert the data into a pandas DataFrame
        df = pd.DataFrame(txCurr)

        # Save the DataFrame to a CSV file
        df.to_csv('tx_curr_data.csv', index=False)
        print('tx_curr_data.csv saved successfully')
    else:
        print(f"Request failed with status code {response.status_code}")
        
def merge_csv_files():
    org_units_df = pd.read_csv('orgunits.csv')
    tx_curr_data_df = pd.read_csv('tx_curr_data.csv')
    pd.merge(org_units_df, tx_curr_data_df, on='orgUnit').to_csv('merged_data.csv', index=False)

def merge_with_indicators_name_files():  
    merged_data_df = pd.read_csv('merged_data.csv')
    TXCURR_Indicators_df = pd.read_csv('TXCURR_Indicators.csv')
    pd.merge(merged_data_df, TXCURR_Indicators_df, on='dataElement') \
        .to_csv('final_merged_data.csv', index=False)

def extract_data_from_indicator(indicator_string):
    try:
        header, data = indicator_string.split('|')
        age, sex = [part.strip() for part in data.split(',')]
        return sex, age
    except:
        print(f"Error processing indicator_string: {indicator_string}")
        return None, None

def insert_data_into_sqlite_database():
    conn = sqlite3.connect('db.sqlite3')
    cur = conn.cursor()
    
    df = pd.read_csv('final_merged_data.csv')
    for index, row in df.iterrows():
        sex, age = extract_data_from_indicator(row['name'])
        
        if not sex or not age:
            continue

        cur.execute("""
            INSERT INTO core_dadosecho (us, indicator, periodo, valor, sexo, Age)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (row['orgUnit'], row['dataElement'], row['period'], row['value'], sex, age))
        
    conn.commit()
    conn.close()


def insert_into_mysql_db():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )

        cursor = connection.cursor()
        df = pd.read_csv('final_merged_data.csv')
        for index, row in df.iterrows():
            sex, age = extract_data_from_indicator(row['name'])
            
            if not sex or not age:
                continue

            cursor.execute("""
                INSERT INTO core_dadosecho (orgUnit, dataElement, period, value, sex, age)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (row['orgUnit'], row['dataElement'], row['period'], row['value'], sex, age))

        connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def main():
    remove_previous_files()
    load_dotenv()
    dhis2auth = (os.getenv("DHIS_USERNAME"), os.getenv("DHIS_PASSWORD"))
    retrieve_tx_curr_data(dhis2auth)
    merge_csv_files()
    merge_with_indicators_name_files()
    insert_data_into_sqlite_database()
    # insert_into_mysql_db()  # Uncomment this when you want to use MySQL
    
if __name__ == "__main__":
    main()
