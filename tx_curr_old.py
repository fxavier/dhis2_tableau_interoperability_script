import csv
import requests
import datetime
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import random
import mysql.connector
from mysql.connector import Error

def remove_previous_files():
    files_to_remove = ["tx_curr_data.csv", "merged_data.csv", "final_merged_data.csv"]

    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
        else:
            print(f"The file {file} does not exist")


def generate_periods():
    # Generate a list of months to pull
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    years = list(range(2019, datetime.datetime.now().year + 1))
    years = [str(i) for i in years]
    initialPeriodList = [sub1 + sub2 for sub1 in years for sub2 in months]
    firstMonth = '2023'  
    currentMonth = str(datetime.datetime.now().year) + months[datetime.datetime.now().month - 1]
    periodList = list(filter(lambda x: x >= firstMonth and x < currentMonth, initialPeriodList))
    
    return periodList

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

    periods = generate_periods()

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
    # Read the CSV files
    orgUnits_df = pd.read_csv('orgunits.csv')
    tx_curr_data_df = pd.read_csv('tx_curr_data.csv')

    # Merge the dataframes
    merged_df = pd.merge(orgUnits_df, tx_curr_data_df, on='orgUnit')

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv('merged_data.csv', index=False)

merge_csv_files()

def merge_with_indicators_name_files():
    # Read the CSV files
    merged_data_df = pd.read_csv('merged_data.csv')
    TXCURR_Indicators_df = pd.read_csv('TXCURR_Indicators.csv')

    # Merge the dataframes
    final_merged_df = pd.merge(merged_data_df, TXCURR_Indicators_df, on='dataElement')

    # Save the merged dataframe to a new CSV file
    final_merged_df.to_csv('final_merged_data.csv', index=False)

merge_csv_files()

def extract_sex(indicator_string):
    if '|' in indicator_string:
        # Split the string on the pipe (|) to separate the header from the data
        header, data = indicator_string.split('|')

        # Split the data part on commas to separate the individual data pieces
        data_parts = data.split(',')

        # Select the second data part (index 1, since indexing starts at 0 in Python)
        sex = data_parts[1].strip()  # .strip() removes any leading or trailing white space
    else:
        print(f"Error: expected '|' in indicator_string but got {indicator_string}")
        sex = None
    return sex


def extract_age(indicator_string):
    if '|' in indicator_string:
        # Split the string on the pipe (|) to separate the header from the data
        header, data = indicator_string.split('|')

        # Split the data part on commas to separate the individual data pieces
        data_parts = data.split(',')

        # Select the first data part (index 0, since indexing starts at 0 in Python)
        age = data_parts[0].strip()  # .strip() removes any leading or trailing white space
    else:
        print(f"Error: expected '|' in indicator_string but got {indicator_string}")
        age = None
    return age


def insert_data_into_sqlite_database():
    # Create an engine
    conn = sqlite3.connect('db.sqlite3')
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE dadosecho (
        provincia TEXT, 
        distrito TEXT, 
        ou_id TEXT, 
        us TEXT, 
        program TEXT, 
        periodo TEXT, 
        indicator TEXT, 
        valor INTEGER, 
        tipoperiodo TEXT, 
        sexo TEXT, 
        Age TEXT, 
        dt_ds TEXT, 
        mdd_6 TEXT, 
        male_eng TEXT, 
        ap3 TEXT, 
        ajuda TEXT, 
        athiv TEXT
    )
''')

    
    # reading data from the CSV file
    with open('final_merged_data.csv') as f:
       reader = csv.reader(f)
       data = list(reader)
       
    for row in data:
        provincia=row[0]
        distrito=row[1]
        ou_id=row[3]
        us=row[2]
        program="TX_CURR"
        periodo=row[5]
        indicator=row[11]
        valor=row[6]
        tipoperiodo="Monthly"
        sexo=extract_sex(indicator)
        Age=extract_age(indicator)
        dt_ds = random.randint(0, 1)
        mdd_6 = bool(random.getrandbits(1))
        male_eng = bool(random.getrandbits(1))
        ap3 = bool(random.getrandbits(1))
        ajuda = random.randint(0, 1)
        athiv = bool(random.getrandbits(1))

        cur.execute("INSERT INTO dadosecho (provincia, distrito, ou_id, us, program, periodo, indicator, valor, tipoperiodo, sexo, Age, dt_ds, mdd_6, male_eng, ap3, ajuda, athiv) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                    (provincia, distrito, ou_id, us, program, periodo, indicator, valor, tipoperiodo, sexo, Age, dt_ds, mdd_6, male_eng, ap3, ajuda, athiv))
        conn.commit()
    conn.close()

def insert_into_mysql_db(data_values):
    try:
        # Establish a connection to your MySQL database
        connection = mysql.connector.connect(
            host='localhost',  # replace with your host
            database='database_name',  # replace with your database name
            user='username',  # replace with your username
            password='password')  # replace with your password

        # Create a cursor object using cursor() method
        cursor = connection.cursor()
# reading data from the CSV file
        with open('final_merged_data.csv') as f:
           reader = csv.reader(f)
           data = list(reader)

        for row in data:
            provincia=row[0]
            distrito=row[1]
            ou_id=row[3]
            us=row[2]
            program="TX_CURR"
            periodo=row[5]
            indicator=row[11]
            valor=row[6]
            tipoperiodo="Monthly"
            sexo=extract_sex(indicator)
            Age=extract_age(indicator)
            dt_ds = random.randint(0, 1)
            mdd_6 = bool(random.getrandbits(1))
            male_eng = bool(random.getrandbits(1))
            ap3 = bool(random.getrandbits(1))
            ajuda = random.randint(0, 1)
            athiv = bool(random.getrandbits(1))

            # Prepare SQL query to INSERT data into database.
            insert_data_query = """
                INSERT INTO dadosecho (provincia, distrito, ou_id, us, program, periodo, indicator, valor, tipoperiodo, sexo, Age, dt_ds, mdd_6, male_eng, ap3, ajuda, athiv) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_data_query, data_values)

            # Commit your changes in the database
            connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    remove_previous_files()
    load_dotenv()
    dhis2auth = (os.getenv("DHIS_USERNAME"), os.getenv("DHIS_PASSWORD"))
    retrieve_tx_curr_data(dhis2auth)
    merge_csv_files()
    merge_with_indicators_name_files()
    insert_data_into_sqlite_database()
   # insert_into_mysql_db()
   
    
    
if __name__ == "__main__":
    main()
