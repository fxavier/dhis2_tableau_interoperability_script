import datetime
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from shutil import copyfile
import numpy as np

def get_current_datetime():
    try:
        return datetime.datetime.now()
    except:
        return datetime.now()

def get_dhis2_auth():
    load_dotenv()
    return (os.getenv("DHIS_USERNAME"), os.getenv("DHIS_PASSWORD"))

def create_backup_directory(main_dir, backupDate):
    backupDir = main_dir + 'Backup' + backupDate
    try:
        os.mkdir(backupDir)
        print("Directory created: " + backupDir)
    except Exception as e:
        print("Directory not created:", e.__class__, "occurred.")
    return backupDir

def backup_files(fileList, main_dir, backupDir):
    for filename in fileList:
        try:
            copyfile(os.path.join(main_dir, filename), os.path.join(backupDir, filename))
            print(filename + " backed up")
        except Exception as e:
            print(filename + " not backed up:", e.__class__, "occurred.")

def retrieve_org_unit_data(main_dir, dhis2auth):
    try:
        # Retrieve Organization Unit Groups
        response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups", auth=dhis2auth)
        organisationUnitGroups = response.json()["organisationUnitGroups"]
        organisationUnitGroups = pd.DataFrame(organisationUnitGroups)

        # Identify the org unit group for ECHO Sites
        echoOrgUnitGroup = organisationUnitGroups.loc[organisationUnitGroups['displayName'] == 'ECHO Sites']["id"].tolist()[0]


        # Get the org unit ids (individual facilities) associated with ECHO
        response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups/" + echoOrgUnitGroup, auth=dhis2auth)
        echoOrgUnits = response.json()["organisationUnits"]
        echoOrgUnits = pd.DataFrame(echoOrgUnits)
        echoOrgUnits = echoOrgUnits["id"].tolist()

        # Get reference data for all org units in DHIS2 (including those not involved in ECHO)
        response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnits?paging=false&fields=id,code,displayName,path", auth=dhis2auth)
        organisationUnits = response.json()["organisationUnits"]
        organisationUnits = pd.DataFrame(organisationUnits)

        # Split the path string for the ECHO sites into ECHO / Province / District / Health Facility columns
        orgUnitData = organisationUnits.loc[organisationUnits['id'].isin(echoOrgUnits)]
        orgUnitData["path"].iloc[0].split('/')[2:]
        pathData = orgUnitData.path.str.split('/', expand=True)
        orgUnitData["province"] = pathData[2]
        orgUnitData["district"] = pathData[3]
        orgUnitData = orgUnitData.rename(columns={"displayName": "health facility"})
        orgUnitData = orgUnitData.drop(columns=["path"])
        orgUnitDataBackup = orgUnitData

        # Replace the province and district IDs with names
        orgUnitData[["province", "district"]] = orgUnitData[["province", "district"]].replace(
            organisationUnits["id"].to_list(), organisationUnits["displayName"].to_list())

        # Get the geo coordinates associated with ECHO
        response = requests.get("https://dhis2.echomoz.org/api/29/geoFeatures?ou=ou:OU_GROUP-" + echoOrgUnitGroup, auth=dhis2auth)
        echoGeoFeatures = response.json()
        echoGeoFeatures = pd.DataFrame(echoGeoFeatures)
        echoGeoFeatures = echoGeoFeatures[["id", "na", "co"]]

        # Parse out longitude and latitude
        echoGeoFeatures["longitude"] = echoGeoFeatures["co"].str.split(",", 1).str[0].str.strip("[")
        echoGeoFeatures["latitude"] = echoGeoFeatures["co"].str.split(",", 1).str[1].str.strip("]")
        echoGeoFeatures = echoGeoFeatures[['id', 'latitude', 'longitude']]

        # Merge geo information into Organisation Unit dataframe
        orgUnitData = orgUnitData.merge(echoGeoFeatures, how="left", on="id")

        # Re-sort the organization unit columns to put health facility last
        cols = orgUnitData.columns.tolist()
        cols = ['id', 'code', 'province', 'district', 'health facility', 'latitude', 'longitude']
        orgUnitData = orgUnitData[cols]

        return orgUnitData

    except Exception as e:
        print("Error occurred in retrieve_org_unit_data:", e)
        return None

def retrieve_reference_data(main_dir, dhis2auth):
    # Get Data Element Group Sets
    response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets?paging=false", auth=dhis2auth)
    dataElementGroupSets = response.json()['dataElementGroupSets']
    dataElementGroupSets = pd.DataFrame(dataElementGroupSets)

    # Restrict to the data element group set for ECHO export data
    echoExportDataElementGroupSet = dataElementGroupSets.loc[dataElementGroupSets['displayName'] == 'ECHO EXPORT'][
        "id"].tolist()[0]

    # Identify the Data Element Groups that are part of that set
    response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets/" + echoExportDataElementGroupSet,
                            auth=dhis2auth)
    exportDataElementGroups = response.json()['dataElementGroups']
    exportDataElementGroups = pd.DataFrame(exportDataElementGroups)

    # Get reference info on all data element groups
    response = requests.get("https://dhis2.echomoz.org/api/dataElementGroups?paging=false", auth=dhis2auth)
    dataElementGroups = response.json()['dataElementGroups']
    dataElementGroups = pd.DataFrame(dataElementGroups)

    # Identify the data element group for ECHO Targets
    targetDataElementGroup = dataElementGroups.loc[dataElementGroups['displayName'] == 'ECHO MOZ | Targets'][
        "id"].tolist()[0]

    # Get reference info on all data elements
    response = requests.get(
        "https://dhis2.echomoz.org/api/29/indicators.json?fields=id,displayName,displayShortName,numerator,denominator,indicatorGroups&paging=false",
        auth=dhis2auth)
    dataElements = response.json()['indicators']
    dataElements = pd.DataFrame(dataElements)

    # Convert the data element group dictionaries to a list
    separator = ';'
    dataElementGroupString = []
    for key, value in dataElements["dataElementGroups"].iteritems():
        temp = value
        keylist = []
        for entry in temp:
            keylist.append(entry["id"])
        dataElementGroupString.append(separator.join(keylist))
    dataElements["dataElementGroups"] = dataElementGroupString

    # Replace the data element group IDs with names
    dataElements[["dataElementGroups"]] = dataElements[["dataElementGroups"]].replace(
        dataElementGroups["id"].to_list(), dataElementGroups["displayName"].to_list(), regex=True)

    # Get information on Category Option Combos
    response = requests.get("https://dhis2.echomoz.org/api/categoryOptionCombos?paging=false", auth=dhis2auth)
    categoryOptionCombos = response.json()['categoryOptionCombos']
    categoryOptionCombos = pd.DataFrame(categoryOptionCombos)

    # Get Indicator Group Sets
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets?paging=false", auth=dhis2auth)
    indicatorGroupSets = response.json()['indicatorGroupSets']
    indicatorGroupSets = pd.DataFrame(indicatorGroupSets)

    # Restrict to the indicator group set for ECHO Export Data
    echoExportIndicatorGroupSet = indicatorGroupSets.loc[indicatorGroupSets['displayName'] == 'ECHO EXPORT'][
        "id"].tolist()[0]

    # Identify indicator groups that are part of the export set
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets/" + echoExportIndicatorGroupSet,
                            auth=dhis2auth)
    exportIndicatorGroups = response.json()['indicatorGroups']
    exportIndicatorGroups = pd.DataFrame(exportIndicatorGroups)

    # Get reference information on all indicator groups
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroups?paging=false", auth=dhis2auth)
    indicatorGroups = response.json()['indicatorGroups']
    indicatorGroups = pd.DataFrame(indicatorGroups)

    # Identify the TX_CURR export group
    txCurrIndicatorGroup = indicatorGroups[indicatorGroups['displayName'].str.contains('EXPORT TX_CURR')]["id"].tolist()[
        0]

    # Get reference information on all indicators
    response = requests.get("https://dhis2.echomoz.org/api/29/indicators?paging=false", auth=dhis2auth)
    indicators = response.json()['indicators']
    indicators = pd.DataFrame(indicators)
    indicators = indicators.set_index("id")

    # Convert the indicator group dictionaries to a list
    separator = ';'
    indicatorGroupString = []
    for key, value in indicators["indicatorGroups"].iteritems():
        temp = value
        keylist = []
        for entry in temp:
            keylist.append(entry["id"])
        indicatorGroupString.append(separator.join(keylist))
    indicators["indicatorGroups"] = indicatorGroupString

    # Replace the indicator group IDs with names
    indicators[["indicatorGroups"]] = indicators[["indicatorGroups"]].replace(
        indicatorGroups["id"].to_list(), indicatorGroups["displayName"].to_list(), regex=True)
    indicators = indicators.reset_index()

    # Replace the IDs in the numerator and denominator columns with names
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["#"], [""], regex=True)
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["\."], [", "],
                                                                                                  regex=True)
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
        dataElements["id"].to_list(), dataElements["displayName"].to_list(), regex=True)
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(
        categoryOptionCombos["id"].to_list(), categoryOptionCombos["displayName"].to_list(), regex=True)

    return exportDataElementGroups, dataElements, indicators, exportIndicatorGroups, txCurrIndicatorGroup

def generate_periods():
    # Generate a list of months to pull
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    years = list(range(2019, datetime.datetime.now().year + 1))
    years = [str(i) for i in years]
    initialPeriodList = [sub1 + sub2 for sub1 in years for sub2 in months]
    firstMonth = '201909'  # HARD-CODE for start of ECHO Dashboards
    currentMonth = str(datetime.datetime.now().year) + months[datetime.datetime.now().month - 1]
    periodList = list(filter(lambda x: x >= firstMonth and x < currentMonth, initialPeriodList))

    # Generate a list of quarters to pull
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    initialQuarterList = [sub1 + sub2 for sub1 in years for sub2 in quarters]
    firstQuarter = '2019Q4'  # HARD-CODE for start of ECHO Dashboards
    currentQuarterNumber = (datetime.datetime.now().month - 1) // 3 + 1
    currentQuarter = str(datetime.datetime.now().year) + 'Q' + str(currentQuarterNumber)
    quarterList = list(filter(lambda x: x >= firstQuarter and x < currentQuarter, initialQuarterList))

    # Create a list of all periods that should have targets
    scaffoldPeriods = [x for x in (initialPeriodList + initialQuarterList) if x >= '2020']
    periods = pd.DataFrame(scaffoldPeriods)
    periods.columns = ['period']
    periods['year'] = periods['period'].str[0:4]
    periods['type'] = np.where(periods['period'].str[4] == 'Q', 'Q', 'M')
    return periodList, quarterList, periods

def load_environment(dhis2auth):
    # Define the function to load the necessary environment data
    # You can customize this function to retrieve the required environment data from DHIS2 or any other source.

    # Placeholder data for demonstration purposes.
    # Replace these data with actual data retrieval from DHIS2 or other sources.
    exportDataElementGroups = pd.DataFrame({
        "id": ["DE_GROUP_1", "DE_GROUP_2"],
        "displayName": ["Data Element Group 1", "Data Element Group 2"]
    })

    dataElements = pd.DataFrame({
        "id": ["DE_1", "DE_2", "DE_3"],
        "displayName": ["Data Element 1", "Data Element 2", "Data Element 3"],
        "dataElementGroups": ["DE_GROUP_1", "DE_GROUP_1", "DE_GROUP_2"]
    })

    indicators = pd.DataFrame({
        "id": ["INDICATOR_1", "INDICATOR_2"],
        "displayName": ["Indicator 1", "Indicator 2"],
        "numerator": ["DE_1", "DE_2"],
        "denominator": ["DE_3", None],
        "dataElementGroups": ["DE_GROUP_1", "DE_GROUP_2"]
    })

    exportIndicatorGroups = pd.DataFrame({
        "id": ["INDICATOR_GROUP_1", "INDICATOR_GROUP_2"],
        "displayName": ["Indicator Group 1", "Indicator Group 2"]
    })

    txCurrIndicatorGroup = "INDICATOR_GROUP_1"

    return exportDataElementGroups, dataElements, indicators, exportIndicatorGroups, txCurrIndicatorGroup


def retrieve_indicator_data(dhis2auth, periodList, quarterList, exportIndicatorGroups, txCurrIndicatorGroup, echoOrgUnitGroup):
    allIndicatorValues = pd.DataFrame()
    txCurrDataValues = pd.DataFrame()
    exportIndicatorGroups['results'] = 0

    dataRetrievalStart = datetime.datetime.now()

    for period in periodList:
        print('Retrieving Period:', period)
        start = datetime.datetime.now()

        for indicatorGroup in list(exportIndicatorGroups['id']):
            response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period +
                                    "&dimension=dx:IN_GROUP-" + indicatorGroup + "&dimension=ou:OU_GROUP-" + echoOrgUnitGroup,
                                    auth=dhis2auth)
            if response.status_code == 200 and response.text != '{}':
                dataValues = response.json()['rows']
                dataValues = pd.DataFrame(dataValues)
                allIndicatorValues = allIndicatorValues.append(dataValues, ignore_index=True, sort=False)
                if indicatorGroup == txCurrIndicatorGroup:
                    txCurrDataValues = txCurrDataValues.append(dataValues, ignore_index=True, sort=False)

            indicatorHeaders = response.json()['headers']

            exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] = \
                exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] + len(
                    dataValues.index)

        elapsed_time = (datetime.datetime.now() - start)
        print(elapsed_time)

    dataRetrievalElapsedTime = (datetime.datetime.now() - dataRetrievalStart)
    print('Indicator Monthly Retrieval:', dataRetrievalElapsedTime)

    if len(noMonthlyResults) > 0:
        allIndicatorQuarterlyValues = pd.DataFrame()

        dataRetrievalStart = datetime.datetime.now()

        for period in quarterList:
            print('Retrieving Quarter:', period)
            start = datetime.datetime.now()

            for indicatorGroup in noMonthlyResults:
                response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period +
                                        "&dimension=dx:IN_GROUP-" + indicatorGroup + "&dimension=ou:OU_GROUP-" + echoOrgUnitGroup,
                                        auth=dhis2auth)
                if response.status_code == 200 and response.text != '{}':
                    dataValues = response.json()['rows']
                    dataValues = pd.DataFrame(dataValues)
                    allIndicatorQuarterlyValues = allIndicatorQuarterlyValues.append(dataValues, ignore_index=True,
                                                                                     sort=False)

                exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] = \
                    exportIndicatorGroups.loc[exportIndicatorGroups['id'] == indicatorGroup, 'results'] + len(
                        dataValues.index)

            elapsed_time = (datetime.datetime.now() - start)
            print(elapsed_time)

        indicatorHeaders = response.json()['headers']
        indicatorHeaders = pd.DataFrame(indicatorHeaders)
        allIndicatorQuarterlyValues.columns = indicatorHeaders['column']

        allIndicatorValues = allIndicatorValues.append(allIndicatorQuarterlyValues, ignore_index=True, sort=False)

        dataRetrievalElapsedTime = (datetime.datetime.now() - dataRetrievalStart)
        print('Indicator Quarterly Retrieval:', dataRetrievalElapsedTime)

    return allIndicatorValues, txCurrDataValues


def retrieve_data_element_data(dhis2auth, periodList, exportDataElementGroups):
    allDataElementValues = pd.DataFrame()
    exportDataElementGroups['results'] = 0

    dataRetrievalStart = datetime.now()

    if len(exportDataElementGroups) > 0:
        for period in periodList:
            print('Retrieving Period:', period)
            start = datetime.datetime.now()

            for dataElementGroup in list(exportDataElementGroups['id']):
                response = requests.get("https://dhis2.echomoz.org/api/29/analytics?dimension=pe:" + period +
                                        "&dimension=dx:DE_GROUP-" + dataElementGroup + "&dimension=co&dimension=ou:OU_GROUP-" + echoOrgUnitGroup,
                                        auth=dhis2auth)
                if response.status_code == 200 and response.text != '{}':
                    dataValues = response.json()['rows']
                    dataValues = pd.DataFrame(dataValues)
                    allDataElementValues = allDataElementValues.append(dataValues, ignore_index=True, sort=False)

                exportDataElementGroups.loc[exportDataElementGroups['id'] == dataElementGroup, 'results'] = \
                    exportDataElementGroups.loc[exportDataElementGroups['id'] == dataElementGroup, 'results'] + len(
                        dataValues.index)

            elapsed_time = (datetime.datetime.now() - start)
            print(elapsed_time)

    dataRetrievalElapsedTime = (datetime.datetime.now() - dataRetrievalStart)
    print('Data Element Monthly Retrieval:', dataRetrievalElapsedTime)

    return allDataElementValues

# Call the modular functions
def main():
    main_dir = r"dhis2/ECHO/Data"
    # Step 1: Setup and environment variables
    dhis2auth = (os.getenv("DHIS_USERNAME"), os.getenv("DHIS_PASSWORD"))  # NOTE: REPLACE WITH OAUTH2 AUTHENTICATION

    # Step 2: Load the environment
    exportDataElementGroups, dataElements, indicators, exportIndicatorGroups, txCurrIndicatorGroup = load_environment(dhis2auth)

    # Step 3: Generate list of periods
    periodList, quarterList, periods = generate_periods()
    

    # # Step 4: Retrieve organization unit data
    orgUnitData = retrieve_org_unit_data(main_dir, dhis2auth)
    print(orgUnitData)

    # Step 5: Retrieve indicator data
    echoOrgUnitGroup = orgUnitData.loc[orgUnitData['displayName'] == 'ECHO Sites']['id'].tolist()[0]
    allIndicatorValues, txCurrDataValues = retrieve_indicator_data(dhis2auth, periodList, quarterList, exportIndicatorGroups, txCurrIndicatorGroup, echoOrgUnitGroup)

    # Step 6: Retrieve data element data
    allDataElementValues = retrieve_data_element_data(dhis2auth, periodList, exportDataElementGroups)

    # Step 7: Perform any further data processing or analysis as needed.
    # Merge Indicator Values with Facility Information
    allIndicatorValues = allIndicatorValues.merge(orgUnitData, how='left', left_on='ou', right_on='id')
    txCurrDataValues = txCurrDataValues.merge(orgUnitData, how='left', left_on='ou', right_on='id')

    # Merge Data Element Values with Facility Information
    allDataElementValues = allDataElementValues.merge(orgUnitData, how='left', left_on='ou', right_on='id')

    # Calculate summary statistics for indicator values
    indicator_summary = allIndicatorValues.groupby('health facility')['value'].agg(['sum', 'mean', 'median', 'max', 'min'])

    # Calculate summary statistics for TxCurr indicator values
    txCurr_summary = txCurrDataValues.groupby('health facility')['value'].agg(['sum', 'mean', 'median', 'max', 'min'])

    # Calculate summary statistics for data element values
    data_element_summary = allDataElementValues.groupby('health facility')['value'].agg(['sum', 'mean', 'median', 'max', 'min'])

if __name__ == "__main__":
    main()
