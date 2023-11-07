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
    except Exception as e:
        print(f"Error occurred in get_current_datetime: {e}")
        return None

def create_backup_directory(main_dir, backup_date):
    backup_dir = os.path.join(main_dir, 'Backup', backup_date)
    try:
        os.makedirs(backup_dir, exist_ok=True)
        print("Directory created: " + backup_dir)
    except Exception as e:
        print(f"Directory not created: {e}")
    return backup_dir

def get_dhis2_auth():
    """
    Retrieve DHIS2 authentication credentials from environment variables.
    
    Returns:
        tuple: A tuple containing the username and password.
    """
    load_dotenv()
    return os.getenv("DHIS_USERNAME"), os.getenv("DHIS_PASSWORD")


def fetch_org_unit_groups(dhis2auth):
    """Fetch Organization Unit Groups from DHIS2."""
    response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnitGroups", auth=dhis2auth)
    if response.ok:
        return response.json()["organisationUnitGroups"]
    else:
        response.raise_for_status()

def fetch_org_units_by_group(group_id, dhis2auth):
    """Fetch Organization Units by Group from DHIS2."""
    response = requests.get(f"https://dhis2.echomoz.org/api/29/organisationUnitGroups/{group_id}", auth=dhis2auth)
    if response.ok:
        return response.json()["organisationUnits"]
    else:
        response.raise_for_status()


def fetch_all_org_units(dhis2auth):
    """Fetch all Organization Units from DHIS2."""
    response = requests.get("https://dhis2.echomoz.org/api/29/organisationUnits", auth=dhis2auth)
    if response.ok:
        return response.json()["organisationUnits"]
    else:
        response.raise_for_status()

def fetch_geo_features_by_group(group_id, dhis2auth):
    """Fetch geographic features for Organization Units by Group from DHIS2."""
    response = requests.get(f"https://dhis2.echomoz.org/api/29/organisationUnitGroups/{group_id}/geoFeatures", auth=dhis2auth)
    if response.ok:
        return response.json()["features"]
    else:
        response.raise_for_status()

def fetch_org_unit_details(org_unit_id, dhis2auth):
    """Fetch details of a specific Organization Unit from DHIS2."""
    response = requests.get(f"https://dhis2.echomoz.org/api/29/organisationUnits/{org_unit_id}", auth=dhis2auth)
    if response.ok:
        return response.json()
    else:
        response.raise_for_status()

def fetch_org_unit_ancestors(org_unit_id, dhis2auth):
    """Fetch ancestor details for a specific Organization Unit from DHIS2."""
    response = requests.get(f"https://dhis2.echomoz.org/api/29/organisationUnits/{org_unit_id}?fields=ancestors[id,name]", auth=dhis2auth)
    if response.ok:
        return response.json()["ancestors"]
    else:
        response.raise_for_status()

def process_org_unit_groups(org_unit_groups_raw):
    """Process raw organization unit groups data into a structured format."""
    # Assuming org_unit_groups_raw is a list of dicts with relevant data
    processed_groups = [
        {
            'id': group['id'],
            'name': group['name']
        }
        for group in org_unit_groups_raw
    ]
    return processed_groups

def process_org_units(org_units_raw):
    """Process raw organization units data into a structured DataFrame."""
    # Convert raw data into a DataFrame, assuming org_units_raw is a list of dicts
    df = pd.DataFrame(org_units_raw)
    # Process the DataFrame as needed, e.g., renaming columns, formatting data, etc.
    df.rename(columns={'id': 'OrgUnitID', 'name': 'OrgUnitName'}, inplace=True)
    return df

def split_org_unit_path(df, column='path'):
    """Split the organization unit path into separate columns."""
    # Assumes the path is a string of IDs separated by slashes
    split_paths = df[column].str.split('/', expand=True)
    # Rename the columns as needed and concatenate with the original DataFrame
    split_paths.columns = [f'Level{i+1}' for i in range(split_paths.shape[1])]
    df = pd.concat([df, split_paths], axis=1)
    return df

def replace_ids_with_names(df, id_name_map, column='OrgUnitID'):
    """Replace organization unit IDs with their corresponding names."""
    # Assuming id_name_map is a dict mapping IDs to names
    df[column] = df[column].apply(lambda x: id_name_map.get(x, x))
    return df

def parse_geo_coordinates(features_raw):
    """Parse geographic coordinates from raw geo features data."""
    # Assuming features_raw is a list of feature dicts with 'geometry' key
    processed_features = [
        {
            'id': feature['id'],
            'coordinates': feature['geometry']['coordinates']
        }
        for feature in features_raw if 'geometry' in feature
    ]
    return processed_features

def merge_geo_data(org_units_df, geo_features):
    """Merge geographic data into the organization units DataFrame."""
    # Assuming geo_features is a list of dicts with 'id' and 'coordinates'
    geo_df = pd.DataFrame(geo_features)
    merged_df = pd.merge(org_units_df, geo_df, left_on='OrgUnitID', right_on='id', how='left')
    return merged_df

def sort_org_unit_columns(merged_df, column_order):
    """Sort the organization unit DataFrame columns based on a specified order."""
    # column_order is a list of column names specifying the desired order
    sorted_df = merged_df[column_order]
    return sorted_df

def format_org_unit_data(merged_df):
    """Perform final formatting on the organization unit DataFrame."""
    # Perform any final formatting required, e.g., converting data types, setting index, etc.
    formatted_df = merged_df.copy()
    formatted_df['coordinates'] = formatted_df['coordinates'].astype(str)
    formatted_df.set_index('OrgUnitID', inplace=True)
    return formatted_df

def create_org_unit_hierarchy(merged_df):
    """Create a hierarchy structure for organization units."""
    # This function assumes that the merged_df has columns for each hierarchy level
    hierarchy = merged_df.apply(
        lambda row: ' > '.join([row[f'Level{i}'] for i in range(1, 5) if f'Level{i}' in row]), axis=1
    )
    merged_df['Hierarchy'] = hierarchy
    return merged_df

def filter_org_units_by_criteria(merged_df, criteria):
    """Filter the organization units DataFrame based on specified criteria."""
    # criteria is a dict where keys are column names and values are the values to filter by
    for column, value in criteria.items():
        merged_df = merged_df[merged_df[column] == value]
    return merged_df

def fetch_data_element_group_sets(dhis2auth):
    """Fetch Data Element Group Sets from the API."""
    response = requests.get("https://dhis2.echomoz.org/api/29/dataElementGroupSets?paging=false", auth=dhis2auth)
    response.raise_for_status()  # This will raise an HTTPError if the request failed
    return pd.DataFrame(response.json()['dataElementGroupSets'])

def fetch_data_element_groups(dhis2auth, group_set_id):
    """Fetch Data Element Groups that are part of a set."""
    response = requests.get(f"https://dhis2.echomoz.org/api/29/dataElementGroupSets/{group_set_id}", auth=dhis2auth)
    response.raise_for_status()
    return pd.DataFrame(response.json()['dataElementGroups'])

def fetch_indicators(dhis2auth):
    """Fetch all Indicators from the API."""
    response = requests.get("https://dhis2.echomoz.org/api/29/indicators?paging=false", auth=dhis2auth)
    response.raise_for_status()
    return pd.DataFrame(response.json()['indicators'])

def fetch_indicator_groups(dhis2auth):
    """Fetch all Indicator Groups from the API."""
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroups?paging=false", auth=dhis2auth)
    response.raise_for_status()
    return pd.DataFrame(response.json()['indicatorGroups'])

def fetch_indicator_group_sets(dhis2auth):
    """Fetch all Indicator Group Sets from the API."""
    response = requests.get("https://dhis2.echomoz.org/api/29/indicatorGroupSets?paging=false", auth=dhis2auth)
    response.raise_for_status()
    return pd.DataFrame(response.json()['indicatorGroupSets'])

def fetch_category_option_combos(dhis2auth):
    """Fetch all Category Option Combos from the API."""
    response = requests.get("https://dhis2.echomoz.org/api/29/categoryOptionCombos?paging=false", auth=dhis2auth)
    response.raise_for_status()
    return pd.DataFrame(response.json()['categoryOptionCombos'])

def process_data_elements(data_elements, data_element_groups):
    """Convert the data element group dictionaries to a list and replace IDs with names."""
    separator = ';'
    data_element_group_string = [
        separator.join(entry["id"] for entry in group) 
        if isinstance(group, list) else ''
        for group in data_elements["dataElementGroups"]
    ]
    data_elements["dataElementGroups"] = data_element_group_string

    # Create a mapping from data element group IDs to names
    id_name_map = data_element_groups.set_index('id')['displayName'].to_dict()

    # Replace the data element group IDs with names
    data_elements["dataElementGroups"] = data_elements["dataElementGroups"].replace(id_name_map, regex=True)
    return data_elements

def process_indicators(indicators, indicator_groups):
    """Convert the indicator group dictionaries to a list and replace IDs with names."""
    separator = ';'
    indicator_group_string = [
        separator.join(entry["id"] for entry in group)
        if isinstance(group, list) else ''
        for group in indicators["indicatorGroups"]
    ]
    indicators["indicatorGroups"] = indicator_group_string

    # Create a mapping from indicator group IDs to names
    id_name_map = indicator_groups.set_index('id')['displayName'].to_dict()

    # Replace the indicator group IDs with names
    indicators["indicatorGroups"] = indicators["indicatorGroups"].replace(id_name_map, regex=True)
    return indicators

def process_indicator_group_sets(indicator_group_sets):
    """Process indicator group sets and format as needed."""
    # Assuming the raw indicator group sets data needs to be formatted into a DataFrame
    processed_indicator_group_sets = pd.DataFrame(indicator_group_sets)
    # Additional formatting can be done here if needed
    return processed_indicator_group_sets

def process_category_option_combos(category_option_combos):
    """Process category option combos and format as needed."""
    # Convert raw category option combos data into a structured DataFrame
    processed_category_option_combos = pd.DataFrame(category_option_combos)
    # Additional formatting can be done here if needed
    return processed_category_option_combos

def replace_ids_with_names(df, id_name_map, column):
    """Replace the IDs with names in a DataFrame based on a given mapping."""
    # Assuming id_name_map is a DataFrame with 'id' and 'displayName' columns
    id_to_name = id_name_map.set_index('id')['displayName'].to_dict()
    df[column] = df[column].replace(id_to_name, regex=True)
    return df

def format_numerators_and_denominators(indicators, data_elements, category_option_combos):
    """Replace the IDs in the numerator and denominator columns with names."""
    # Remove '#' characters and replace '.' with ', '
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["#"], [""], regex=True)
    indicators[["numerator", "denominator"]] = indicators[["numerator", "denominator"]].replace(["\."], [", "], regex=True)
    
    # Create mappings from IDs to names for data elements and category option combos
    data_element_id_to_name = data_elements.set_index('id')['displayName'].to_dict()
    category_option_combo_id_to_name = category_option_combos.set_index('id')['displayName'].to_dict()
    
    # Replace the data element and category option combo IDs with names
    indicators['numerator'] = indicators['numerator'].replace(data_element_id_to_name, regex=True)
    indicators['numerator'] = indicators['numerator'].replace(category_option_combo_id_to_name, regex=True)
    indicators['denominator'] = indicators['denominator'].replace(data_element_id_to_name, regex=True)
    indicators['denominator'] = indicators['denominator'].replace(category_option_combo_id_to_name, regex=True)
    
    return indicators

def get_echo_export_group_set_id(data_element_group_sets, export_name):
    """Extract the ID of the 'ECHO EXPORT' data element group set."""
    return data_element_group_sets.loc[data_element_group_sets['displayName'] == export_name, 'id'].tolist()[0]

def retrieve_reference_data(main_dir, dhis2auth):
    # Fetch data element group sets
    data_element_group_sets = fetch_data_element_group_sets(dhis2auth)
    
    # Extract the ID for the 'ECHO EXPORT' data element group set
    echo_export_data_element_group_set_id = get_echo_export_group_set_id(data_element_group_sets, 'ECHO EXPORT')
    
    # Fetch data element groups within the 'ECHO EXPORT' group set
    export_data_element_groups = fetch_data_element_groups(dhis2auth, echo_export_data_element_group_set_id)

    # Fetch and process data elements
    data_elements_raw = fetch_indicators(dhis2auth)  # Assuming this function gets raw data elements
    data_elements = process_data_elements(data_elements_raw, export_data_element_groups)

    # Fetch and process indicators
    indicators_raw = fetch_indicators(dhis2auth)  # Assuming this function gets raw indicators
    indicator_groups = fetch_indicator_groups(dhis2auth)
    category_option_combos = fetch_category_option_combos(dhis2auth)
    
    # Format numerators and denominators within the indicators
    indicators = format_numerators_and_denominators(indicators_raw, data_elements, category_option_combos)

    # Fetch and process indicator groups
    indicator_group_sets = fetch_indicator_group_sets(dhis2auth)
    processed_indicator_group_sets = process_indicator_group_sets(indicator_group_sets)

    # Assume 'get_tx_curr_indicator_group' is a function to fetch the 'TX_CURR' indicator group
    tx_curr_indicator_group = get_tx_curr_indicator_group(indicator_groups, 'TX_CURR')

    return export_data_element_groups, data_elements, indicators, processed_indicator_group_sets, tx_curr_indicator_group

def get_month_list():
    """Generate a list of month codes."""
    months = [f'{i:02}' for i in range(1, 13)]
    return months

def get_year_list(start_year=2019):
    """Generate a list of years from a start year to the current year."""
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(start_year, current_year + 1)]
    return years

def generate_month_periods(start_period='201909'):
    """Generate a list of month periods filtered from start_period to current month."""
    months = get_month_list()
    years = get_year_list()
    initial_period_list = [year + month for year in years for month in months]
    current_period = datetime.datetime.now().strftime('%Y%m')
    period_list = [period for period in initial_period_list if start_period <= period < current_period]
    return period_list

def generate_quarter_periods(start_period='2019Q4'):
    """Generate a list of quarter periods filtered from start_period to current quarter."""
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    years = get_year_list()
    initial_quarter_list = [year + quarter for year in years for quarter in quarters]
    current_quarter = datetime.datetime.now().strftime('%YQ') + str((datetime.datetime.now().month - 1) // 3 + 1)
    quarter_list = [period for period in initial_quarter_list if start_period <= period < current_quarter]
    return quarter_list

def generate_scaffold_periods():
    """Create a list of all periods that should have targets, starting from 2020."""
    months = get_month_list()
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    years = get_year_list(start_year=2020)
    scaffold_periods = [year + period for year in years for period in (months + quarters)]
    periods_df = pd.DataFrame(scaffold_periods, columns=['period'])
    periods_df['year'] = periods_df['period'].str[0:4]
    periods_df['type'] = np.where(periods_df['period'].str[4] == 'Q', 'Q', 'M')
    return periods_df

def generate_periods():
    """Generate lists of periods for months, quarters, and a scaffold for targets."""
    month_list = generate_month_periods()
    quarter_list = generate_quarter_periods()
    scaffold_periods = generate_scaffold_periods()
    return month_list, quarter_list, scaffold_periods

