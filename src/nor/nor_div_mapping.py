import requests
from copy import deepcopy
import json
import zipfile
import io
import geopandas as gpd
import pandas as pd
#from src.utils.s3manager import S3Manager
from datetime import date, datetime as dt
import tempfile
import os
import json
import os
from pathlib import Path


class StatNorMappings:
    """
    Class to call Statistics Norway (SSB) API for administrative units.

    Terminology:
        *'unit' refers to an individual administrative unit
        *'level' refers to the administrative level of a unit

    Units:
        * 1a - Level 1 units - counties for administrative and local/regional election purposes
        * 1b - Level 1 units - counties for parliamentary election purposes - electoral districts (2020 - current)
        * 2 - Level 2 units - municipalities
        * 3 - Level 3 units - precincts (where available - implementation pending)

    Collection process:
        * Defines API endpoints based on year and unit level - 1b transitions from equivalent to admin. counties to electoral districts in 2020
        * Collects Level 1 -> Level 2 unit code keymap
        * Collects changes in level 1 and level 2 codes, respectively from prior year to the input year
        * Builds keymap dictionaries with parent-to-child relationships between units
        * Appends changes from prior year to respective keymap dictionaries
        * Persists data to GCP

    Outputs:
        * Level 1a dictionary - dictionary with entries for each unit at level 1a and their constituent level 2 units
        * Level 1b dictionary - dictionary with entries for each unit at level 1b and their constituent level 2 units
        * 2 dictionary - level 2 dictionary with an entry for each level 2  corresponding level 3 unit codes (Pending implementation)

    Purpose:
        * Collects data structures containing valid unit codes and their relationships valid in a specific year
    """
    def __init__(self):
        pass

    @classmethod
    def get_mappings(cls, year, to_cloud=False):
        """
        Method to generate complete keymap dictionaries valid for a single year
        :param year: validity year
        :param to_cloud: bool specifying whether to store locally or persist to cloud
        :return: 1a dict, 1b dict, 2 dict (not yet implemented)
        """

        #get straightforward level 1a keymap:
        lvl_1a_endpoint = '104'
        lvl_2_endpoint = '131'
        # Generate keymap for level 1a and unit changes
        lvl_1a_mappings = cls.call_api_for_mappings(lvl_1_endpoint=lvl_1a_endpoint, year=year)
        lvl_1a_unit_changes = cls.call_api_for_unit_changes(year=year, endpoint=lvl_1a_endpoint)
        lvl_2_unit_changes = cls.call_api_for_unit_changes(year=year, endpoint=lvl_2_endpoint)

        # Populate level codes and name by dictionary - level 1a
        lvl_1a_mappings['level_1_type_name'] = "administrative county"
        lvl_1a_mappings['level_1_type_code'] = "1a"
        lvl_1a_mappings['unit_changes']['level_1_changes'] = lvl_1a_unit_changes
        lvl_1a_mappings['unit_changes']['level_2_changes'] = lvl_2_unit_changes

        # get level 1b keymap:
        if year == 2020:
            lvl_1b_mappings = cls.level_1b_transition() # handles the full processing for level 1b separately

        elif year > 2020:
            lvl_1b_endpoint = "543"
            lvl_1b_mappings = cls.call_api_for_mappings(lvl_1_endpoint=lvl_1b_endpoint, year=year)
            lvl_1b_unit_changes = cls.call_api_for_unit_changes(year=year, endpoint=lvl_1b_endpoint)
            lvl_1b_mappings['level_1_type_name'] = "electoral district"
            lvl_1b_mappings['level_1_type_code'] = "1b"
            lvl_1b_mappings['unit_changes']['level_1_changes'] = lvl_1b_unit_changes
            lvl_1b_mappings['unit_changes']['level_2_changes'] = lvl_2_unit_changes

        else: # if year < 2020
            lvl_1b_mappings = deepcopy(lvl_1a_mappings)
            lvl_1b_mappings['level_1_type_name'] = "electoral county uniform with administrative county"
            lvl_1b_mappings['level_1_type_code'] = "1b"

        if to_cloud:
            print("GCP connection not implemented yet")
        else:
            cls.save_locally(lvl_1a_mappings)
            cls.save_locally(lvl_1b_mappings)
            print(f"Mappings for {year} saved successfully")

        return lvl_1a_mappings, lvl_1b_mappings

    @classmethod
    def level_1b_transition(cls):
        """
        Implementation for 2020 level 1b edge case
        """
        units21 = cls.call_api_for_mappings(lvl_1_endpoint='543', year=2021)
        units19 = cls.call_api_for_mappings(lvl_1_endpoint='104', year=2019)
        lvl_2_unit_changes = cls.call_api_for_unit_changes(endpoint="131", year=2020)
        lvl_1_unit_changes = []
        for unit in units19['unit_mappings']:
            lvl_1b_change = {
                "old_unit_code": unit['source_unit_code'],
                "old_unit_name": unit['source_unit_name'],
                "new_unit_code": f"v{unit['source_unit_code']}",
                "new_unit_name": f"{unit['source_unit_name']} valgdistrikt",
                "unit_change_occurred": "2020-01-01"
            }
            lvl_1_unit_changes.append(lvl_1b_change)
        units21['unit_changes']['level_1_changes'] = lvl_1_unit_changes
        units21['unit_changes']['level_2_changes'] = lvl_2_unit_changes

        return units21

    @classmethod
    def call_api_for_mappings(cls, lvl_1_endpoint, year):

        lvl_2_endpoint = '131'

        url = f'https://data.ssb.no/api/klass/v1/classifications/{lvl_1_endpoint}/correspondsAt?targetClassificationId={lvl_2_endpoint}&date={year}-04-01'

        response = requests.get(url)
        r = json.loads(response.content)
        data = r['correspondenceItems']

        grouped_dict = {}
        #
        for entry in data:

            source_code = entry['sourceCode']
            source_name = entry['sourceName']
            target_code = entry['targetCode']
            target_name = entry['targetName']

            if source_code not in grouped_dict:
                grouped_dict[source_code] = {
                    'source_unit_code': source_code,
                    'source_unit_name': source_name,
                    'target_units': []
                }

            grouped_dict[source_code]['target_units'].append(
                {
                    'target_unit_code': target_code,
                    'target_unit_name': target_name,
                })

        mappings = {
            "level_1_type_code": "",
            "level_1_type_name": "",
            "metadata": {
                "source": "SSB",
                "retrieved_on": date.today().isoformat(),
                "year": year,
            },
            "unit_mappings": list(grouped_dict.values()),
            "unit_changes": {"level_1_changes": [],
                             "level_2_changes": []}
        }
        return mappings

    @classmethod
    def call_api_for_unit_changes(cls, endpoint, year):

        url = f'https://data.ssb.no/api/klass/v1/classifications/{endpoint}/changes?from={year-1}-04-01&to={year}-04-01'

        response = requests.get(url)
        r = json.loads(response.content)
        data = r['codeChanges']

        changed_units = []

        for entry in data if len(data) > 0 else []:

            changes = {
                "old_unit_code": entry['oldCode'],
                "old_unit_name": entry['oldName'],
                "new_unit_code": entry['newCode'],
                "new_unit_name": entry['newName'],
                "unit_change_occurred": entry['changeOccurred']
            }
            changed_units.append(changes)

        return changed_units

    @classmethod
    def to_gcp(cls, data):
        #implement
        pass

    @classmethod
    def save_locally(cls, data):

        base_path = Path("/Users/holden-data/Desktop/democracy-atlas/data/raw/nor/mappings")

        # Structure: data/raw/nor/mappings/{level}/{year}.json
        level_dir = data.get('level_1_type_code')
        save_path = base_path / level_dir
        filename = f"{data['metadata']['year']}.json"

        save_path.mkdir(parents=True, exist_ok=True)

        file_path = save_path / filename

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"Saved to: {file_path}")
        return file_path

if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3 and sys.argv[1] == "mappings":
        year = int(sys.argv[2])
        StatNorMappings.get_mappings(year)
    else:
        print("Usage: python nor_div_mapping.py mappings <year>")
    #
    # # collect social data for year x, level z
    #
    # # store raw data to in s3
    #
    # # run function


## Kartverket API
#
# class NorwayMapsCollector:
#     def __init__(self):
#         pass
#
#     ### Methods
#
#     # retrieve national file for year x
#
#     # convert sosi file to geojson
#
#     # parse national geojson for level 2 units
#
#     # generate national file for year x, level 1
#
#     # store raw data in s3

