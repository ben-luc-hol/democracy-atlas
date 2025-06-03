import requests
from copy import deepcopy
import json
import zipfile
import io
import geopandas as gpd
import pandas as pd
#from pipelines.utils.s3manager import S3Manager
from datetime import date, datetime as dt
import tempfile
import os
import json
import os
from pathlib import Path


class NorwayCollector:
    def __init__(self):
        pass

class StatisticsNorwayCollector:
    L1_FILTER = "vs:ValgdistrikterMedBergen"
    L2_FILTER = "vs:KommunValg"

    def __init__(self):
        pass

    @classmethod
    def get_mappings(cls, year, to_cloud=False):

        l1a_endpoint = '104'
        l1a_type = "administrative county"
        l1a_mappings = cls.call_api_for_mappings(l1_endpoint=l1a_endpoint, year=year)

        if year <= 2019:
            l1b_endpoint = '104'  # Parliamentary = Administrative counties
            l1b_type = "parliamentary county (same as administrative)"
        else:
            l1b_endpoint = '543'  # Parliamentary = Electoral districts
            l1b_type = "electoral district"

        if l1b_endpoint == "104":
            l1b_mappings = deepcopy(l1a_mappings)
        else:
            l1b_mappings = cls.call_api_for_mappings(l1b_endpoint, year)


        unit_changes_l1a = cls.get_admin_unit_changes(year=year, level_1a=True)
        unit_changes_l1b = cls.get_admin_unit_changes(year=year, level_1a=False)


        l1a_mappings['level_1_type_code'] = "1A"
        l1a_mappings['level_1_type_name'] = l1a_type
        l1a_mappings['unit_changes'] = unit_changes_l1a


        l1b_mappings['level_1_type_code'] = "1B"
        l1b_mappings['level_1_type_name'] = l1b_type
        l1b_mappings['unit_changes'] = unit_changes_l1b

        if to_cloud:
            print("Cloud storage not yet implemented")

        cls.save_locally(l1a_mappings, data_type="mappings", level=None)
        cls.save_locally(l1b_mappings, data_type="mappings", level=None)
        print(f"Mappings for {year} saved successfully")

        return l1a_mappings, l1b_mappings


    @classmethod
    def call_api_for_mappings(cls, l1_endpoint, year):

        l2_endpoint = '131'

        url = f'https://data.ssb.no/api/klass/v1/classifications/{l1_endpoint}/correspondsAt?targetClassificationId={l2_endpoint}&date={year}-04-01'

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
            "level_1_type_code": None,
            "level_1_type_name": None,
            "metadata": {
                "source": "SSB",
                "retrieved_at": date.today().isoformat(),
                "year": year,
            },
            "unit_mappings": list(grouped_dict.values()),
            "unit_changes": None
        }
        return mappings

    @classmethod
    def get_admin_unit_changes(cls, year, level_1a = True):

        l2_endpoint = '131'
        l2_changes = cls.call_api_for_unit_changes(endpoint=l2_endpoint, year=year)

        if level_1a or year <= 2019:
            l1_endpoint = '104'
            l1_changes = cls.call_api_for_unit_changes(endpoint=l1_endpoint, year=year)
        elif year == 2020:
            l1_changes = cls.level_1b_transition()
        else:
            l1_endpoint = '543'
            l1_changes = cls.call_api_for_unit_changes(endpoint=l1_endpoint, year=year)

        unit_changes = {
            "level_1_unit_changes": l1_changes,
            "level_2_unit_changes": l2_changes
        }

        return unit_changes

    @classmethod
    def level_1b_transition(cls):
        units19 = cls.call_api_for_mappings(l1_endpoint='104', year=2019)
        new_mappings = []
        for unit in units19['unit_mappings']:
            changed_unit = {
                "old_unit_code": unit['source_unit_code'],
                "old_unit_name": unit['source_unit_name'],
                "new_unit_code": f"{unit['source_unit_code']}",
                "new_unit_name": f"{unit['source_unit_name']} valgdistrikt",
                "unit_change_occurred": "2020-01-01"
            }
            new_mappings.append(changed_unit)
        return new_mappings


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
    def get_result_parliament(cls, year, unit_code, level):

        votes_cast = cls.get_votes_cast_parliament(year, unit_code=unit_code, level=level)
        vote_dist = cls.get_vote_distribution_parliament(year, unit_code=unit_code, level=level)
        turnout = cls.get_turnout(year, unit_code=unit_code, level=level)

        if level == 1:
            unit_level = "1B"
        elif level == 2:
            unit_level = "2A"
        else:
            raise ValueError("Level must be 1 or 2")

        if vote_dist['unit_code'] == votes_cast['unit_code']:

            result = {
                "unit_code": unit_code,
                "unit_name": vote_dist['unit_name'],
                "unit_level": unit_level,
                "date_retrieved": vote_dist['retrieved'],
                "last_updated": vote_dist['last_updated'],
                "valid_votes_cast": votes_cast['total']['valid'],
                "discarded_votes": votes_cast['total']['discarded'],
                "blank_votes": votes_cast['total']['blank'],
                "turnout": turnout,
                "votes_by_type": {
                    "election_day_vote": {
                        "valid": votes_cast['election_day_vote']['valid'],
                        "discarded": votes_cast['election_day_vote']['discarded'],
                        "blank": votes_cast['election_day_vote']['blank']
                    },
                    "early_vote": {
                        "valid": votes_cast['early_vote']['valid'],
                        "discarded": votes_cast['early_vote']['discarded'],
                        "blank": votes_cast['early_vote']['blank']
                    }
                },
                "results": vote_dist['vote_distribution']
            }

            if level == 1:
                seat_dist = cls.get_seat_distribution(year=year, unit_code=unit_code)
                result['seat_distribution'] = seat_dist['seat_distribution']

            return result

    @classmethod
    def get_votes_cast_parliament(cls, year, unit_code, level):

        if level == 1:
            filter = cls.L1_FILTER
            code = (f"v{unit_code}" if year < 2020 else unit_code)
        elif level == 2:
            filter = cls.L2_FILTER
            code = unit_code
        else:
            raise ValueError("Unit code must be 1 or 2")

        vote_count_url = "https://data.ssb.no/api/v0/no/table/11691/"
        vote_count_post = { "query": [
                    {
                      "code": "Region",
                      "selection": {
                        "filter": filter,
                        "values": [
                          code
                        ]
                      }
                    },
                    {
                      "code": "StemmeGyldigNyn",
                      "selection": {
                        "filter": "item",
                        "values": [
                          "1N",
                          "2N",
                          "3N"
                        ]
                      }
                    },
                    {
                      "code": "StemmeTidspktNyn",
                      "selection": {
                        "filter": "item",
                        "values": [
                          "1N",
                          "2N"
                        ]
                      }
                    },
                    {
                      "code": "Tid",
                      "selection": {
                        "filter": "item",
                        "values": [
                          f"{year}"
                        ]
                      }
                    }
                  ],
                  "response": {
                    "format": "json-stat2"
                  }
                }

        r = requests.post(vote_count_url, json=vote_count_post)
        r_votes = json.loads(r.content)

        values = r_votes['value']

        result = {
            "unit_code": unit_code,
            "unit_name": r_votes['dimension']['Region']['category']['label'][f'{code}'],
            "retrieved": date.today().isoformat(),
            "last_updated": r_votes['updated'][:10],
            "total": {
                "valid": values[0] + values[1],
                "discarded": values[2] + values[3],
                "blank": values[4] + values[5]
            },
            "election_day_vote": {
                "valid": values[0],
                "discarded": values[2],
                "blank": values[4]
            },
            "early_vote": {
                "valid": values[1],
                "discarded": values[3],
                "blank": values[5]
            }
        }

        return result

    @classmethod
    def get_vote_distribution_parliament(cls, year, unit_code, level):

        if level == 1:
            filter = cls.L1_FILTER
            code = (f"v{unit_code}" if year < 2020 else unit_code)
        elif level == 2:
            filter = cls.L2_FILTER
            code = unit_code
        else:
            raise ValueError("Unit code must be 1 or 2")

        url = "https://data.ssb.no/api/v0/no/table/08092/"

        post = {"query":
                [
                    {
                        "code": "Region",
                        "selection": {
                            "filter": filter,
                            "values": [code]
                        }
                    },
                    {
                        "code": "ContentsCode",
                        "selection": {
                            "filter": "item",
                            "values": ["Godkjente1"]
                        }
                    },
                    {
                        "code": "Tid",
                        "selection": {
                            "filter": "item",
                            "values": [f"{year}"]
                        }
                    }
                ],
                "response": {
                    "format": "json-stat2"
                }
        }
        r = requests.post(url, json=post)
        r_votes = json.loads(r.content)

        unit_name = r_votes['dimension']['Region']['category']['label'][code]

        party_categories = r_votes['dimension']['PolitParti']['category']
        party_labels = party_categories['label']
        party_index = party_categories['index']

        values = r_votes['value']

        vote_distribution = []

        for party_code, party_name in party_labels.items():
            index_pos = party_index[party_code]
            votes = values[index_pos]
            if votes is not None:
                vote_distribution.append({
                    "party_code": party_code,
                    "party_name": party_name,
                    "votes": votes
                })

        result = {
            'unit_code': unit_code,
            'unit_name': unit_name,
            'retrieved': date.today().isoformat(),
            'last_updated': r_votes['updated'][:10],
            'vote_distribution': vote_distribution
        }

        return result

    @classmethod
    def get_turnout(cls, year, unit_code, level):

        if level == 1:
            filter = cls.L1_FILTER
            code = (f"v{unit_code}" if year < 2020 else unit_code)
        elif level == 2:
            filter = cls.L2_FILTER
            code = unit_code
        else:
            raise ValueError("Unit code must be 1 or 2")

        post = {
          "query": [
            {
              "code": "Region",
              "selection": {
                "filter": filter,
                "values": [
                  code
                ]
              }
            },
            {
              "code": "Tid",
              "selection": {
                "filter": "item",
                "values": [
                  f"{year}"
                ]
              }
            }
          ],
          "response": {
            "format": "json-stat2"
          }
        }

        url = "https://data.ssb.no/api/v0/no/table/08243/"
        r = requests.post(url, json=post)
        r_turnout = json.loads(r.content)
        result = r_turnout['value'][0]/100

        return result

    @classmethod
    def get_seat_distribution(cls, year, unit_code):

        code = (f"v{unit_code}" if year < 2020 else unit_code)

        post = {
          "query": [
            {
              "code": "Region",
              "selection": {
                "filter": cls.L1_FILTER,
                "values": [
                  code
                ]
              }
            },
            {
              "code": "PolitParti",
              "selection": {
                "filter": "item",
                "values": [
                  "01","02","03","04","08","55","05","06","07","100","130","150","75","29","71","54","122","12","74","46","76",
                  "56","13","16","131","25","151","125","126","24","17","15","154","132","18","26","152","19","48","77","44",
                  "133","20","134","135","78","09","136","57","49","58","73","123","153","155","10","124","156","28","11","33",
                  "21","22","60","59","72","47","70","79","14","127","83","27","137","61","90","90a","90b","90c","90d","90e",
                  "90f","90g","90h","91","92"
                ]
              }
            },
            {
              "code": "Tid",
              "selection": {
                "filter": "item",
                "values": [
                  f"{year}"
                ]
              }
            }
          ],
          "response": {
            "format": "json-stat2"
          }
        }
        url = "https://data.ssb.no/api/v0/no/table/08219/"
        r = requests.post(url, json=post)
        r_seats= json.loads(r.content)

        unit_name = r_seats['dimension']['Region']['category']['label'][code]

        party_categories = r_seats['dimension']['PolitParti']['category']
        party_labels = party_categories['label']
        party_index = party_categories['index']

        values = r_seats['value']

        seat_distribution = []

        for party_code, party_name in party_labels.items():
            index_pos = party_index[party_code]
            seats = values[index_pos]
            if seats > 0:
                seat_distribution.append({
                    "party_code": party_code,
                    "party_name": party_name,
                    "seats": seats
                })

        result = {
            'unit_code': unit_code,
            'unit_name': unit_name,
            'retrieved': date.today().isoformat(),
            'last_updated': r_seats['updated'][:10],
            'seat_distribution': seat_distribution
        }

        return result

    @classmethod
    def save_locally(cls, data, data_type="mappings", level=None):

        base_path = Path("/Users/ben-holden-artifex/Desktop/electoral-explorer/data/raw/nor")

        if data_type == "mappings":
            # Structure: data/raw/nor/mappings/{level}/{year}.json
            level_dir = data.get('level_1_type_code', level)
            save_path = base_path / "mappings" / level_dir
            filename = f"{data['metadata']['year']}.json"

        elif data_type == "results":
            # Structure: data/raw/nor/results/{level}/{year}/{unit_code}.json
            level_dir = data['unit_level']
            year = data.get('year', 2021)
            save_path = base_path / "results" / level_dir / str(year)
            filename = f"{data['unit_code']}.json"

        else:
            raise ValueError("data_type must be 'mappings' or 'results'")

        save_path.mkdir(parents=True, exist_ok=True)

        file_path = save_path / filename

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"💾 Saved {data_type} to: {file_path}")
        return file_path

    @classmethod
    def load_locally(cls, data_type, year, level=None, unit_code=None):

        base_path = Path("data/raw/nor")

        if data_type == "mappings":
            file_path = base_path / "mappings" / level / f"{year}.json"
        elif data_type == "results":
            file_path = base_path / "results" / level / str(year) / f"{unit_code}.json"
        else:
            raise ValueError("data_type must be 'mappings' or 'results'")

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)


    @classmethod
    def run_results(cls, year, to_cloud=False):
        pass


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3 and sys.argv[1] == "mappings":
        year = int(sys.argv[2])
        StatisticsNorwayCollector.get_mappings(year)
    else:
        print("Usage: python norway_data_collection.py mappings <year>")
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

