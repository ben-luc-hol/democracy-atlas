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

class NorResultsParliament:
    L1_FILTER = "vs:ValgdistrikterMedBergen"
    L2_FILTER = "vs:KommunValg"

    def __init__(self):
        pass
    #u
    #ut
    #implement retry logic; if the response is good from the api
    @classmethod
    def get_result(cls, year, unit_code, level):

        #try except / raise for status
        votes_cast = cls.get_sum_votes(year, unit_code=unit_code, level=level)
        # try except / raise for status
        vote_dist = cls.get_dist_votes(year, unit_code=unit_code, level=level)
        # try except / raise for status
        turnout = cls.get_turnout(year, unit_code=unit_code, level=level)

        if level == 1:
            unit_level = "1b"
        elif level == 2:
            unit_level = "2"
        else:
            raise ValueError("Level must be 1 or 2")

        if vote_dist['unit_code'] == votes_cast['unit_code']:

            full_result = {
                "year": year,
                "election_type":"parliamentary",
                "unit_code": unit_code,
                "unit_name": vote_dist['unit_name'],
                "level_code": unit_level,
                "retrieved_on": vote_dist['retrieved'],
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
                seat_dist = cls.get_seats(year=year, unit_code=unit_code)
                full_result['seat_distribution'] = seat_dist['seat_distribution']

            return full_result

    @classmethod
    def get_sum_votes(cls, year, unit_code, level):

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
    def get_dist_votes(cls, year, unit_code, level):
#introduce try except logic here -- account for "u" and "ut" suffixes on unit codes -----
#introduce raise-for-status on API calls and try-except
        if level == 1:
            filter = cls.L1_FILTER
            unit_code = (f"v{unit_code}" if year < 2020 else unit_code)
        elif level == 2:
            filter = cls.L2_FILTER
        else:
            raise ValueError("Unit code must be 1 or 2")

        url = "https://data.ssb.no/api/v0/no/table/08092/"

        post = {"query":
                [
                    {
                        "code": "Region",
                        "selection": {
                            "filter": filter,
                            "values": [unit_code]
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

        unit_name = r_votes['dimension']['Region']['category']['label'][unit_code]

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
    def get_seats(cls, year, unit_code):

        unit_code = (f"v{unit_code}" if year < 2020 else unit_code)

        post = {
          "query": [
            {
              "code": "Region",
              "selection": {
                "filter": cls.L1_FILTER,
                "values": [
                  unit_code
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
    def save_locally(cls, data):

        base_path = Path("/Users/holden-data/Desktop/democracy-atlas/data/raw/nor")

        # Structure: data/raw/nor/results/{level}/{year}/{unit_code}.json
        level_dir = data['unit_level']
        year = data.get('year', 2021)
        save_path = base_path / "results" / level_dir / str(year)
        filename = f"{data['unit_code']}.json"
        save_path.mkdir(parents=True, exist_ok=True)

        file_path = save_path / filename

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"Saved {data['']} to: {file_path}")
        return file_path


    @classmethod
    def run_results(cls, year, to_cloud=False):
        pass


class NorResultsLocal:
    def __init__(self):
        pass

    @classmethod
    def get_result(cls, year, unit_code, level, election_type):
        pass

   @classmethod
    def get_sum_votes(cls, year, unit_code, level, election_type):
        pass

    @classmethod
    def get_dist_votes(cls, year, unit_code, level, election_type):
        pass

    @classmethod
    def get_turnout(cls, year, unit_code, level):
        pass

    @classmethod
    def get_seats(cls, year, unit_code):
        pass

