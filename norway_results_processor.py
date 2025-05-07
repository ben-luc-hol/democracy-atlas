import requests
import json

class ResultsProcessor:
    """
    Class to get and process historical Norwegian election data from
    official source ("valgdirektoratet" - Election directorate)
    """
    pass

    def get_result_at(self, year, geo_level, council=None):
        """
        Calls official results API for results for a specific year, election type, and administrative division
        :param year: election year (YYYY)
        :param geo_level: Geographic aggr level for results. 1 - 4 (1 = national, 2=by county, 3=by municipality, 4=by precinct)
        :param council: If 'year' is a local election year, the type of election must be specified, either "county" or "municipal".
        :return: Raw JSON output w/ official results
        """

        national_election_years = [1973 + 4 * i for i in range(14)]
        local_election_years = [1971 + 4 * i for i in range(14)]

        if year not in [national_election_years, local_election_years]:
            raise ValueError('Enter valid election year')

        if year in national_election_years and not council:
            election_type = "st"
        elif year in local_election_years:
            if council == "municipal":
                election_type = "ko"
            elif council == "municipal":
                election_type = "fy"
            else:
                raise ValueError(
                    'Invalid council value. for off-year elections a value must be specified: "county" or "municipal")'
                )

        geo_level_keymap = {1:'land',
                            2:'fylke',
                            3:'kommune',
                            4:'stemmekrets'}


        if not geo_level in geo_level_keymap:
            raise ValueError('Invalid geo_level: Must specify be 1 (national), 2 (county), 3 (municipality), or 4 (precinct')

        url = f'https://valgresultat.no/api/{year}/totalrapport/{election_type}/partier/{geo_level_keymap[geo_level]}'

        response = requests.get(url)
        result_json = json.loads(response.content)
        return result_json

