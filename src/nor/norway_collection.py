from src.nor.nor_div_mapping import StatNorMappings

# implement full data collection for norway here

class NorwayCollector:
    def __init__(self):
        pass

    def process_mappings(self, start_year, end_year):
        for year in range(start_year, end_year+1):
            StatNorMappings.get_mappings(year, to_cloud=False)

    def process_results(self, year, to_cloud=False):
        pass

