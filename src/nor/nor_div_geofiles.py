import requests
import json
import zipfile
import io
import geopandas as gpd
import pandas as pd
from src.utils.s3manager import S3Manager
from datetime import date, datetime as dt
import tempfile
import os

class NorGeoProcessor:
    """
    Class to identify, map, ingest, process, and store geographic data for Norway.

    Administrative division levels:
    - Counties ("Fylker"):
        * 1st level administrative units for local elections and public administration.
        * Level code = "1A"
    - Electoral Districts ("Valgdistrikt"):
        * 1st level administrative units for national election purposes
        * Historically the same as Level "1A" until the 2021 national election
        * Level code = "1B"
    - Municipalities ("Kommuner"):
        * 2nd level administrative units.
    - Precincts ("Valgkretser"):
        * 3rd level administrative units for public elections, where available.

    Boundaries and makeup of administrative divisions have changed over time.
    The class treats each year separately and uses a SCD Type II table to map the historical changes in administrative unit relationships at levels 1 and 2.

    DATA SOURCES:
        * SSB (Statistics Bureau):
            - API endpoint for geographic unit code data from 1945 to current year
            - API endpoint used for continuity and correspondence tables

        * Kartverket (Mapping Authority):
            - Source for geographic data from 1997 to the current year
            - Files from 2019 to current year are available as straight GeoJSON files
            - Files from 1997 - 2018 are available as single SOSI file (unique format for Norway) that's handled accordingly.

    DATA PROCESSING:
        *Raw Data
            *Geographic unit mappings
            *GeoJSON files

        *Processed Data
            *SCD Type II
            *Geographic Units (parquet)
            *TopoJSON files
            *Views for front-end

    DATA STORAGE:
        *AWS S3
    """
    def __init__(self, year):
        self.year = year
        self.configure_s3()

    def configure_s3(self):
        self.s3 = S3Manager(bucket="election-atlas", region="us-east-1")
        self.raw = "raw/country=Norway/"
        self.geodata = "geodata/country=Norway/"
        self.dimensions = "dimensions/country=Norway/"

    def get_region_mappings(self, year=None):
        """
        Method to call SSB APIs and generate mapping dictionaries of the administrative
        divisions that are valid for that particular year.

            * Level 1A dictionary mapping counties to their respective constituent municipalities
            * Level 1B dictionary mapping electoral districts to their respective constituent municipalities
            * Level 2A dictionary mapping municipalities to their respective constituent precincts

        Generates and stores JSON data for administrative divisions valid for that year.

        :param year: year to get subdivisions for, defaults to 'self.year' if None
        :returns list(dict): [level 1A dict, level 1B dict, level 2A dict]
        """

        if not year:
            year = self.year

        key = f"raw/country=nor/year={year}/geography/L1_to_L2_keymap.json"

        subdivision_codes = self.get_subdivision_codes(year=year)

        data = {
            "metadata": {
                "source": "SSB",
                "retrieved_at": date.today().isoformat(),
                "year":year
            },
            "subdivisions": subdivision_codes
        }

        self.s3.write_json(
            data = data,
            key = key,
            indent= 2
        )
        return data

    @classmethod
    def get_subdivision_codes(self, year):
        """
        Gets subdivision codes in use for the specified year from SSB
        :param year: year to get subdivisions for, defaults to 'self.year' if None
        :param level: level code to get subdivisions for ('1A', '1B', '2A')
        :return:
        """

        level_2_endpoint = '131'

        if year > 2020:
            level_1_endpoint = '543'
        else:
            level_1_endpoint = '104'

        url = f'https://data.ssb.no/api/klass/v1/classifications/{level_1_endpoint}/correspondsAt?targetClassificationId={level_2_endpoint}&date={year}-03-01'

        response = requests.get(url)
        r = json.loads(response.content)
        data = r['correspondenceItems']


        level_1_dict = {}

        for entry in data:
            level_1_code = entry['sourceCode']
            level_1_name = entry['sourceName']
            level_2_code = entry['targetCode']
            level_2_name = entry['targetName']

            if level_1_code not in level_1_dict:
                level_1_dict[level_1_code] = {
                    'year': self.year,
                    'level_1_code': level_1_code,
                    'level_1_name': level_1_name,
                    'constituents': []
                }

            level_1_dict[level_1_code]['constituents'].append({
                'level_2_code': level_2_code,
                'level_2_name': level_2_name
            })
        return list(level_1_dict.values())


    def get_geodata(self, year=None, keymap: dict = None):
        """
        Method to get and process GeoJSON data from Norway's mapping authority valid during the specified year.
        Stores geoJSON data in S3.

        :param year: year for which to get subdivision data. Defaults to 'self.year' if None
        :return:
        """
        if not year:
            year = self.year

        if not keymap:
            keymap = self.s3.read_json(
                key=f"raw/country=nor/year={year}/subdivision_keymap.json"
            )

        zip_url, file_type = self.get_direct_zip_url(year=year)

        if file_type == "SOSI":
            gdf_all_L2 = self.L2_gdf_from_sosi(zip_url)
        elif file_type == "GeoJson":
            gdf_all_L2 = self.L2_gdf_from_geojson(zip_url)
        else:
            raise ValueError(f"Invalid file type: {file_type}")

        gdfs_single_L1 = []
        for L1_region in keymap.keys()
            gdf_filtered_L2 = self.process_individual_L2(L1_region, gdf_all_L2)
            gdf_single_L1 = self.build_L1(L1_region, gdf_filtered_L2)
            gdfs_single_L1.append(gdf_single_L1)
        gdf_all_L1 = pd.concat(gdfs_single_L1)

        self.consolidate_L1(gdf_all_L1)
        self.consolidate_L2(gdf_all_L2)

        level_1_gdfs = self.process_individual_L2(level_2_gdf)
        self.process_individual_L1(level_1_gdfs)
        self.process_consolidated_L
    def process_individual_L2(self, gdf):
        pass



    def L2_gdf_from_geojson(self, zip_url: str):
        """
        Processes a GeoJSON for a particular year from .zip file url to output
        Returns a processed GeoDataFrame containing all Level 2 subdivisions
        :param zip_url:
        :return: GeoDataFrame
        """
        zip_bytes = requests.get(zip_url).content
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            name = [n for n in zf.namelist() if n.lower().endswith('.geojson')][0]
            with zf.open(name) as f:
                gdf = gpd.read_file(f, layer="Kommune")

            gdf = gdf.rename(
                columns={
                    "oppdateringsdato": "latest_update",
                    "datauttaksdato": "retrieved_at",
                    "kommunenummer": "level_2_code",
                    "kommunenavn": "level_2_name",
                    "gyldigFra": "valid_from",
                    "gyldigTil": "valid_to"
                }
            )
            gdf['level_1_code'] = None
            gdf['retrieved_at'] = gdf['retrieved_at'].dt.strftime('%Y-%m-%d')
            gdf['latest_update'] = gdf['latest_update'].dt.strftime('%Y-%m-%d')
            gdf['valid_from'] = pd.to_datetime(gdf['valid_from'], format='%Y%m%d', errors='coerce').dt.strftime(
                '%Y-%m-%d')
            gdf['valid_to'] = pd.to_datetime(gdf['valid_to'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')

            gdf = gdf[['level_2_code', 'level_2_name', 'level_1_code', 'retrieved_at', 'latest_update', 'valid_from',
                       'valid_to', 'geometry']].copy()
            return gdf

    def L2_gdf_from_sosi(self, zip_url: str):
        pass


    def process_geofiles(self, year=None):
        """
        Method to get and process GeoJSON data from Norway's mapping authority valid during the specified year.
        Stores geoJSON data in S3.

        :param year: year for which to get subdivision data. Defaults to 'self.year' if None
        :return:
        """
        # if not year:
        #     year = self.year
        #
        # zip_url, file_type = self.get_direct_zip_url(year=year)
        #
        # zip_bytes = requests.get(zip_url).content
        #
        # region_keymap = self.s3.read_json(
        #     key = f"raw/country=nor/year={year}/subdivision_keymap.json"
        # )

        # if file_type == "GeoJSON":
        #     with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        #         name = [n for n in zf.namelist() if n.lower().endswith('.geojson')][0]
        #         with zf.open(name) as f:
        #             gdf = gpd.read_file(f, layer="Kommune")
        #
        #         gdf = gdf.rename(
        #                 columns={
        #                     "oppdateringsdato":"latest_update",
        #                     "datauttaksdato":"retrieved_at",
        #                     "kommunenummer":"level_2_code",
        #                     "kommunenavn":"level_2_name",
        #                     "gyldigFra":"valid_from",
        #                     "gyldigTil":"valid_to"
        #                 }
        #             )
        #         gdf['level_1_code'] = None
        #         gdf['retrieved_at'] = gdf['retrieved_at'].dt.strftime('%Y-%m-%d')
        #         gdf['latest_update'] = gdf['latest_update'].dt.strftime('%Y-%m-%d')
        #         gdf['valid_from'] = pd.to_datetime(gdf['valid_from'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        #         gdf['valid_to'] = pd.to_datetime(gdf['valid_to'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        #
        #         gdf = gdf[['level_2_code', 'level_2_name', 'level_1_code', 'retrieved_at', 'latest_update', 'valid_from', 'valid_to', 'geometry']].copy()

            region_gdfs = []

            for region in region_keymap:

                district_gdfs = []

                level_1_code = region['level_1_code']
                level_1_name = region['level_1_name']

                for district in region['constituents']:
                    level_2_code = district['level_2_code']
                    level_1_code = region['level_1_code']
                    filtered_gdf = gdf[gdf['level_2_code'] == level_2_code].copy()
                    filtered_gdf['level_1_code'] = level_1_code

                    if not filtered_gdf.empty:

                        geojson_dict = json.loads(filtered_gdf.to_json())

                        path = f"shapefiles/country=nor/year={year}/level=2/{level_2_code}"
                        self.s3.write_json(data=geojson_dict,key=path, indent=2)

                        district_gdfs.append(geojson_dict)

               if district_gdfs:
                    combined_gdf = gpd.GeoDataFrame.from_dict(district_gdfs)
                    unified_geo = combined_gdf.geometry.unary_union

                    all_level_2_gdf = pd.concat(district_gdfs, ignore_index=True)
                    simplified_l2_gdf = all_level_2_gdf.copy()
                    simplified_l2_gdf['geometry'] = simplified_l2_gdf.geometry.simplify(0.001, preserve_topology=True)
                    simplified_geojson = json.loads(simplified_l2_gdf.to_json())

                    path = f"views/country=nor/year={year}/level=2/simplified.geojson"
                    self.s3.write_json(data=simplified_geojson, key=path, indent=2)


                    level_1_data = {
                        'level_1_code': level_1_code,
                        'level_1_name': level_1_name,
                        'retrieved_at': combined_gdf['retrieved_at'].iloc[0],
                        'num_municipalities': len(district_gdfs),
                        'geometry': unified_geo
                    }

                    level_1_gdf = gpd.GeoDataFrame([level_1_data], geometry='geometry', crs=gdf.crs)
                    level_1_geojson = json.loads(level_1_gdf.to_json())

                    path = f"shapefiles/country=nor/level=1/{level_1_code}.geojson"
                    self.s3.write_json(data=level_1_geojson,key=path, indent=2)
                    region_gdfs.append(level_1_gdf)

                    simplified_district_gdf = level_1_gdf.copy()
                    simplified_district_gdf['geometry'] = simplified_district_gdf.geometry.simplify(0.001, preserve_topology=True)
                    simplified_district_geojson = json.loads(simplified_district_gdf.to_json())
                    path = f"shapefiles/country=nor/year={year}/level=1/simplified/{level_1_code}.geojson"

            if region_gdfs:
                all_level_1 = pd.concat(region_gdfs, ignore_index=True)
                all_level_1_geojson = json.loads(all_level_1.to_json())
                path = f"shapefiles/country=nor/year={year}/level=1/simplified.geojson"
                self.s3.write_json(data=all_level_1_geojson, key = path, indent=2)


            return gdf

    def create_topojson_file(self, year):
        """Create consolidated TopoJSON file for web display"""
        import tempfile
        import subprocess

        # Create temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Path for GeoJSON files
            l1_path = os.path.join(tmpdir, "level_1.geojson")
            l2_path = os.path.join(tmpdir, "level_2.geojson")

            # Output path
            topo_path = os.path.join(tmpdir, "nor.topojson")

            # Get consolidated GeoJSON files from S3
            l1_geojson = self.s3.read_json(f"shapefiles/country=nor/year={year}/consolidated/level_1.geojson")
            l2_geojson = self.s3.read_json(f"shapefiles/country=nor/year={year}/consolidated/level_2.geojson")

            # Write to temp files
            with open(l1_path, 'w') as f:
                json.dump(l1_geojson, f)
            with open(l2_path, 'w') as f:
                json.dump(l2_geojson, f)

            # Run topojson command (requires Node.js and topojson-server)
            subprocess.run([
                'npx', 'topojson-server',
                '--out', topo_path,
                '--properties',  # Preserve properties
                f'counties={l1_path}',
                f'municipalities={l2_path}'
            ], check=True)

            # Read the TopoJSON file
            with open(topo_path, 'r') as f:
                topojson_data = json.load(f)

            # Save to S3
            web_path = f"views/country=nor/year={year}/nor.topojson"
            self.s3.write_json(data=topojson_data, key=web_path)

    def level_2_to_dict(geo_df, level_2_code):
        """
        Extract a single municipality  and convert it to a dictionary

        :param: geo_df: geodataframe containing level 2 data
        :param level_2_code:  level 2 code
        :returns dict:  dictionary representation of the municipality
        """
        # Filter for the specific municipality
        filtered_gdf = geo_df[geo_df['kommunenummer'] == level_2_code].copy()

        if filtered_gdf.empty:
            return None

        # Get the first (should be only) row
        row = filtered_gdf.iloc[0]

        # Convert to dictionary, handling the geometry specially
        result = {col: row[col] for col in filtered_gdf.columns if col != 'geometry'}

        from shapely.geometry import mapping
        result['geometry'] = mapping(row['geometry'])

        return result

    def get_direct_zip_url(self, year):

        CURRENT_YEAR = date.today().year
        GEOJSON_START_AT_SOURCE = 2019
        HISTORICAL_START = 1997

        if year == CURRENT_YEAR:
            return f"https://nedlasting.geonorge.no/geonorge/Basisdata/Kommuner/GEOJSON/Basisdata_0000_Norge_4258_Kommuner_GEOJSON.zip", "GeoJSON"

        elif year >= GEOJSON_START_AT_SOURCE:
            return f"https://nedlasting.geonorge.no/geonorge/Basisdata/Kommuner{year}/GEOJSON/Basisdata_0000_Norge_4258_Kommuner{year}_GEOJSON.zip", "GeoJSON"

        elif year >= HISTORICAL_START:
            # SOSI format for historical data (1997-2018)
            return f"https://nedlasting.geonorge.no/geonorge/Basisdata/AdministrativeEnheter{year}/SOSI/Basisdata_0000_Norge_25833_AdministrativeEnheter{year}_SOSI.zip", "SOSI"
        else:
            # Fallback to 1997 for earlier years
            return f"https://nedlasting.geonorge.no/geonorge/Basisdata/AdministrativeEnheter1997/SOSI/Basisdata_0000_Norge_25833_AdministrativeEnheter1997_SOSI.zip", "SOSI"

    # def get_geojson(self, feed, l2_code):
    #     """
    #        Gets GeoJSON file for a single level 2 subdivision
    #        :param feed:
    #        :param l2_code:
    #        :return:
    #        """
    #     entry = [e for e in feed if e['l2_code'] == l2_code][0]
    #     zip_url = entry['url']
    #
    #     gdf = gdf.rename(
    #         columns={
    #             "oppdateringsdato": "last_updated",
    #             "datauttaksdato": "request_date",
    #             "kommunenummer": "l2_code",
    #             "gyldigFra": "valid_from",
    #             "gyldigTil": "valid_to"
    #         })
    #
    #     return gdf


    # def get_correspondence(self, ):

#
# def run_all_level1(self):
#     ssb_entries = self.get_subdivision_codes()
#     grouped_entries = defaultdict(list)
#     for entry in ssb_entries:
#         grouped_entries[entry['level_1_code']].append(entry)
#
#     for l1_code, items in grouped_entries.items():
#         l1_name = items[0]['level_1_name']
#         l2_codes = [e["level_2_code"] for e in items]
#         self.process_level1(l2_codes=l2_codes, level1_code=l1_code, level1_name=l1_name)
#
#
# def process_level1(self, l2_codes, level1_code, level1_name):
#     gdfs = []
#
#     if self.download_type == "GeoJSON":
#         feed = self.get_map_xml_feed()
#
#     for l2_code in l2_codes:
#         gdf = self.get_geojson(feed=feed, l2_code=l2_code)
#
#         # if l2_code == gdf['l2_code']:
#         #     continue
#         # else:
#         #     raise Exception("L2")
#         gdf['year'] = self.year
#         gdf['code_match'] = gdf['l2_code'] == l2_code
#         gdf['l2_code'] = l2_code
#         gdf['l1_code'] = level1_code
#         gdf['l1_name'] = level1_name
#         gdf['admin_level'] = 2
#
#         gdf = gdf[['l2_code', 'l2_name', 'admin_level', 'year', 'code_match', 'l1_code', 'l1_name', 'last_updated',
#                    'valid_from', 'valid_to', 'geometry']].copy()
#
#         key = f"{self.shapefiles}/year={self.year}/level=2/{l2_code}.geojson"
#
#         self.s3.put_json(key, gdf.to_json())
#
#         gdfs.append(gdf)
#

#
#
#     def process_level_one_unit(self, level_1_code, national_file):
#         pass
#
#
#
#
#
#
#
#
#
#
#
#
#
#



 # def process_level_1(self):
 #     pass
 #

            ###

        #full_l2_gdf = pd.concat(gdfs, ignore_index=True)
        #gdf = full_l1_gdf
    #
    #
