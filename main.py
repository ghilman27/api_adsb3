import numpy as np
import pandas as pd
import uvicorn

from fastapi import FastAPI, Query
from typing import Optional, List
from urllib.parse import unquote

class DKIEducationData:
    """
    Class of data access layer protocol
    responsible for control in accessing and processing data,
    connecting controller to the database (pandas dataframe)
    """
    def __init__(self):
        self.path = 'data-dki-menurut-pendidikan-tahun-2014.csv'
        self.__open()
        self.__clean()

    def __open(self):
        self.df = pd.read_csv(self.path)

    def __clean(self):
        """
        For cleaning data:
        removing 'tahun' column (because of uniform value), 
        filling null values with zeros,
        converting string number into integer
        """
        if self.df is None:
            return
        self.df.drop(['tahun'], axis=1, inplace=True)
        self.df.fillna(0, inplace=True)
        cols = list(self.df.select_dtypes(exclude=['object']).columns)
        self.df[cols] = self.df[cols].applymap(np.int64)
    
    def select_all(self):
        return self.df.to_dict('records')

    def get_column_names(self, from_='nama_provinsi', until='strata_III'):
        return list(self.df.loc[:, from_:until].columns)

    def summary_total_by(self, groups, filters=[]):
        """
        Aggregator to get total students from all educational backgrounds
        based on specified groups and filters
        :Args:
        - groups: list of string of groups for aggregation
        - filters: list of tuples [(column_name, filter_value), ....]

        Example
        ------- 
        >>> '''Get total students from all backgrounds from all districts in north jakarta'''
        >>> summary_total_by('nama_kecamatan', filters=[('nama_kabupaten/kota', 'JAKARTA UTARA')])
        """
        if len(filters) == 0:
            data = (self.df
                    .groupby(groups)
                    .sum()
                    .reset_index()
                    .to_dict('records'))
        else:
            filtered_data = self.df.query(
                ' | '.join([
                    f'`{column}` == "{value}"' for column, value in filters 
                ])
            )
            data = (filtered_data
                    .groupby(groups)
                    .sum()
                    .reset_index()
                    .to_dict('records'))
        return data.pop() if len(data) == 1 else data   

class Controller:
    """
    Controller class for connecting each api endpoint with data access layer
    """
    def __init__(self):
        self.data = DKIEducationData()
    
    def display_raw(self):
        return self.data.select_all()

    def summary(self):
        return self.data.summary_total_by('nama_provinsi')

    def summary_cities(self):
        groups = self.data.get_column_names(until='nama_kabupaten/kota')
        return self.data.summary_total_by(groups)

    def summary_districts(self, cities=[]):
        """
        Summary per all districts,
        can be filtered by city
        example: Get total students from all backgrounds from all districts in north jakarta
        """
        groups = self.data.get_column_names(until='nama_kecamatan')
        filters = [('nama_kabupaten/kota', unquote(city)) for city in cities]
        return self.data.summary_total_by(groups, filters=filters)

    def summary_sub_districts(self, cities=[], districts=[]):
        groups = self.data.get_column_names(until='nama_kelurahan')
        filters = [('nama_kabupaten/kota', unquote(city)) for city in cities]
        filters.extend([('nama_kecamatan', unquote(district)) for district in districts])
        return self.data.summary_total_by(groups, filters=filters)

    def summary_city(self, city):
        return self.data.summary_total_by(
            groups='nama_kabupaten/kota', 
            filters=[('nama_kabupaten/kota', unquote(city))]
        )

    def summary_district(self, district):
        return self.data.summary_total_by(
            groups='nama_kecamatan', 
            filters=[('nama_kecamatan', unquote(district))]
        )

    def summary_sub_district(self, sub_district):
        return self.data.summary_total_by(
            groups='nama_kelurahan', 
            filters=[('nama_kelurahan', unquote(sub_district))]
        )

app = FastAPI()
controller = Controller()

@app.get('/')
async def home():
    return {
        'message': 'Welcome to DKI Education Data API',
        'instruction': 'try some endpoints',
        'endpoints': {
            '/docs': 'API documentation',
            '/raw': 'display raw data',
            '/summary': 'display summary data for the whole jakarta',
            '/cities': 'display summary data per city',
            '/districts': 'display summary data per district',
            '/subdistricts': 'display summary data per subdistrict',
            '/city': 'display individual summary data per city',
            '/district': 'display individual summary data per district',
            '/subdistrict': 'display individual summary data per subdistrict',
        }
    }

@app.get('/raw')
async def display_raw():
    """
    display raw data
    """
    return controller.display_raw()

@app.get('/summary')
async def summary():
    """
    display summary data for the whole jakarta
    """
    return controller.summary()

@app.get('/cities')
async def summary_cities():
    """
    display summary data per cities in jakarta
    """
    return controller.summary_cities()

@app.get('/districts')
async def summary_districts(
    cities: Optional[List[str]] = Query(
        [],
        title='City',
        alias='city',
        description='Query for filtering districts by city',
        min_length=3,
    ),
):
    """
    display summary data per districts
    can be filtered by cities (in query parameters)
    """
    return controller.summary_districts(cities)

@app.get('/subdistricts')
async def summary_sub_districts(
    cities: Optional[List[str]] = Query(
        [],
        title='City',
        alias='city',
        description='Query for filtering sub districts by city',
        min_length=3,
    ),
    districts: Optional[List[str]] = Query(
        [],
        title='District',
        alias='district',
        description='Query for filtering sub districts by district',
        min_length=3,
    ),
):
    """
    display summary data per sub districts
    can be filtered by cities and districts (in query parameters)
    """
    return controller.summary_sub_districts(cities, districts)

@app.get('/city/{city}')
async def summary_city(city: str):
    """
    display summary for individual city
    """
    return controller.summary_city(city)

@app.get('/district/{district}')
async def summary_district(district: str):
    """
    display summary for individual district
    """
    return controller.summary_district(district)

@app.get('/subdistrict/{sub_district}')
async def summary_sub_district(sub_district: str):
    """
    display summary for individual sub district
    """
    return controller.summary_sub_district(sub_district)
