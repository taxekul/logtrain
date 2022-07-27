import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

from google.auth import compute_engine
#from train_reading import TrainReading

import pandas as pd
import pandas_gbq
from datetime import datetime
from dateutil import tz
from google.cloud import bigquery


class TrainManager:
    def __init__(self) -> None:
        # Läs in VT-tågnummer
        with open('train_ids.txt', 'r') as train_file:
            self.train_ids = [train_id.strip() for train_id in train_file.readlines()]
        
        self.columns=(
                'train_id',
                'route_id',
                'active',
                'timestamp',
                'latitude',
                'longitude',
#                'position',
                'speed',
                'direction',
            )
        self.init_df()

        credentials = compute_engine.Credentials()
        pandas_gbq.context.credentials = credentials

        self.records = []

        self.client = None#bigquery.Client()


    def init_df(self):
        self.df = pd.DataFrame(columns=self.columns)
        self.df = self.df.astype(dtype={'train_id': 'string',
                                'route_id': 'string',
                                'active': bool,
                                'timestamp': 'datetime64[ns]',
                                'latitude': float,
                                'longitude': float,
                                #                'position',
                                'speed': float,
                                'direction': float})
    

    def add_to_df(self,reading):
        reading = reading.split(',')
        train_id = reading[14].split('.')[0]

        # Avbryt om fel tåg eller ingen position
        if train_id not in self.train_ids:
            return False
        elif reading[3] == '' or reading[5] == '' or reading[16] == '':
            return False

        else:
            train_id = reading[14].split('.')[0]
            route_id = reading[16].split('.')[0]
            active = True if reading[2] == 'A' else False

            dt_format = '%d%m%y%H%M%S.%f'
            timestamp = datetime.strptime(reading[9] + reading[1], dt_format)
            timestamp=pd.Timestamp(timestamp)
#            timestamp = timestamp.replace(tzinfo=tz.gettz('UTC'))
#            timestamp = timestamp.astimezone(tz.gettz('Europe/Stockholm'))

            latitude = int(reading[3][0:2]) + float(reading[3][2:]) / 60
            longitude = int(reading[5][0:3]) + float(reading[5][3:]) / 60
            position = (latitude, longitude)

            speed = 0 if reading[7] == '' else float(reading[7]) * 1.852
            direction = 0 if reading[8] == '' else float(reading[8])

            record = {'train_id': train_id, 'route_id': route_id, 'active': active,
                      'timestamp': timestamp, 'latitude': latitude, 'longitude': longitude, 'speed': speed, 'direction': direction}

            self.df=self.df.append(record, ignore_index=True)

            return True

    def insert_df(self):
        pandas_gbq.to_gbq(self.df, 'logtrain_data.logtrain_data_table',project_id='spry-starlight-329007', if_exists='append')
        self.init_df()
#        df = pd.DataFrame(columns=self.columns)


    def add(self, reading):
        reading = reading.split(',')
        train_id = reading[14].split('.')[0]

        # Avbryt om fel tåg eller ingen position
        if train_id not in self.train_ids:
            return False
        elif reading[3] == '' or reading[5] == '' or reading[16]=='':
            return False

        else:
            train_id = reading[14].split('.')[0]
            route_id = reading[16].split('.')[0]
            active = True if reading[2] == 'A' else False

            dt_format = '%d%m%y%H%M%S.%f'
            timestamp = datetime.strptime(reading[9] + reading[1], dt_format)
            timestamp = timestamp.replace(tzinfo=tz.gettz('UTC'))
            timestamp = timestamp.astimezone(tz.gettz('Europe/Stockholm'))

            latitude = int(reading[3][0:2]) + float(reading[3][2:]) / 60
            longitude = int(reading[5][0:3]) + float(reading[5][3:]) / 60
            position = (latitude, longitude)

            speed = 0 if reading[7] == '' else float(reading[7]) * 1.852
            direction = 0 if reading[8] == '' else float(reading[8])

            record = {'train_id': train_id, 'route_id': route_id, 'active': active,
                      'timestamp': timestamp, 'latitude': latitude, 'longitude': longitude, 'speed': speed, 'direction': direction}
            
            self.records.append(record)
            
            return True

    def insert_and_reset(self):
        # 1 generate sql
        for record in self.records:
            query, job_config = self.generate_query2(record)
            # Make an API request.
            query_job = self.client.query(query, job_config=job_config)

        # 2 connect to db
        # 3 reset
        self.records = []

    def generate_query(self):
        table = 'logtrain_data.logtrain_data_table'
        fields = ','.join(self.columns)
        values =''
        for record in self.records:
            str_record = [str(s) for s in record.values()]
            values += f'({",".join(str_record)})'

        query = f'INSERT INTO {table} ({fields}) VALUES (@train_id,@route_id, @active, @timestamp, @latitude, @longitude, @speed, @direction)'

        return query


    def generate_query2(self, record):
        table = 'logtrain_data.logtrain_data_table'
        fields = ','.join(self.columns)
        query = f'INSERT INTO {table} ({fields}) VALUES (@train_id,@route_id, @active, @timestamp, @latitude, @longitude, @speed, @direction)'

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("train_id", "STRING", record['train_id']),
                bigquery.ScalarQueryParameter("route_id", "STRING", 0 if record['route_id']=='' else record['route_id']),
                bigquery.ScalarQueryParameter("active", "BOOL", record['active']),
                bigquery.ScalarQueryParameter("timestamp", "DATETIME", record['timestamp']),
                bigquery.ScalarQueryParameter("latitude", "FLOAT64", record['latitude']),
                bigquery.ScalarQueryParameter("longitude", "FLOAT64", record['longitude']),
                bigquery.ScalarQueryParameter("speed", "FLOAT64", record['speed']),
                bigquery.ScalarQueryParameter("direction", "FLOAT64", record['direction']),
            ]
        )
        return query, job_config


#        for record in self.records:
