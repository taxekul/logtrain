# import warnings
# warnings.simplefilter(action="ignore", category=FutureWarning)

# from google.auth import compute_engine
# import pandas as pd
# import pandas_gbq
from datetime import datetime
from datetime import timedelta
import mysql.connector
import logging

# from google.cloud import bigquery


class TrainManager:
    def __init__(self, add_interval_seconds=0, insert_interval_seconds=60, insert_interval_records=100) -> None:
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
                'speed',
                'direction',
            )
#        self.init_df()

        # Senaste uppdateringen för respektive route
        self.latest_update = {}
        self.add_interval_seconds = add_interval_seconds
        self.latest_insert = datetime.now()
        self.insert_interval_seconds=insert_interval_seconds
        self.insert_interval_records = insert_interval_records


        self.db = mysql.connector.connect(user='my_remote', password="my_remote",
                             host='34.172.240.147',
                             database='korv')
        self.cursor = self.db.cursor()


    #Lägga till i databasen nu?
    # @property 
    # def time_to_insert(self):
    #     return datetime.now() > self.latest_insert + timedelta(seconds=self.insert_interval_seconds) or self.df.shape[0]>=self.insert_interval_records


    def add_to_db(self,reading):
        reading = reading.split(',')
        train_id = reading[14].split('.')[0]

        # Avbryt om fel tåg eller ingen position eller route
        if False:#train_id not in self.train_ids:
            
            return False
        elif reading[3] == '' or reading[5] == '' or reading[16] == '':
            return False
        else:
            train_id = reading[14].split('.')[0]
            route_id = reading[16].split('.')[0]
            active = True if reading[2] == 'A' else False

            dt_format = '%d%m%y%H%M%S.%f'
            timestamp_timestamp = datetime.strptime(reading[9] + reading[1], dt_format)
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
#            timestamp=pd.Timestamp(timestamp)

            # Avbryt om för tidigt
            if timestamp_timestamp < self.latest_update.get(route_id, datetime.fromtimestamp(0)) + timedelta(seconds=self.add_interval_seconds):
                return False

            # Gör om till decimal i stället för minuter osv
            latitude = int(reading[3][0:2]) + float(reading[3][2:]) / 60
            longitude = int(reading[5][0:3]) + float(reading[5][3:]) / 60
            position = (latitude, longitude)

            # km/h i stället för knop
            speed = 0 if reading[7] == '' else float(reading[7]) * 1.852

            direction = 0 if reading[8] == '' else float(reading[8])

            record = {'train_id': train_id, 'route_id': route_id, 
                      'timestamp': timestamp, 'latitude': latitude, 'longitude': longitude, 'speed': speed, 'direction': direction}
            values = str(*record.values())
#            logging.info(f'v: {values}')
            query_insert = f"""INSERT INTO readings (train_id,route_id,timestamp,latitude,longitude,speed,direction) VALUES ({record['train_id']},{record['route_id']},
            {record['timestamp']},{record['latitude']},{record['longitude']},{record['speed']},{record['direction']}"""
##            val = record.values()
            self.cursor.execute(query_insert)#, values)
            self.db.commit()

            return True

            self.df=self.df.append(record, ignore_index=True)
            self.latest_update[route_id] = timestamp
            # print('Tillagt')
            return True