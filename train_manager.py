from train_reading import TrainReading
import pandas as pd
from datetime import datetime
from dateutil import tz


class TrainManager:
    def __init__(self) -> None:
        with open('train_ids.txt', 'r') as train_file:
            self.train_ids = [train_id.strip() for train_id in train_file.readlines()]

        self.df_readings = pd.DataFrame(
            columns=(
                'train_id',
                'route_id',
                'active',
                'timestamp',
                'position',
                'speed',
                'direction',
            )
        )

        self.readings = {}
        for train_id in self.train_ids:
            self.readings[train_id] = []

    def add(self, reading):
        reading = reading.split(',')
        train_id = reading[14].split('.')[0]

        # I listan?
        if train_id not in self.train_ids:
            return False
        elif reading[3] == '' or reading[5] == '':
            return False

        else:
            self.new_reading = TrainReading(reading)
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
            news_dict = {'train_id':train_id, 'route_id':route_id, 'active':active, 'timestamp':timestamp, 'position':position, 'speed':speed, 'direction':direction}
            news = (train_id, route_id, active, timestamp, position, speed, direction)
            new_series = pd.Series(news)
            self.df_readings=self.df_readings.append(news_dict, ignore_index=True)
#            self.df_readings=pd.concat([self.df_readings, new_series], ignore_index=True)
#            self.df_readings = self.df_readings.append(new_series, ignore_index=True)
            
#            self.df_readings.loc[len(self.df_readings)] = new_series
            self.readings[self.new_reading.train_id].append(self.new_reading)
            return True


korv = TrainManager()
