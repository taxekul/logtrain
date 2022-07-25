from datetime import datetime
from dateutil import tz

# import datetime
class TrainReading:
    def __init__(self, data):
        self.data=data
        if data[3] == '' or data[5] == '':# or data[7] == '' or data[8] == '':
            return    

        self.active = True if data[2] == "A" else False
        self.latitude = int(data[3][0:2]) + float(data[3][2:]) / 60
        self.longitude = int(data[5][0:3]) + float(data[5][3:]) / 60
        self.position = (self.latitude, self.longitude)

        self.speed = 0 if data[7]=='' else float(data[7])*1.852
        self.direction = 0 if data[8]=='' else float(data[8])
        self.train_id = data[14].split(".")[0] 
        self.route_id = data[16].split(".")[0]
        
        # Tidpunkt
        try:
            dt_format = "%d%m%y%H%M%S.%f"
            self.datetime = datetime.strptime(data[9] + data[1], dt_format)
            self.datetime = self.datetime.replace(tzinfo=tz.gettz('UTC'))
            self.datetime = self.datetime.astimezone(tz.gettz('Europe/Stockholm'))


        except Exception as e:
            print('TrainReading Exception:',e.format())
