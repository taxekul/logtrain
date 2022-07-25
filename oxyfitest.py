from gevent import sleep
import websocket
import time
from geopy import distance
from train_manager import TrainManager
import beepy
from station_manager import StationManager

i = 0
stations = None
ids = set()


def on_message(ws, message):
    global i
    global ids
    global stations

    if train_manager.add(message):

        if train_manager.new_reading.route_id == '18967':
            closest = (None,10000000)
            for row in stations.stations.iterrows():
                pos=row[1].Position.split(',')
                pos = (float(pos[0]), float(pos[1]))
                d = round(float(distance.distance(train_manager.new_reading.position, pos).meters),1)
                if d<closest[1]:
                    closest = (row[1].Namn,d)
                    
            print(closest)

            train_manager.new_reading.position
        if train_manager.new_reading.route_id == '18965000':
            if len(train_manager.readings[train_manager.new_reading.train_id])>1:
                old_position = train_manager.readings[train_manager.new_reading.train_id][-2].position
                new_position = train_manager.new_reading.position
                print (train_manager.new_reading.position)
                #sleep(5)
                dist =float(distance.distance(old_position,new_position).meters)
                print(round(dist,1),
                '\t', 
                round(dist*3.6,1),
                '\t',
                train_manager.new_reading.speed)

    i += 1

    if i % 1000 == 0:
        print(i)
        train_manager.df_readings.to_csv('readings.csv', sep=';')
#        print(train_manager.df_readings.shape)
    if i>100000:
        print(ids)
        print(len(ids))
        exit()


def on_error(ws, e):
    print(e)

stations = StationManager()

test = on_message
ws = websocket.WebSocketApp("wss://api.oxyfi.com/trainpos/listen?v=1&key=21f372da5fb1eac65250b56ef6fa60ab",on_message = test, on_error = on_error)
train_manager = TrainManager()
try:
    result = ws.run_forever(ping_timeout = 10)
except Exception as e:
    print('Exception:',e.format())

print('Avslutar!' , result)