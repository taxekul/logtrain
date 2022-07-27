#from gevent import sleep
import websocket
import time
from train_manager import TrainManager

i = 0
ids = set()


def on_message(ws, message):
    global i
    global ids

    if train_manager.add(message):
        if len(train_manager.records) > 20:
            train_manager.insert_and_reset()
            time.sleep(5)



def on_error(ws, e):
    print(e)

test = on_message
train_manager = TrainManager()
#on_message(None, '$GPRMC,061108.00,A,6040.30193,N,01709.38107,E,0.015,,270722,,,A*70,,9067.trains.se,,,oxyfi')

ws = websocket.WebSocketApp(
    "wss://api.oxyfi.com/trainpos/listen?v=1&key=21f372da5fb1eac65250b56ef6fa60ab", on_message=test, on_error=on_error)
try:
    result = ws.run_forever()
except Exception as e:
    print('Exception:',e.format())

print('Avslutar!' , result)
