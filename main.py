#from gevent import sleepimport warnings
import websocket
import time
from train_manager import TrainManager
import logging

import google.cloud.logging
client = google.cloud.logging.Client()
client.setup_logging()


import logging


print('RUNING',flush=True)

def on_message(ws, message):
    # Spara
    train_manager.add_to_db(message)
    
    #    train_manager.insert_df_into_bigquery()


def on_error(ws, e):
    print(e)


test = on_message
train_manager = TrainManager(add_interval_seconds=5,insert_interval_seconds=300, insert_interval_records=1000)

ws = websocket.WebSocketApp(
    "wss://api.oxyfi.com/trainpos/listen?v=1&key=21f372da5fb1eac65250b56ef6fa60ab", on_message=test, on_error=on_error)
try:
    result = ws.run_forever()
except Exception as e:
    print('Exception:',e.format())

print('Avslutar!' , result)
# print(train_manager.df.head())
# print(train_manager.df.info())
# print(train_manager.df.shape[0])