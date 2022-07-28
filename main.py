#from gevent import sleepimport warnings
import websocket
import time
from train_manager import TrainManager

i = 0
ids = set()


def on_message(ws, message):
    
    global i
    global ids

    # Spara
    if train_manager.add_to_df(message):
        # Spara när 1000 poster
        if train_manager.df.shape[0] >= 1000:
            train_manager.insert_df_into_bigquery()

            # i+=1
            # if i==2:
#            exit()
#            train_manager.insert_and_reset()
#            time.sleep(5)



def on_error(ws, e):
    print(e)

test = on_message
train_manager = TrainManager(5)

ws = websocket.WebSocketApp(
    "wss://api.oxyfi.com/trainpos/listen?v=1&key=21f372da5fb1eac65250b56ef6fa60ab", on_message=test, on_error=on_error)
try:
    result = ws.run_forever()
except Exception as e:
    print('Exception:',e.format())

print('Avslutar!' , result)
print(train_manager.df.head())
print(train_manager.df.info())
print(train_manager.df.shape[0])