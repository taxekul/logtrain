import pandas as pd

class Station:
    def __init__(self, data) -> None:
        pass

class StationManager:
    def __init__(self, filename='stations.csv') -> None:
        self.stations = pd.read_csv(filename,delimiter=';')        
        print()
        pass

manager = StationManager()