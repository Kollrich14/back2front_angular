import requests
import pymongo
import datetime
import json

MONGO_DB_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "WeatherDB"
MONGO_DB_WEATHER_COLLECTION_NAME = "Weather"
MONGO_DB_FORECAST_COLLECTION_NAME = "Forecast"


def getData():
    ret = {}

    r = requests.get('https://goweather.herokuapp.com/weather/vienna')
    data = r.json()

    keys = data.keys()

    if "temperature" not in keys and "wind" not in keys and "description" not in keys and "forecast" not in keys:
       return ret

    #forecast = data["forecast"]

    ret = {"Weather": {"timestamp": datetime.datetime.now(),
                       "temperature": data["temperature"], 
                       "wind": data["wind"], 
                       "description": data["description"]},
           "Forecast1": {},
           "Forecast2": {}}

    return ret

def main():
    print("Connect to database")
    dbClient = pymongo.MongoClient(MONGO_DB_URL)

    print("Check & Open DB")
    if MONGO_DB_NAME not in dbClient.list_database_names():
        return -1 
    
    db = dbClient[MONGO_DB_NAME]
    
    print("Check Collections")
    if MONGO_DB_WEATHER_COLLECTION_NAME not in db.list_collection_names():
        return -2

    colWeather = db[MONGO_DB_WEATHER_COLLECTION_NAME]

    if MONGO_DB_FORECAST_COLLECTION_NAME not in db.list_collection_names():
        return -3

    colForecast = db[MONGO_DB_FORECAST_COLLECTION_NAME]
    
    print("get data")
    response = getData()

    print("add weather to db")
    x = colWeather.insert_one(response["Weather"])


    print("Finished!")
    return 0












print("main", main())
