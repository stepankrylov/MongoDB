from pymongo import MongoClient
import csv
from pprint import pprint
import re
from datetime import datetime

client = MongoClient()

mongo_tikets_db = client['tikets']
print(mongo_tikets_db)

artists_collection = mongo_tikets_db.artists

def read_data():
    obj = []
    with open('artists.csv', encoding='utf-8') as fp:
        reader = csv.reader(fp)
        c = 0
        for row in reader:
            if c != 0:
                artist, price, place, date = row
                data = dict(
                            artist=artist, 
                            price=int(price), 
                            place=place, 
                            date=datetime(
                                          year=2019, 
                                          month=int(date.split('.')[1]), 
                                          day=int(date.split('.')[0])
                                        )
                            )
                obj.append(data)
            c += 1
    pprint(obj)
    res = artists_collection.insert_many(obj)
    return res

def find_cheapest():
    for item in artists_collection.find().sort('price'):
        pprint(item)

def find_by_name(list_artists):
    for artist in list_artists:
        regex = re.compile('\w*[ -]*\w*[ ]*\w*[ ]*\w*[ ]*\d*'+ artist +'\w*[ -]*\w*[ ]*\w*[ ]*\w*[ ]*\d*')
        for item in artists_collection.find({'artist': regex}).sort('price'):
            pprint(item)

def find_date(after, before):
    for item in artists_collection.find(
        {'date': {'$gte': datetime(2019, int(after.split('.')[0]), int(after.split('.')[1]), 0, 0), 
        '$lte': datetime(2019, int(before.split('.')[0]), int(before.split('.')[1]), 0, 0)}}
        ).sort('date'):
        pprint(item)

def drop():
    client.drop_database(mongo_tikets_db)

if __name__ == "__main__":
    #find_date('6.01', '10.20')
    #find_by_name(['Seconds', 'Шуфутинский', '1975', 'Music', 'ДжаZ'])
    find_cheapest()
