import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["demo"]
mycol = mydb["mqtt"]

mydict = {"city": "John", "address": "Highway 13", "n_articles": "38"}

x = mycol.insert_one(mydict)

print(x.inserted_id)