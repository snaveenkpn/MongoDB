from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint


#This line will find the .env file from the project and load the content from .env file to here
load_dotenv(find_dotenv())

#Retrieves the value of the mongo_password variable from the environment and stores it in the password variable.
password=os.environ.get("mongo_password")

connection_string = f"mongodb+srv://snaveenkpn:{password}@pymongo1.29asc.mongodb.net/"


#This line will use connection string and connect to our db
client = MongoClient(connection_string)

dbs=client.list_database_names()
print(dbs)

#Here we will list the collection that is inside school db.
my_db=client.school
list_collection=my_db.list_collection_names()
print(list_collection)

def insert_doc():
    collection = my_db.students

    new_person={
        "name": "naveen",
        "age": 25
    }

    inserted_id=collection.insert_one(new_person)
    print(inserted_id)

#insert_doc()

gym=client.gym
gym_collection=gym.gym_member

def insertmany_doc():

    first_names=["naveen","john","rohit","captain","peter"]
    last_names=["Srinivasan","cena","sharma","america","parker"]
    ages=[25,40,35,36,29]

    docs=[]

    for firstname, lastname, age in zip(first_names,last_names,ages):
        doc={"firstname": firstname,"lastname": lastname,"age": age}

        docs.append(doc)

    gym_collection.insert_many(docs)

#insertmany_doc()

printer = pprint.PrettyPrinter()

def findallpeople():

    people=gym_collection.find()

    for person in people:
        printer.pprint(person)


#findallpeople()

def find_naveen():
    naveen= gym_collection.find_one({"firstname":"naveen"})

    printer.pprint(naveen)

#find_naveen()


def count_gymcollection():
    count=gym_collection.count_documents(filter={})

    printer.pprint(count)

#count_gymcollection()

def getperson_byid(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person=gym_collection.find_one({"_id": _id})

    printer.pprint(person)

#getperson_byid("66c06c2231fd2e61cea33180")

def age_by_range(minage, maxage):

    query= {"$and": [{"age": {"$gte":minage}},{"age":{"$lte":maxage}}]}
    age_ranges=gym_collection.find(query).sort({"age":-1})

    for age_range in age_ranges:
        printer.pprint(age_range)

#age_by_range(20,30)

def project_columns():
    columns={"firstname":1, "_id":0, "age":1}

    projection=gym_collection.find({}, columns)

    for person in projection:
        printer.pprint(person)

#project_columns()

def update_person():

    john_age= {"$set": {"age": 50}}
    
    

    gym_collection.update_one({"firstname": "john"}, john_age)

#update_person()

def update_one_byid(person_id):
    from bson.objectid import ObjectId

    _id=ObjectId(person_id)

    age={"$set":{"age":26}}

    gym_collection.update_one({"_id": _id}, age)

#update_one_byid("66c06bf48bd1d08a51a41edc")

def replace_one_byid(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    fields = {
        "firstname":"NAVEEN",
        "lastname":"SRINIVASAN",
        "age": 25
    }

    gym_collection.replace_one({"_id":_id}, fields)

#replace_one_byid("66c06bf48bd1d08a51a41edc")

def delete_one(person_id):
    from bson.objectid import ObjectId

    _id=ObjectId(person_id)

    gym_collection.delete_one({"_id":_id})

#delete_one("66c06bf48bd1d08a51a41edc")

def delete_many():
    gym_collection.delete_many({})

#delete_many()

# ---------------------------------------------------------------------#
#Relationship

address = {
    "country":"india",
    "zip":"600118",
}

def embed_address(person_id, address):
    from bson.objectid import ObjectId

    _id=ObjectId(person_id)

    gym_collection.update_one({"_id":_id},{"$addToSet": {"addresses": address}})

#embed_address("66c06c2231fd2e61cea3317f", address)

def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId

    address["owner_id"]=person_id

    gym_prod=gym.production

    gym_prod.insert_one(address)

add_address_relationship("66c06c2231fd2e61cea3317f", address)

