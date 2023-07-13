#!/usr/bin/env python
# coding: utf-8

# # Eat Safe, Love

# ## Part 1: Database and Jupyter Notebook Set Up

# Import the data provided in the `establishments.json` file from your Terminal. Name the database `uk_food` and the collection `establishments`.
# 
# Within this markdown cell, copy the line of text you used to import the data from your Terminal. This way, future analysts will be able to repeat your process.
# 
# e.g.: Import the dataset with mongoimport --type json -d uk_food -c establishments --drop --jsonArray establishments.json

# In[1]:


# Import dependencies
from pymongo import MongoClient
from pprint import pprint


# In[ ]:


# Create an instance of MongoClient
mongo = MongoClient(port=27017)


# In[ ]:


# confirm that our new database was created
print(mongo.list_database_names())


# In[ ]:


# assign the uk_food database to a variable name
db = mongo['uk_food']


# In[ ]:


# review the collections in our new database
db.list_collection_names()


# In[ ]:


# review a document in the establishments collection
print(db.establishments.find_one())


# In[ ]:


# assign the collection to a variable
establishments = db['establishments']


# ## Part 2: Update the Database

# 1. An exciting new halal restaurant just opened in Greenwich, but hasn't been rated yet. The magazine has asked you to include it in your analysis. Add the following restaurant "Penang Flavours" to the database.

# In[ ]:


# Create a dictionary for the new restaurant data
new_resturant = {}


# In[ ]:


# Insert the new restaurant into the collection
establishments.insert_one(new_restaurant)


# In[ ]:


# Check that the new restaurant was inserted
print(list(establishments.find({"BusinessName":"Penang Flavours"})))


# 2. Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the `BusinessTypeID` and `BusinessType` fields.

# In[ ]:


# Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the BusinessTypeID and BusinessType fields
query = {'BusinessType': 'Restaurant/Cafe/Canteen'}
fields = {'BusinessTypeID': 1, 'BusinessType':1, '_id': 0}
results = establishments.find_one(query, fields)

pprint(results)


# 3. Update the new restaurant with the `BusinessTypeID` you found.

# In[ ]:


# Update the new restaurant with the correct BusinessTypeID
establishments.update_one({'BusinessName': 'Penang Flavours'}, {'$set': {'BusinessTypeID': '1'}})


# In[ ]:


# Confirm that the new restaurant was updated
print(list(establishments.find_one({'BusinessName': 'Penang Flavours'}))


# 4. The magazine is not interested in any establishments in Dover, so check how many documents contain the Dover Local Authority. Then, remove any establishments within the Dover Local Authority from the database, and check the number of documents to ensure they were deleted.

# In[ ]:


# Find how many documents have LocalAuthorityName as "Dover"
establishments.count_documents({'LocalAuthorityName': 'Dover'})


# In[ ]:


# Delete all documents where LocalAuthorityName is "Dover"
establishments.delete_many({'LocalAuthorityName': 'Dover'})


# In[ ]:


# Check if any remaining documents include Dover
print(list(establishments.count_documents({'LocalAuthorityName': 'Dover'}))


# In[ ]:


# Check that other documents remain with 'find_one'
print(list(establishments.find_one())


# 5. Some of the number values are stored as strings, when they should be stored as numbers.

# Use `update_many` to convert `latitude` and `longitude` to decimal numbers.

# In[ ]:


# Change the data type from String to Decimal for longitude and latitude
establishments.update_many({}, [{'$set':{'geocode.latitude': {'$toDecimal': '$geocode.latitude'},
                                             'geocode.longitude':{'$toDecimal': '$geocode.longitude'}}}])


# Use `update_many` to convert `RatingValue` to integer numbers.

# In[ ]:


# Set non 1-5 Rating Values to Null
non_ratings = ["AwaitingInspection", "Awaiting Inspection", "AwaitingPublication", "Pass", "Exempt"]
establishments.update_many({"RatingValue": {"$in": non_ratings}}, [ {'$set':{ "RatingValue" : None}} ])


# In[ ]:


# Change the data type from String to Integer for RatingValue
establishments.update_many({}, [ {'$set': {'RatingValue': {'$toInt': '$RatingValue'}} } ])


# In[ ]:


# Check that the coordinates and rating value are now numbers
print(list(establishments.find_one())

