#!/usr/bin/env python
# coding: utf-8

# # Eat Safe, Love

# ## Notebook Set Up

# In[ ]:


# Import dependencies
from pymongo import MongoClient
from pprint import pprint


# In[ ]:


# Create an instance of MongoClient
mongo = MongoClient(port=27017)


# In[ ]:


# assign the uk_food database to a variable name
db = mongo['uk_food']


# In[ ]:


# review the collections in our database
db.list_collection_names()


# In[ ]:


# assign the collection to a variable
establishments = db['establishments']


# ## Part 3: Exploratory Analysis
# Unless otherwise stated, for each question: 
# * Use `count_documents` to display the number of documents contained in the result.
# * Display the first document in the results using `pprint`.
# * Convert the result to a Pandas DataFrame, print the number of rows in the DataFrame, and display the first 10 rows.

# ### 1. Which establishments have a hygiene score equal to 20?

# In[ ]:


# Find the establishments with a hygiene score of 20
query = {'scores.Hygiene':  20}
# Use count_documents to display the number of documents in the result
hygiene_score_count = establishments.count_documents(query)
print(f'{hygiene_score_count} establishments with a hygiene score of 20.')

# Display the first document in the results using pprint
pprint(results[0])


# In[ ]:


# Convert the result to a Pandas DataFrame
hygiene_df = pd.DataFrame(results)
# Display the number of rows in the DataFrame
print("DataFrame Rows: ", len(hygiene_df))
# Display the first 10 rows of the DataFrame
hygiene_df.head(10)


# ### 2. Which establishments in London have a `RatingValue` greater than or equal to 4?

# In[ ]:


# Find the establishments with London as the Local Authority and has a RatingValue greater than or equal to 4.
rating_query = {"LocalAuthorityName" : {"$regex" : "London"} , "RatingValue" : {"$gte":"4"}}

rating_results = establishments.find(rating_query)
# Use count_documents to display the number of documents in the result
rating = establishments.count_documents(rating_query)

print('Total establishments in London with a rating of 4 or greater: ', rating)
print()

# Display the first document in the results using pprint
pprint(rating_results[0])


# In[ ]:


# Convert the result to a Pandas DataFrame 
rating_df = pd.DataFrame(rating_results) 
# Display the number of rows in the DataFrame
print("DataFrame Rows: ", len(rating_df))
# Display the first 10 rows of the DataFrame
rating_df.head(10)


# ### 3. What are the top 5 establishments with a `RatingValue` rating value of 5, sorted by lowest hygiene score, nearest to the new restaurant added, "Penang Flavours"?

# In[ ]:


# Search within 0.01 degree on either side of the latitude and longitude.
# Rating value must equal 5
# Sort by hygiene score
Penang_query = {'BusinessName': 'Penang Flavours'}
longitude_fields = {'geocode.longitude': 1, '_id': 0}
latitude_fields = {'geocode.latitude': 1, '_id': 0}

degree_search = 0.01
latitude = establishments.find_one(Penang_query, latitude_fields)['geocode']['latitude']
longitude = establishments.find_one(Penang_query, longitude_fields)['geocode']['longitude']

query = {'RatingValue': 5, 
         'geocode.latitude': {'$lte': (latitude + degree_search), '$gte': (latitude - degree_search)},
         'geocode.longitude': {'$lte': (longitude + degree_search), '$gte': (longitude - degree_search)}}
sort =  [('scores.Hygiene', 1)]
limit = 5
# Print the results
results = list(establishments.find(query).sort(sort).limit(limit))
for restaurants in results:
    pprint(restaurants)


# In[ ]:


# Convert result to Pandas DataFrame
results_df = pd.DataFrame(results)
results_df


# ### 4. How many establishments in each Local Authority area have a hygiene score of 0?

# In[ ]:


# Create a pipeline that: 
# 1. Matches establishments with a hygiene score of 0
# 2. Groups the matches by Local Authority
# 3. Sorts the matches from highest to lowest
match_query = {'$match': {'scores.Hygiene': 0}}

group_query = {'$group': {'_id': '$LocalAuthorityName', 'count': {'$sum': 1} }}

sort_values = {'$sort': {'count': -1}}

pipeline = [match_query, group_query, sort_values]
# Print the number of documents in the results

results = list(establishments.aggregate(pipeline))

print(f'There are {len(results)} documents.')
# Print the first 10 results

print(pipeline.get_result(10))


# In[ ]:


# Convert the result to a Pandas DataFrame
pipeline_df.ConvertToDF()
# Display the number of rows in the DataFrame
print(pipeline_df.set_result)
# Display the first 10 rows of the DataFrame
pipeliene_df.set_result.head(10)

