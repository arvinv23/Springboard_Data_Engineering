#!/usr/bin/env python
# coding: utf-8

# In[21]:


#Sources

# https://www.kaggle.com/code/anthokalel/basket-analysis-for-dog-food/input

# https://www.kaggle.com/code/nolenja/pet-food-customer-order/notebook


# In[22]:


from plotly.offline import init_notebook_mode, iplot
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import holoviews as hv
init_notebook_mode(connected=True)
hv.extension('bokeh')

import seaborn as sns
import matplotlib.pyplot as plt


# In[23]:


aisles = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/aisles.csv")
departements = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/departments.csv")
order_products__prior = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/order_products__prior.csv")
order_products__train = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/order_products__train.csv")
orders = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/orders.csv")
products = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/products.csv")


# In[24]:


aisles[aisles['aisle'].str.contains('dog')] #Which aisle does contain the dog food ? 
dog_food_products = products.query("aisle_id == 40") #Dog products


# In[25]:


#users, orders and products on the same dataframe
orders_order_product = pd.merge(orders, order_products__train, on = ['order_id'])
user_product_dog = pd.merge(orders_order_product, dog_food_products, on = ['product_id'])


# In[26]:


most_dogfood_bought = user_product_dog['product_name'].value_counts().head(15).index.tolist() # The most dog food bought
user_product_dog_top = user_product_dog[user_product_dog['product_name'].isin(most_dogfood_bought)] # Selection in Dataframe


# In[27]:


fig = px.pie(user_product_dog_top, values='product_id', names='product_name', title='Top 15 of food dog bought')
fig.show()


# In[28]:


pfco = pd.read_csv("/Users/aravindh/Desktop/Data Engineering/Capstone 1/Data Collection/pet_food_customer_orders.csv")
pfco.head()


# In[29]:


print(pfco.pet_allergen_list.value_counts())


# In[30]:


top10_pet_health_issue = pfco.groupby('pet_health_issue_list')['pet_health_issue_list'].agg('count').sort_values(ascending=False)[:10]
top10_pet_health_issue.head(10)


# In[31]:


colors = ['#f1f1f1' for _ in range(len(top10_pet_health_issue))]
colors[0] =  '#E50914'

plt.figure(figsize=(10,5))
plt.bar(top10_pet_health_issue.index, top10_pet_health_issue, width=0.8, linewidth=0.6, color=colors)
plt.grid(axis='y', linestyle='-', alpha=0.2)
plt.tick_params(axis='both', which='major', labelsize=12)
plt.title('Top 10 pet_health_issue' , fontsize=18, fontfamily='Malgun Gothic', fontweight='bold', position=(0, 0))

sns.despine(top=True, right=True, left=True, bottom=False)
plt.xticks(rotation=70) 
plt.show()


# In[32]:


plt.figure(figsize=(20, 10))
group=pfco.groupby('customer_id')
custom=group.size()[group.size()>=3]
custom.value_counts()
custom.value_counts().plot.pie(autopct='%1.1f%%')


# In[33]:


regular_customer=pfco[pfco.customer_id.isin(custom.index)]
passenger=pfco[~pfco.customer_id.isin(custom.index)]
pfco.head()


# In[34]:


sns.countplot(regular_customer['pet_has_active_subscription'])


# In[35]:


regular_customer.signup_promo.value_counts()


# In[36]:


t=regular_customer[regular_customer.pet_has_active_subscription==True]

t.signup_promo.value_counts()


# In[37]:


t=pfco.set_index(['customer_id'])

print(t.pet_food_tier.value_counts())


# In[38]:


pip install tweepy


# In[2]:


import tweepy
import json


# In[3]:


consumer_key = "NYoeB8WRdc9RpqXjeL9w9E2nR"
consumer_secret = "XYNHAIUmE8dR0Wo05nF4QzStXu1XcIRWqoKh9LMWlSdwboXiJ7"
access_token = "1809311278349053952-JeVJ3TS7LhO2B2rtmx1eSunVLki8jI"
access_token_secret = "rDhBsJnBIWqPF0fg9CMdT9OuOwaVbL0ulgshqEnG3UWtr"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# In[4]:


search_query = "healthy pet food"
tweet_count = 100


# In[20]:


# Set up authentication
bearer_token = "AAAAAAAAAAAAAAAAAAAAAA5GvAEAAAAAvbJv9%2F0UJ7zRiVrd589XTa%2Fnb8M%3DB6Xoyj92B45BVOqbNFWFPTzrbo6ClqdJm5R1YTZOtr3Y5IBoVy"
# Create a Tweepy StreamingClient
class TweetListener(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print("Tweet ID:", tweet.id)
        print("Username:", tweet.user.username)
        print("Tweet Text:", tweet.text)
        print("Created At:", tweet.created_at)
        print("----")
        # Store the tweet data as per your requirements

    def on_error(self, status_code):
        if status_code == 420:
            return False
        print("Error:", status_code)

# Create a TweetListener object
stream = TweetListener(bearer_token)

# Start streaming tweets
stream.sample()


# In[ ]:




