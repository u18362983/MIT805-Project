#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
import seaborn as sns


custlist = pd.read_csv("c:/AWorkfile/archive/part-00000.csv", header='infer', sep=",")
custlist .columns


# In[2]:


#show first five rows of record
custlist.head(5)


# In[3]:


#The last rows of the dataset
custlist .tail()


# In[4]:


#Dataset shape
custlist.shape


# In[5]:


#Summary of the dataset
pd.set_option('display.float_format', lambda x: '%.3f' % x)
custlist .describe()


# In[6]:


#Sorting the values and listing and checking customers with highest actions
df = custlist.sort_values(by=['actions'], ascending=False)
df.head


# In[8]:


#Checking the customers with the most views and writting to a file 
df_first_10 = df.head(10)
df_first_10


# In[ ]:





# In[9]:


top100 = custlist.nlargest(100, 'actions')
top100.to_csv('c:/AWorkfile/archive/top100.csv')
custlist.head()


# In[10]:


#Counting customers number of actions
count1 = custlist.groupby('actions')['cust_id'].count()
count1


# In[11]:


#Plotting the how many customers had an action in a count of events
plt.plot(count1)
plt.show()


# In[12]:


import pandas as pd
from matplotlib.pyplot import pie, axis, show
get_ipython().run_line_magic('matplotlib', 'inline')

df =  pd.read_csv('c:/AWorkfile/archive/Topcust.csv')
df.head()

sums = df.groupby('brand')["user_id"].count()
print(sums)
axis('equal');
pie(sums, labels=sums.index);
show()


# In[13]:


df =  pd.read_csv('c:/AWorkfile/archive/Topcust.csv')
sums = df.groupby('event_type')["user_id"].count()
print(sums)
axis('equal');
pie(sums, labels=sums.index);
show()


# In[ ]:




