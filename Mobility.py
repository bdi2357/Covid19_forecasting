#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os,re
import pandas as pd


# In[2]:


df = pd.read_csv("/Users/itaybd/Documents/Global_Mobility_Report.csv")


# In[3]:


df.columns


# In[4]:


df[df['country_region']=="Israel"].shape


# In[5]:


set(df[df['country_region']=="Israel"]['sub_region_1'])


# In[6]:


TA= df[(df['country_region']=="Israel") &  (df['sub_region_1']=='Tel Aviv District')]
print(TA.columns)


# In[7]:


print(TA[["date",'workplaces_percent_change_from_baseline']].tail())


# In[8]:

exit(0)
df[df['country_region']=="United States"].shape


# In[9]:


set(df[df['country_region']=="United States"]['sub_region_1'])


# In[10]:


cols=['transit_stations_percent_change_from_baseline','workplaces_percent_change_from_baseline']


# In[11]:


get_ipython().run_cell_magic('timeit', '', 'r1= df[cols[0]] - df[cols[1]]')


# In[12]:


def minus(a,b):
    return a-b


# In[13]:


get_ipython().run_cell_magic('timeit', '', 'r2 = minus(df[cols[0]].values,df[cols[1]].values)')


# In[14]:


#%%timeit
#r3 = df.apply(lambda r: minus(r[cols[0]],r[cols[1]]),axis=1)


# In[15]:


r1= df[cols[0]] - df[cols[1]]
r2 = pd.Series(minus(df[cols[0]].values,df[cols[1]].values))
r1.head(),r2.head(),type(r1),type(r2)


# In[16]:


r1.shape


# In[17]:




