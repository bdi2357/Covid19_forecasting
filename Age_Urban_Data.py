#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os,re
import pandas as pd


# In[74]:


df_s = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUS.csv")


# In[75]:


df_s.shape


# In[76]:


import wikipedia as wiki


# In[77]:


p = wiki.page("List_of_countries_by_age_structure")


# In[78]:


df = pd.read_html(p.html())[0]
for c in df.columns[1:]:
    df[c] = df.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
df.columns


# In[79]:


df[[('Country','Country'), ('Population by age',  'Over 65[3]')]][:50]


# In[80]:


state = pd.read_csv("/Users/itaybd/Documents/state_demographics.csv")
state.columns


# In[81]:


state[['State','Age.Percent 65 and Older']]


# In[82]:


u = wiki.page("Urbanization_by_country")
uf = pd.read_html(u.html())[0]
"""
for c in uf.columns[1:]:
    uf[c] = uf.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
"""
uf.columns


# In[83]:


c = 'Urban Population (%)'
def to_val(s):
    if len(re.findall('[0-9]+\.[0-9]+',str(s)))>0:
        return float(re.findall('[0-9]+\.[0-9]+',str(s))[0])*0.01
    else:
        return 1.0
        
uf[c] = uf.apply(lambda r: to_val(r[c]),axis=1 )
uf[['Nation', 'Urban Population (%)']][:30]


# In[84]:


us = wiki.page("Urbanization_in_the_United_States")
usf2 = pd.read_html(us.html())[0]
"""
for c in uf.columns[1:]:
    uf[c] = uf.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
"""
usf2.head()


# In[85]:


#usf2 = usf2.rename(columns={x:x[1] for x in usf2.columns})
usf2.columns = [x[1] for x in usf2.columns]


# In[86]:


usf2.columns[0],usf2[['State/Territory','2010']][5:15]
usf3 = usf2[5:][['State/Territory','2010']]
usf3['State'] = usf3.apply(lambda r: re.findall('[A-Za-z\ \-]+',r['State/Territory'])[0],axis=1)
usf3[20:40]


# In[87]:


df_all = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUSM2.csv",index_col="index") 


# In[88]:


df_ind =  pd.read_csv("../output_covid/test_covid19_wUSN3_index.csv")


# In[89]:


df_ind.head()


# In[90]:


countries_l = set(df_ind.apply(lambda r: eval(r["index"])[0],axis=1))
countries_u = set(uf['Nation'])
mtch = countries_l.intersection(countries_u)
len(mtch)


# In[92]:


us_d = list(usf3['State'])
us_du = set(["US_"+x for x in us_d])
len(countries_l),len(us_du.intersection(countries_l))


# In[93]:


usf3['State_us'] = "US_" + usf3['State'] 


# In[94]:


ufn = uf[["Nation",'Urban Population (%)']]
ufn.head()
usf4 = usf3[["State_us","2010"]]
c="2010"
usf4[c] = usf4.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
usf4.head()
usf4.columns = ["Country_Province","Urban Population Ratio"]
ufn.columns = ["Country_Province","Urban Population Ratio"]


# In[95]:


cnct1 = pd.concat([ufn,usf4])
cnct1.tail()


# In[97]:


age_c = df[[('Country','Country'), ('Population by age',  'Over 65[3]')]]
age_c.columns = ["Country_Province","Over 65 Ratio"]
age_c.head()


# In[102]:


age_u = state[['State','Age.Percent 65 and Older']]
age_u['Age.Percent 65 and Older'] *= 0.01
age_u.columns = ["Country_Province","Over 65 Ratio"]


# In[103]:


cnct2 = pd.concat([age_c,age_u])
cnct2.head(),cnct2.tail()


# In[128]:


cnct1_d = cnct1.set_index("Country_Province")
cnct2_d = cnct2.set_index("Country_Province")
ix2 = set(cnct1_d.index.values).intersection(set(cnct2_d.index.values))
ix2 = sorted(list(ix2))
cnct1_d = cnct1_d.loc[ix2]
cnct2_d = cnct2_d.loc[ix2]
cnct2_d = cnct2_d[~cnct2_d.index.duplicated(keep='first')]
cnct1_d = cnct1_d.sort_index()
cnct2_d = cnct2_d.sort_index()
cnct1_d.shape,cnct2_d.shape,len(ix2),[x for x in cnct2_d.index.values if not  x in cnct1_d.index.values]


# In[129]:


cnct_all = pd.concat([cnct1_d,cnct2_d],axis=1)


# In[131]:


cnct_all.tail()


# In[ ]:




