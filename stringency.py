#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time


# In[3]:


test_path ="../covid-policy-tracker/data/timeseries/index_stringency.csv"


# In[4]:


ds = pd.read_csv(test_path)


# In[5]:


ds.shape


# In[6]:


output_path = "/Users/itaybd/output_covid"
df1 = pd.read_csv(os.path.join(output_path,"test_covid19_wUS2_index.csv"))
df1.head()


# In[7]:


df2 = pd.read_csv((os.path.join(output_path,"test_stringency_index.csv")))
df2.head()


# In[8]:


df2.shape,df1.shape


# In[9]:


d1 = df1.set_index("index")["val"].to_dict()
d2 = df2.set_index("index")["val"].to_dict()
d2k = set(d2.keys())
d3 =[ k for k in d1.keys() if k in d2k]
len(d3)


# In[13]:


Ax=(set([eval(x)[0] for x in d3]))
len(Ax), list(Ax)[:10]


# In[14]:


import wikipedia


# In[20]:


t = wikipedia.page("OECD")


# In[30]:


oecd_c = pd.read_html(t.html())[5]


# In[31]:


L = list(oecd_c["Country"])


# In[34]:


Ax_oecd = [x for x in Ax if x in L]
len(Ax_oecd)
not_found = [x for x in L if not x in Ax]
not_found


# In[37]:


A3=[]
for t in not_found :
    for x in Ax:
        if x.find(t)>-1:
            A3.append(x)
A3


# In[39]:


Ax


# In[42]:


#d1 = df1.set_index("index")["val"].to_dict()
#d2 = df2.set_index("index")["val"].to_dict()
d1s = set([eval(k)[0] for k in d1.keys()])
d2s = set([eval(k)[0] for k in d2.keys()])


# In[43]:


len(d1s),len(d2s)


# In[44]:



not_found = [x for x in L if not x in d1s]
not_found


# In[46]:


A3=[]
for t in not_found :
    for x in d1s:
        if x.find(t)>-1:
            A3.append(x)
A3


# In[47]:


os.listdir("../covid-policy-tracker/data")


# In[48]:


OxWorld = pd.read_csv("../covid-policy-tracker/data/OxCGRT_latest.csv")


# In[49]:


list(OxWorld.columns)


# In[53]:


#set (OxWorld['RegionName'].astype(str) )
OxWorld[OxWorld['RegionName']=="Alabama"].shape


# In[56]:


def add_st(s):
    if len(s)==1:
        return "0"+s
    return s
def convert_dates(dt):
    spl = parse(dt,dayfirst=False)
    return str(spl.year)+"-"+add_st(str(spl.month))+"-"+add_st(str(spl.day))
OxWorld["Date"] = OxWorld.apply(lambda r: convert_dates(str(r["Date"])),axis=1 )


# In[57]:


OxWorld["Date"].head()


# In[61]:


set(OxWorld["C2_Workplace closing"].astype(str))


# In[66]:


list(OxWorld["C2_Workplace closing"].astype(str)).count('nan')


# In[91]:


OxWorld2 = OxWorld[OxWorld["Date"]>"2020-03-01"]
S1 = set(OxWorld2[OxWorld2.apply(lambda r: str(r["C2_Flag"]) !='nan',axis=1)]["CountryName"])
S2 = set(OxWorld2["CountryName"])
Germany = OxWorld2[OxWorld2["CountryName"]=="Germany"]
len(S1),len(S2),(S2-S1)


# In[100]:


#Germany[Germany["C1_Flag"] =='nan'].shape
Alabama = OxWorld2[OxWorld2["RegionName"]=="Alabama"]
NewYork = OxWorld2[OxWorld2["RegionName"]=="New York"]
Germany.head()


# In[98]:


Alabama.head()


# In[101]:


NewYork[50:].head()


# In[112]:


def c_func(cols):
    #str(r[country_col])+("_"+str(r[province_col])).replace("_nan","")
    def lm(r):
        s = str(r[cols[0]]).replace("United States","US")
        for c in cols[1:]:
            s+= ("_"+str(r[c])).replace("_nan","")
        return s
    return lm
key_cols2 = ['CountryName','RegionName']
key_cols_func = c_func
OxWorld2["key"] = OxWorld2.apply(lambda r: key_cols_func(key_cols2)(r),axis=1)


# In[113]:


list(set(OxWorld2["key"]))[:20]


# In[114]:


A = set([eval(k)[0] for k in d1.keys()])


# In[115]:


B= set(OxWorld2["key"])
len(A),len(B)


# In[119]:


intersect= A.intersection(B)
len(intersect)


# In[117]:


list([x for x in B  if not x in A])


# In[118]:


list([x for x in A  if not x in B])


# In[120]:


list(intersect)[:10]


# In[125]:


[x for x in Ax_oecd if not x in intersect],len(Ax_oecd),len([x for x in intersect if x.find("US_")>-1])


# In[126]:


OxWG = {k:v for k,v in OxWorld2.groupby("key")}


# In[127]:


OxWG["Germany"].head()


# In[147]:


from collections import OrderedDict
def prep_data_with_dt(file_name,col_name,key_cols,key_cols_func):
    df = pd.read_csv(file_name)
    #df.dropna(subset=["C1_School closing"],axis=0)
    df = df[df["C1_School closing"].astype(str)!="nan"]
    df["Date"] = OxWorld.apply(lambda r: convert_dates(str(r["Date"])),axis=1 )
    df = df.set_index("Date")
    df["key"] = df.apply(lambda r: key_cols_func(key_cols)(r),axis=1)
    return OrderedDict([(k,v) for k,v  in df.groupby("key")])
    
    
    


# In[148]:


file_name = "../covid-policy-tracker/data/OxCGRT_latest.csv"
col_name =""
OxWN = prep_data_with_dt(file_name,col_name,key_cols2,key_cols_func)


# In[145]:


OxWN["Israel"].shape


# In[146]:


from FeatureGenerator import *


# In[150]:


col_name = "C1_School closing"
dct1= create_df_dict(file_name=file_name,col_name=col_name,key_cols=key_cols2,key_cols_func=key_cols_func,prep_data=prep_data_with_dt,rep_indexes=False,dict_indexes={})


# In[154]:


lags = [1,7,14,28,56]
lags2 = [7,14,28]
col_tar = col_name
add = 0
DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols2,key_cols_func=key_cols_func,prep_data=prep_data_with_dt)


# In[159]:


DF4.tail()


# In[160]:


list(dict_indexes.keys())[2500:2510]


# In[ ]:




