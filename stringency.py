#!/usr/bin/env python
# coding: utf-8

# In[2]:

from FeatureGenerator import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time



from collections import OrderedDict

# In[177]:
start_all = time.time()


def integrate_frames(strg,dc,ind_dc,ind_strg):
    LL = (set(ind_dc.index.values).intersection(set(ind_strg.index.values)))
    MM = sorted(list(LL))
    ind_strng_d = ind_strg["val"].to_dict()
    ind_dc_d = ind_dc["val"].to_dict()
    reverse_d1 = {ind_strng_d[m]:m for m in MM}
    new_ind_dict = {k:ind_dc_d[reverse_d1[k]] for k in reverse_d1.keys()}
    S = set(new_ind_dict.keys())
    S2 = set(new_ind_dict.values())
    strg_f = strg[strg.apply(lambda r: r.name in S,axis =1)]
    dc_f = dc[dc.apply(lambda r: r.name in S2,axis =1)]
    dc_f.shape
    strg_f["new index"] = strg_f.apply(lambda r: new_ind_dict[r.name] ,axis=1 )
    strg_f = strg_f.set_index("new index")
    strg_f = strg_f.sort_index()
    dc_f = dc_f.sort_index()
    complete = pd.concat([dc_f,strg_f],axis=1)
    complete = complete.loc[:,~complete.columns.duplicated()]



strg = pd.read_csv("/Users/itaybd/output_covid/test_stringency2.csv",index_col="index")


# In[181]:


dc  = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUSM2.csv",index_col="index")


# In[182]:


ind_dc = pd.read_csv("../output_covid/test_covid19_wUSN3_index.csv", index_col ="index")
ind_strg = pd.read_csv("../output_covid/test_stringency_index2.csv",index_col = "index")


# In[184]:


ind_dc.shape,ind_strg.shape


# In[186]:


LL = (set(ind_dc.index.values).intersection(set(ind_strg.index.values)))


# In[188]:


list(LL)[7000:7010],len(LL)


# In[203]:


MM = sorted(list(LL))
print(ind_strg.columns)
ind_strng_d = ind_strg["val"].to_dict()
ind_dc_d = ind_dc["val"].to_dict()
reverse_d1 = {ind_strng_d[m]:m for m in MM}
new_ind_dict = {k:ind_dc_d[reverse_d1[k]] for k in reverse_d1.keys()}
S = set(new_ind_dict.keys())
S2 = set(new_ind_dict.values())
strg_f = strg[strg.apply(lambda r: r.name in S,axis =1)]
strg_f.shape


# In[204]:


dc_f = dc[dc.apply(lambda r: r.name in S2,axis =1)]
dc_f.shape


# In[205]:


strg_f["new index"] = strg_f.apply(lambda r: new_ind_dict[r.name] ,axis=1 )
strg_f = strg_f.set_index("new index")
strg_f = strg_f.sort_index()
dc_f = dc_f.sort_index()
complete = pd.concat([dc_f,strg_f],axis=1)
complete = complete.loc[:,~complete.columns.duplicated()]
complete.shape

complete.to_csv("../output_covid/test_with_stringency.csv",index_label ="index")

# In[206]:

print("Total time : %0.2f"%(time.time()-start_all))

print(complete.shape)

print(len(set(strg_f.index.values)- set(dc_f.index.values)),len(set(dc_f.index.values)- set(strg_f.index.values)))


# In[207]:


#print(list(complete.columns))


# In[ ]:




