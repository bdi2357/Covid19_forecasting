#!/usr/bin/env python
# coding: utf-8

# In[2]:

from FeatureGenerator import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time
import inspect


from collections import OrderedDict

# In[177]:
start_all = time.time()


def integrate_frames(strg,ind_strg,dc,ind_dc):
    LL = (set(ind_dc.index.values).intersection(set(ind_strg.index.values)))
    MM = sorted(list(LL))
    ind_strg.to_csv("../index_strng_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))
    ind_dc.to_csv("../index_covidf_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))
    strg.to_csv("../strng_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))
    dc.to_csv("../covidf_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))


    ind_strng_d = ind_strg["val"].to_dict()
    ind_dc_d = ind_dc["val"].to_dict()
    ind_dc_reverse = {ind_dc_d[k]: k for k in ind_dc_d.keys()}
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
    new_d = {ind_dc_reverse[k]: k for k in complete.index.values}
    DF_ind = pd.DataFrame(list(new_d.items()),columns=["index","val"])
    DF_ind.to_csv("../index_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))

    return complete.loc[:,~complete.columns.duplicated()],DF_ind

if __name__ == "__main__":
    start = time.time()
    #strg = pd.read_csv("/Users/itaybd/output_covid/test_stringency2.csv",index_col="index")
    #dc  = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUSM2.csv",index_col="index")
    #ind_dc = pd.read_csv("../output_covid/test_covid19_wUSN3_index.csv", index_col ="index")
    #ind_strg = pd.read_csv("../output_covid/test_stringency_index2.csv",index_col = "index")
    ind_strg = pd.read_csv("../index_strng_stringency.py_integrate_frames.csv",index_col="index")
    ind_dc = pd.read_csv("../index_covidf_stringency.py_integrate_frames.csv",index_col="index")
    strg = pd.read_csv("/Users/itaybd/output_covid/stringency_features_2020-12-06.csv",index_col="index")
    dc  = pd.read_csv("../output_covid/test_deathsANDconfirmed_covid19_wUS_2020-11-01_<function last_git_commit_date at 0x1182a1a60>.csv",index_col="index")
    print("download time is %0.2f" %(time.time()-start))
    integ_st = time.time()
    c,d = integrate_frames(strg,ind_strg,dc,ind_dc)
    print(c.shape)
    print(d.head(),d.tail())
    print("integrate_frames time is %0.2f" %(time.time()-integ_st))
    exit(0)
# In[184]:
"""
exit(0)

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

"""


