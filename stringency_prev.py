#!/usr/bin/env python
# coding: utf-8

# In[2]:

from FeatureGenerator import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time



from collections import OrderedDict


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
def prep_data_with_dt(file_name,col_name,key_cols,key_cols_func):
    df = pd.read_csv(file_name)
    #df.dropna(subset=["C1_School closing"],axis=0)
    df = df[df["C1_School closing"].astype(str)!="nan"]
    df["Date"] = df.apply(lambda r: convert_dates(str(r["Date"])),axis=1 )
    df = df.set_index("Date")
    df["key"] = df.apply(lambda r: key_cols_func(key_cols)(r),axis=1)
    return OrderedDict([(k,v) for k,v  in df.groupby("key")])
    
def initialize_features_func_directional2(lags,col_name):
    funcs_dict = OrderedDict()# OrderedDict()    
    for ii in lags:
        funcs_dict["%s_diff_%d"%(col_name,ii)] = {"func":diff,"params" : {"col":col_name,"back":ii}}
        funcs_dict["%s_max_%d"%(col_name,ii)] = {"func":mx,"params" : {"col":col_name,"back":ii}}
        funcs_dict["%s_min_%d"%(col_name,ii)] = {"func":mn,"params" : {"col":col_name,"back":ii}}

        
    return funcs_dict
    


# In[148]:


file_name = "../covid-policy-tracker/data/OxCGRT_latest.csv"



# In[146]:





# In[150]:


col_name = "C1_School closing"
#dct1= create_df_dict(file_name=file_name,col_name=col_name,key_cols=key_cols2,key_cols_func=key_cols_func,prep_data=prep_data_with_dt,rep_indexes=False,dict_indexes={})


# In[154]:
cils =[
 'C1_School closing',
 'C2_Workplace closing',
 'C3_Cancel public events',
 'C4_Restrictions on gatherings',
 'C5_Close public transport',
 'C6_Stay at home requirements',
 'C7_Restrictions on internal movement',
 'C8_International travel controls',
 'H6_Facial Coverings',
 'H6_Flag',
 'StringencyIndex',
 ]

D_all = {}
D_ind ={}
lags = [1,7,14,28,56]
lags2 = [7,14,28]
add = 0
CLL = ['C1_School closing','C2_Workplace closing','C3_Cancel public events']
for col_name in cils :
    col_tar = col_name
    D_all[col_name],D_ind[col_name] = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols2,key_cols_func=key_cols_func,prep_data=prep_data_with_dt,initialize_features_func_directional=initialize_features_func_directional2)
intersect_all = set.intersection(*[set(x.keys()) for x in D_ind.values()])

print("intersect_all len is: %d"%len(intersect_all)) 
indexes = sorted([D_ind[cils[0]][k] for k in intersect_all])
for col_name in cils :
    D_all[col_name] = D_all[col_name].loc[indexes]
DFC = pd.concat([D_all[col_name ] for col_name in cils ],axis=1)
DFC = DFC.loc[:,~DFC.columns.duplicated()]
"""
S=[]
for k in intersect_all:
    A =[]
    for c in cils:
        A.append(D_ind[c][k])
    if len(set(A))>1:
        S.append(k)
print("multiple indexes len is: %d"%len(S))
"""
dict_indexes = OrderedDict([(k,D_ind[cils[0]][k]) for k in intersect_all])
#for col_name in 

output_path = "/Users/itaybd/output_covid"
DFC.to_csv(os.path.join(output_path,"test_stringency2.csv"),index_label="index")
DF_ind = pd.DataFrame(list(dict_indexes.items()),columns=["index","val"])
DF_ind.to_csv(os.path.join(output_path,"test_stringency_index2.csv"),index=False)

# In[159]:


print(DF4.tail())


# In[160]:


list(dict_indexes.keys())[2500:2510]


# In[ ]:




