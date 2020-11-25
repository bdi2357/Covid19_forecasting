#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os,re
import pandas as pd
import wikipedia as wiki
import random

def get_age_data_world():
    p = wiki.page("List_of_countries_by_age_structure")
    df = pd.read_html(p.html())[0]
    for c in df.columns[1:]:
        df[c] = df.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
    age_c = df[[('Country','Country'), ('Population by age',  'Over 65[3]')]]
    age_c.columns = ["Country_Province","Over 65 Ratio"]
    return age_c

def get_age_data_us(file_name):
    state = pd.read_csv(file_name)
    age_u = state[['State','Age.Percent 65 and Older']]
    age_u['Age.Percent 65 and Older'] *= 0.01
    age_u.columns = ["Country_Province","Over 65 Ratio"]
    age_u['Country_Province'] = "US_" + age_u['Country_Province'] 
    return age_u

def create_age_unified_df(file_name):
    return pd.concat([get_age_data_world(),get_age_data_us(file_name)])



def get_urbanization_glob():    
    u = wiki.page("Urbanization_by_country")
    uf = pd.read_html(u.html())[0]
    c = 'Urban Population (%)'
    def to_val(s):
        if len(re.findall('[0-9]+\.[0-9]+',str(s)))>0:
            return float(re.findall('[0-9]+\.[0-9]+',str(s))[0])*0.01
        else:
            return 1.0
        
    uf[c] = uf.apply(lambda r: to_val(r[c]),axis=1 )
    ufn = uf[["Nation",'Urban Population (%)']]
    ufn.columns = ["Country_Province","Urban Population Ratio"]
    return ufn

def get_urbanization_usa(): 
    us = wiki.page("Urbanization_in_the_United_States")
    usf2 = pd.read_html(us.html())[0]
    usf2.columns = [x[1] for x in usf2.columns]
    usf3 = usf2[5:][['State/Territory','2010']]
    usf3['State'] = usf3.apply(lambda r: re.findall('[A-Za-z\ \-]+',r['State/Territory'])[0],axis=1)
    usf3['State_us'] = "US_" + usf3['State']
    usf4 = usf3[["State_us","2010"]]
    c="2010"
    usf4[c] = usf4.apply(lambda r: float(re.findall('[0-9]+\.[0-9]+',r[c])[0])*0.01,axis=1 )
    usf4.columns = ["Country_Province","Urban Population Ratio"]
    return usf4

def create_urban_unified_df():
    return pd.concat([get_urbanization_glob(),get_urbanization_usa()])



def concat_rel(cnct1,cnct2,key_col):
    cnct1_d = cnct1.set_index(key_col)
    cnct2_d = cnct2.set_index(key_col)
    ix2 = set(cnct1_d.index.values).intersection(set(cnct2_d.index.values))
    ix2 = sorted(list(ix2))
    cnct1_d = cnct1_d.loc[ix2]
    cnct2_d = cnct2_d.loc[ix2]
    cnct2_d = cnct2_d[~cnct2_d.index.duplicated(keep='first')]
    cnct1_d = cnct1_d.sort_index()
    cnct2_d = cnct2_d.sort_index()
    cnct1_d.shape,cnct2_d.shape,len(ix2),[x for x in cnct2_d.index.values if not  x in cnct1_d.index.values]
    return pd.concat([cnct1_d,cnct2_d],axis=1)

def create_urban_age_df(age_us_file_name,key_col):
    return concat_rel(create_age_unified_df(age_us_file_name),create_urban_unified_df(),key_col)

def update_existing_df(cnct_all,dc,ind_dc,rel_cols):
    s1 =set(cnct_all.index.values)
    dict_cols = {}
    for c in rel_cols:
        dict_cols[c] = cnct_all[c].to_dict()
    ind_dc_f = ind_dc[ind_dc.apply(lambda r:eval(r.name)[0] in s1,axis=1)]
    dc = dc.loc[list(ind_dc_f["val"])]
    for c in rel_cols:
        print(c)
        dc[c] = dc.apply(lambda r: dict_cols[c][r["Country_Province"]],axis=1)
    return dc

if __name__ == "__main__":
    age_us_file_name = "data/state_demographics.csv"
    key_col = "Country_Province"
    cnct_all =create_urban_age_df(age_us_file_name,key_col)
    print(cnct_all.shape,cnct_all.columns)
    s1 =set(cnct_all.index.values)
    dc  = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUSM2.csv",index_col="index")
    ind_dc = pd.read_csv("../output_covid/test_covid19_wUSN3_index.csv", index_col ="index")
    dc = update_existing_df(cnct_all,dc,ind_dc,list(cnct_all.columns))
    samp = random.sample(list(dc.index.values),10)
    print(dc.loc[samp][["Country_Province"]+list(cnct_all.columns)])
    exit(0)
    print("Country_Province" in dc.columns)
    print(ind_dc.shape)
    ind_dc_f = ind_dc[ind_dc.apply(lambda r:eval(r.name)[0] in s1,axis=1)]
    print(ind_dc_f.shape)
    print(len(set(ind_dc_f.apply(lambda r:eval(r.name)[0],axis=1))))
    print("#"*150)

    print(set(ind_dc_f.apply(lambda r:eval(r.name)[0],axis=1)))
    d1 = cnct_all["Over 65 Ratio"].to_dict()
    d2 = cnct_all["Urban Population Ratio"].to_dict()
    print(ind_dc_f.columns)
    dc = dc.loc[list(ind_dc_f["val"])]
    dc["Over 65 Ratio"] = dc.apply(lambda r: d1[r["Country_Province"]],axis=1)

    print(dc[["Country_Province","Over 65 Ratio"]].tail())
    """
    countries_l = set(ind_dc.apply(lambda r: eval(r.name)[0],axis=1))
    cnct_all = cnct_all[cnct_all.apply(lambda r: r[key_col] in countries_l,axis=1)]
    cnct_all_rel =  list(cnct_all[key_col])

    ind_dc["Country_Province"] = ind_dc.apply(lambda r: eval(r.name)[0],axis=1)
    """



# In[182]:





exit(0)

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

print(cnct_all.head())
print(cnct_all.tail())


# In[ ]:




