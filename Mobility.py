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
TA.columns


# In[7]:


TA[["date",'workplaces_percent_change_from_baseline']].tail()


# In[8]:


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


f_n = "test_deathsNconfirmed_covid19_wUS.csv"
output_path = "/Users/itaybd/output_covid"
dfn = pd.read_csv(os.path.join(output_path,f_n),index_col="index")


# In[18]:


list(dfn.columns)


# In[19]:


#basic functions
def minus(a,b):
    return a-b
def plus(a,b):
    return a+b
def divide(a,b):
    return a/b
def mul(a,b):
    return a*b

#df[m1+o["name"]+m2] = o["func"](df[m1].values,df[m2].values)
operators_l=[]
operators_l.append({"name": "_div_","func":divide})
dfn.shape


# In[20]:


def feature_mixing(df, prefix1,prefix2, filter_strings1,filter_strings2,operators):
    added_cols = []
    def list_comp_str(c,lst):
        for st in lst:
            if c.find(st)>-1:
                return False
        return True

    mix1 = [c for c in df.columns if c[0:len(prefix1)] == prefix1 and list_comp_str(c,filter_strings1)]
    mix2 = [c for c in df.columns if c[0:len(prefix2)] == prefix2 and list_comp_str(c,filter_strings2)]
    for m1 in mix1:
        for m2 in mix2:
            for o in operators:
                df[m1+o["name"]+m2] = o["func"](df[m1].values,df[m2].values)
                added_cols.append(m1+o["name"]+m2)
    return added_cols


# In[21]:


gfn = {k:v for k,v in dfn.groupby("Country_Province")}


# In[22]:


gfn["Israel"].tail()


# In[23]:


isr =  gfn["Israel"].copy()
isr[['daily_deaths_sum_7',
 'daily_deaths_sum_7_lag_7',
 'daily_deaths_sum_7_lag_14',
 'daily_deaths_sum_7_lag_28']].tail()


# In[24]:


prefix1 = "daily_deaths_sum_7"
prefix2 = "daily_confirmed_sum_7"
filter_strings1 =[]
filter_strings2 =[]
operators = operators_l
feature_mixing(isr, prefix1,prefix2, filter_strings1,filter_strings2,operators)


# In[25]:


LL = list(isr.columns)[-10:]
isr[LL].tail()


# In[26]:


prefix1 = "daily_deaths_sum_28"
prefix2 = "daily_deaths_sum_28"
filter_strings1 =["lag_28","lag_14"]
filter_strings2 =["lag_1","lag_7"]
operators = operators_l
feature_mixing(isr, prefix1,prefix2, filter_strings1,filter_strings2,operators)


# In[27]:


LL = list(isr.columns)[-10:]
isr[LL].tail()


# In[59]:


prefix1 = "daily_confirmed_sum_28"
prefix2 = "daily_confirmed_sum_28"
filter_strings1 =["lag_28","lag_14"]
filter_strings2 =["lag_1","lag_7"]
operators = operators_l
feature_mixing(isr, prefix1,prefix2, filter_strings1,filter_strings2,operators)


# In[60]:


LL = list(isr.columns)[-10:]
isr[LL].tail()


# In[61]:


import time
start_t = time.time()
for k in gfn.keys():
    prefix1 = "daily_deaths_sum_7"
    prefix2 = "daily_confirmed_sum_7"
    filter_strings1 =[]
    filter_strings2 =[]
    operators = operators_l
    a_c =feature_mixing(gfn[k], prefix1,prefix2, filter_strings1,filter_strings2,operators)
print("calc features time is %0.2f"%(time.time()-start_t))
print(a_c)
    


# In[64]:


def from_gf_to_df(frame,gf,cols):
    #frame[col] = frame.apply(lambda r: gf[r.name[0]].at[r.name[1],col],axis=1)
    if isinstance(cols,str):
        frame[cols] = pd.concat([gf[k][cols] for k in gf.keys()])
    else:
        for col in cols:
            frame[col] = pd.concat([gf[k][col] for k in gf.keys()])
#from_gf_to_df(frame,gf,[func_name]+[func_name+"_lag_%d"%lg2 for lg2 in lags2])


# In[65]:


start_t2 = time.time()
from_gf_to_df(dfn,gfn,a_c)
print("from_gf_to_df time : %0.2f"%(time.time()- start_t2))


# In[66]:


gfn2 = {k:v for k,v in dfn.groupby("Country_Province")}


# In[67]:


isr2 = gfn2["Israel"]


# In[68]:


isr[['daily_deaths_sum_7',
 'daily_deaths_sum_7_lag_7',
 'daily_deaths_sum_7_lag_14',
 'daily_deaths_sum_7_lag_28']].tail()


# In[69]:


isr2[['daily_deaths_sum_7',
 'daily_deaths_sum_7_lag_7',
 'daily_deaths_sum_7_lag_14',
 'daily_deaths_sum_7_lag_28']].tail()


# In[70]:


isr["daily_deaths_sum_7_lag_7_div_daily_confirmed_sum_7_lag_14"].tail()


# In[71]:


isr2["daily_deaths_sum_7_lag_7_div_daily_confirmed_sum_7_lag_14"].tail()


# In[72]:


dfn.shape


# In[73]:


"Country_Province" in dfn.columns


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




