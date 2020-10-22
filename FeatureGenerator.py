#!/usr/bin/env python
# coding: utf-8

# In[134]:


import pandas as pd
import os,sys,re
import numpy as np
from collections import OrderedDict
import random,time
os.getcwd()

start_all = time.time()
# In[135]:
#global dict_indexes #= OrderedDict()
#global dict_indexes_rev #= OrderedDict()
def create_covid_coutry_date_DataFrame(file_name,country_col = 'Country/Region',province_col= 'Province/State'):
    df = pd.read_csv(file_name)
    df["key"] = df.apply(lambda r: str(r[country_col])+("_"+str(r[province_col])).replace("_nan",""),axis=1)
    df = df.set_index("key")
    df_mod_t = df_mod.T
    DF_countries = {c:pd.DataFrame(df_mod_t[c]) for c in df_mod_t.columns}
    return DF_countries



def prepare_covid(gf,column_rep,fix_index = False):
    for k in gf.keys():
        gf[k] = gf[k].rename(columns={k:column_rep})
        #gf[k]["daily_"+column_rep] = gf[k][column_rep].diff(1).fillna(0)
        if fix_index:
            gf[k].index = gf[k].apply(lambda r: (k,r.name),axis=1)
        
    df_all =  pd.concat(gf.values())#, keys=gf.keys())
    df_all  = df_all.sort_index()
    L  = list(df_all.index.values)
    E = list(enumerate(L))
    print("HERE"*30)
    dict_indexes = {x[1]:x[0] for x in E}
    dict_indexes_rev = {x[0]:x[1] for x in E}
    df_all.index = [x[0] for x in E]
    #return D_real_indexes_vals,df_all

    return df_all,dict_indexes,dict_indexes_rev
     





# In[143]:

orig_path = "/Users/itaybd/covid19/COVID-19/new_covid/COVID-19/"

file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
col_name = "deaths"
lags = [1,7,14,28,56]
lags2 = [7,14,28]


# In[144]:


start = time.time()
def create_df_dict(file_name,col_name,rep_indexes=False,dict_indexes={}):
    df = pd.read_csv(file_name)
    df["key"] = df.apply(lambda r: str(r['Country/Region'])+("_"+str(r['Province/State'])).replace("_nan",""),axis=1)
    df = df.set_index("key")
    df_mod = df[df.columns[4:]]
    df_mod_t = df_mod.T
    DF_countries = {c:pd.DataFrame(df_mod_t[c]) for c in df_mod_t.columns}
    for c in DF_countries.keys():
        DF_countries[c] = DF_countries[c].rename(columns={c:col_name})
        if col_name.find("daily") == -1:
            DF_countries[c]["daily_"+col_name] = DF_countries[c][col_name].diff(1).fillna(0)
        DF_countries[c]["Date"] = DF_countries[c].index
        DF_countries[c]["Country_Province"] = c 
        if rep_indexes:
            DF_countries[c].index = DF_countries[c].apply(lambda r: dict_indexes[(c,r.name)],axis=1)
        else:
             DF_countries[c].index = DF_countries[c].apply(lambda r: (c,r.name),axis=1)
    return DF_countries
def add_lags(gf,lags,col_name):
    for c in gf.keys():
        
        for lg in lags:
            gf[c][col_name+"_lag_%d"%lg] = gf[c][col_name].shift(lg)
    return gf
round(time.time()-start,2)


# In[145]:


def lag(df,col,lag,fillna_val = 0.0):
	return df[col].shift(lag).fillna(0.0)
def ma(df,col,window):
	return df[col].rolling(window).mean()
def sum(df,col,window):
    return df[col].rolling(window).sum()
def std(df,col,window):
	return df[col].rolling(window).std()
def pct_change(df,col,back):
	return df[col].pct_change(back).fillna(0.0)
def diff(df,col,back):
    return df[col].diff(back).fillna(0.0)
def local_cvt(df,col,back):
    return df[col].diff(back).fillna(0.0) -df[col].shift(back).fillna(0.0).diff(back).fillna(0.0)


# In[146]:


def initialize_features_func_directional(lags,col_name):
    funcs_dict = OrderedDict()# OrderedDict()    
    for ii in lags:
        funcs_dict["%s_ma_%d"%(col_name,ii)] = {"func":ma,"params" : {"col":col_name,"window":ii}}
        funcs_dict["%s_sum_%d"%(col_name,ii)] = {"func":sum,"params" : {"col":col_name,"window":ii}}
        funcs_dict["%s_std_%d"%(col_name,ii)] = {"func":std,"params" : {"col":col_name,"window":ii}}
        #funcs_dict["%s_pct_%d"%(col_name,ii)] = {"func":pct_change,"params" : {"col":col_name,"back":ii}}
        funcs_dict["%s_diff_%d"%(col_name,ii)] = {"func":diff,"params" : {"col":col_name,"back":ii}}
        funcs_dict["%s_local_cvt_%d"%(col_name,ii)] = {"func":local_cvt,"params" : {"col":col_name,"back":ii}}

    return funcs_dict
def from_gf_to_df(frame,gf,cols):
    #frame[col] = frame.apply(lambda r: gf[r.name[0]].at[r.name[1],col],axis=1)
    if isinstance(cols,str):
        frame[cols] = pd.concat([gf[k][cols] for k in gf.keys()])
    else:
        for col in cols:
            frame[col] = pd.concat([gf[k][col] for k in gf.keys()])


# In[147]:


from tqdm import tqdm
def genetate_directional_features(frame,gf,func_dict,lags,lags2):
    func_dict_keys = list(func_dict.keys())
    for func_name in tqdm(func_dict_keys): #func_dict.keys():
            for kk in tqdm(gf.keys()):
                 params_d = func_dict[func_name]["params"].copy()
                 params_d["df"] = gf[kk]
                 
                 gf[kk][func_name] = func_dict[func_name]["func"](**params_d)
                 for lg2  in lags2 :
                    gf[kk][func_name+"_lag_%d"%lg2] = gf[kk][func_name].shift(lg2) 


                 """
                 for jj in lags:
                    gf[kk][func_name+"__%d"%jj] = gf[kk][func_name].shift(jj).fillna(0)
                 """
            start2= time.time()
            from_gf_to_df(frame,gf,[func_name]+[func_name+"_lag_%d"%lg2 for lg2 in lags2])

            print("start2:",round(time.time()-start2,2))
            """
            for lg in lags:
                from_gf_to_df(frame,gf,func_name)
                #from_gf_to_df(frame,gf,func_name+"__%d"%lg)
            """

            


# In[148]:

def main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar="deaths"):
    daily_col = "daily_"+col_tar
    start = time.time()
    DF_all = create_df_dict(file_name=file_name,col_name=col_name)
    print(round(time.time()-start,2))
    start = time.time()
    functions_dict_deaths = initialize_features_func_directional(lags,daily_col)
    print(round(time.time()-start,2))
    DF4,dict_indexes,dict_indexes_rev = prepare_covid(DF_all,col_tar)
    start = time.time()
    DF_d = create_df_dict(file_name,col_name,True,dict_indexes)
    print(round(time.time()-start,2))
    start = time.time()
    DF_d = add_lags(gf=DF_d,lags= lags,col_name = col_name)
    print(round(time.time()-start,2))
    start = time.time()

    for lg in lags:
        start2= time.time()
        from_gf_to_df(DF4,DF_d,col_name+"_lag_%d"%lg)
        print("start2:",round(time.time()-start2,2))
    print(round(time.time()-start,2))
    dict_deaths = initialize_features_func_directional(lags,daily_col)
    start = time.time()
    genetate_directional_features(DF4,DF_d,dict_deaths,lags,lags2)
    print(round(time.time()-start,2))
    return DF4

# In[154]:


#DF_d["Israel"][-5:],DF4.at[("Israel","10/13/20"),"deaths"],DF4.at[("Israel","10/13/20"),"deaths_lag_7"]


# In[155]:








DF4 = main_generator()

print("total time %0.2f"%(time.time() - start_all))
start3 = time.time()
if not os.path.isdir("/Users/itaybd/output_covid"):
    os.mkdir("/Users/itaybd/output_covid")
print(list(DF4.columns))

DF4.to_csv("/Users/itaybd/output_covid/test_deaths_covid19.csv",index_label="index")
print("save time %0.2f"%(time.time() - start3))



