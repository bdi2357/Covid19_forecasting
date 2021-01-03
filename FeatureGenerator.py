#!/usr/bin/env python
import pandas as pd
import os,sys,re
import numpy as np
from collections import OrderedDict
import random,time
from dateutil.parser import parse

os.getcwd()

def add_st(s):
    if len(s)==1:
        return "0"+s
    return s
def convert_US(file_name,key_col):
    df = pd.read_csv(file_name)
    dfs = df.groupby(key_col).sum()
    dfs2 = dfs[list(dfs.columns[10:])]
    df_mod_t = dfs2.T
    df_mod_t.index = df_mod_t.apply(lambda r: convert_dates(r.name),axis=1 )
    DF_countries = {c:pd.DataFrame(df_mod_t[c]) for c in df_mod_t.columns}
    return DF_countries

"""
def convert_dates(dt):
    spl = dt.split("/")
    return spl[2]+"-"+add_st(spl[0])+"-"+add_st(spl[1])
"""
def convert_dates(dt,dayfirst=False):
    spl = parse(dt,dayfirst=False)
    return str(spl.year)+"-"+add_st(str(spl.month))+"-"+add_st(str(spl.day))




def prepare_data_with_indexing(gf,column_rep,fix_index = False,add = 0):
    for k in gf.keys():
        gf[k] = gf[k].rename(columns={k:column_rep})
        #gf[k]["daily_"+column_rep] = gf[k][column_rep].diff(1).fillna(0)
        if fix_index:
            gf[k].index = gf[k].apply(lambda r: (k,r.name),axis=1)
        
    df_all =  pd.concat(gf.values())#, keys=gf.keys())
    df_all  = df_all.sort_index()
    L  = list(df_all.index.values)
    E = list(enumerate(L))
    
    dict_indexes = {x[1]:x[0]+add for x in E}
    dict_indexes_rev = {x[0]+add:x[1] for x in E}
    df_all.index = [x[0]+add for x in E]
    #return D_real_indexes_vals,df_all

    return df_all,dict_indexes,dict_indexes_rev





# In[144]:


start = time.time()
def dates_expr(d):
    try: 
        parse(d)
        return True

    except ValueError:
        return False
    return False
    #return len(re.findall('[0-9]+/[0-9]+/[0-9]+',d)) >0 and d == re.findall('[0-9]+/[0-9]+/[0-9]+',d)[0]
#def create_df_dict(file_name,col_name,country_col='Country/Region',province_col='Province/State',rep_indexes=False,dict_indexes={}):
def prep_data_dt_cols(file_name,col_name,key_cols,key_cols_func):
    df = pd.read_csv(file_name)
    df["key"] = df.apply(lambda r: key_cols_func(key_cols)(r),axis=1)
    df = df.groupby("key").sum()
    date_cols = [c for c in df.columns if dates_expr(c)]  
    df_mod = df[date_cols]
    df_mod_t = df_mod.T
    df_mod_t.index = df_mod_t.apply(lambda r: convert_dates(r.name),axis=1 )
    return OrderedDict([ (c,pd.DataFrame(df_mod_t[c])) for c in df_mod_t.columns])


def create_df_dict(file_name,col_name,key_cols,key_cols_func,prep_data,rep_indexes=False,dict_indexes={},prefix = "daily"):
    
    DF_countries = prep_data(file_name,col_name,key_cols,key_cols_func)
    for c in DF_countries.keys():
        DF_countries[c] = DF_countries[c].rename(columns={c:col_name})
        if col_name.find(prefix) == -1:
            
            DF_countries[c][col_name] = DF_countries[c][col_name].replace('.','0.0')
            DF_countries[c][col_name] = DF_countries[c][col_name].astype(float)
            DF_countries[c][prefix+col_name] = DF_countries[c][col_name].diff(1).fillna(0)
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

#directional operators


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
def mx(df,col,back):
    return df[col].rolling(back).max()
def mn(df,col,back):
    return df[col].rolling(back).min()
def local_cvt(df,col,back):
    return df[col].diff(back).fillna(0.0) -df[col].shift(back).fillna(0.0).diff(back).fillna(0.0)


#basic functions
def minus(a,b):
    return a-b
def plus(a,b):
    return a+b
def divide(a,b):
    return a/b
def mul(a,b):
    return a*b
def cond(a,b):
    if (a>0):
        return b
    else:
        return 0

#df[m1+o["name"]+m2] = o["func"](df[m1].values,df[m2].values)
operators_l=[]
operators_l.append({"name": "_div_","func":divide})


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

def df_specific_generate_directional_features(df,func_dict,lags2):
    func_dict_keys = list(func_dict.keys())
    for func_name in tqdm(func_dict_keys):
        params_d = func_dict[func_name]["params"].copy()
        params_d["df"] = df

        df[func_name] = func_dict[func_name]["func"](**params_d)
        for lg2 in lags2:
            df[func_name + "_lag_%d" % lg2] = df[func_name].shift(lg2)
    


def genetate_directional_features(frame,gf,func_dict,lags2):
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





def main_generator(file_name,col_name,lags,lags2,col_tar,add,key_cols,key_cols_func,prep_data,dict_indexes={},initialize_features_func_directional=initialize_features_func_directional):

    daily_col = "daily_"+col_tar
    start = time.time()
    DF_all = create_df_dict(file_name=file_name,col_name=col_name,key_cols=key_cols,key_cols_func=key_cols_func,prep_data=prep_data,dict_indexes=dict_indexes)
    print(round(time.time()-start,2))
    start = time.time()
    functions_dict_deaths = initialize_features_func_directional(lags,daily_col)
    print(round(time.time()-start,2))
    
    #gf,column_rep,fix_index = False,add = 0
    DF4,dict_indexes,dict_indexes_rev = prepare_data_with_indexing(gf=DF_all,column_rep= col_tar,add=add)
    start = time.time()
    #DF_d = create_df_dict(file_name,col_name,country_col,province_col,True,dict_indexes)
    DF_d = create_df_dict(file_name=file_name,col_name=col_name,key_cols=key_cols,key_cols_func=key_cols_func,rep_indexes=True,dict_indexes=dict_indexes,prep_data=prep_data)

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
    genetate_directional_features(DF4,DF_d,dict_deaths,lags2)
    print(round(time.time()-start,2))
    return DF4,dict_indexes



