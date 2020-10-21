#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os,re,sys
from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")
dest_dir = "modified_covid19_ts" 
if not os.path.isdir(dest_dir):
    os.mkdir(dest_dir)

D = {"deaths":"Fatalities","confirmed":"ConfirmedCases"}
for ky in D.keys():
    df = pd.read_csv("/Users/itaybd/covid19/COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_%s_global.csv"%ky)



    # In[6]:


    dates = list(df.columns[4:])



    # In[8]:


    df["key"] = df.apply(lambda r: str(r["Province/State"]) +"_"+ str(r["Country/Region"]),axis=1)


    # In[9]:


    df2 = df.set_index("key")[dates]
    df2.head()


    # In[32]:


    new_df = pd.DataFrame(columns= list(df.columns[:4])+["Date",D[ky]])
    print(new_df.columns)
    xdf = df.set_index("key")
    df.shape,xdf.shape


    # In[33]:


    count = 0
    import time
    st = time.time()
    for v in xdf.index.values:
        print("key ",v)
        rw = xdf.loc[v]
        for c in df.columns[4:-1]:
            #print("#"*22)
            #print(rw)
            #print(c)
            #print(rw[c])
            #print([rw[df.columns[jj]] for jj in range(4)] )
            mm = [rw[df.columns[jj]] for jj in range(4)] +[c]+[rw[c]]
            #print(mm,len(mm))
            new_df.loc[count] = mm#[ [rw[df.columns[jj]] for jj in range(4)] +[c]+[rw[c]]]
            count+=1
    new_df.head(),new_df.tail()
    print("total running time %0.3f"%(time.time() - st))



    new_df.to_csv(os.path.join(dest_dir,"%s_global.csv"%D[ky]),index = False)


    new_df.tail()

conf1 = pd.read_csv(os.path.join(dest_dir,"%s_global.csv"%D["confirmed"]))
fatal1 = pd.read_csv(os.path.join(dest_dir,"%s_global.csv"%D["deaths"]))
merged = conf1.merge(fatal1, on=['Province/State','Country/Region',"Date"])
#merge2 = pd.merge(conf1, fatal1, on=['Province/State','Country/Region',"Date"], how='inner')
print(merged.tail())
print("#"*50)
merged_m = merged.drop(columns=[c for c in merged.columns if c.find("Unname")>-1 or  c.find("_y")>-1])
print(merged_m.tail())
print("$"*50)
merged_m = merged_m.rename(columns={c:c.replace("_x","") for c in merged_m.columns})
print(merged_m.tail())
def sp_dt(dt):
    d = dt.split("/")
    if len(d[1]) ==1:
        d1 = '0'+d[1]
    else:
        d1 = d[1]
    if len(d[0]) == 1:
        d0 = '0'+d[0]
    else:
        d0=d[0]
    return "20"+d[2]+"-"+d0+"-"+d1
merged_m["Date"] = merged_m.apply(lambda r: sp_dt(r["Date"]),axis=1)
merged_m.to_csv(os.path.join(dest_dir,"merged_global%s.csv"%today),index=False)


# In[ ]:




