import pandas as pd
import os,sys,re
import geopy.distance
import time
import argparse
st = time.time()
glob1 = pd.read_csv("csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
glob2 = pd.read_csv("week4/train.csv")

glob1["key"] = glob1.apply(lambda r: str(r['Province/State'])+"_"+ str(r['Country/Region']),axis=1)

glob2["key"] = glob2.apply(lambda r: str(r['Province_State'])+"_"+ str(r['Country_Region']),axis=1)

globU = pd.read_csv("csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv")
globU["key"] = globU.apply(lambda r: str(r['Province_State'])+"_"+ str(r['Country_Region']),axis=1)

dict_lat = glob1.set_index("key")["Lat"].to_dict()

dict_long = glob1.set_index("key")["Long"].to_dict()


dict_long.update(globU.set_index("key")["Long_"].to_dict())
dict_lat.update(globU.set_index("key")["Lat"].to_dict())

glob2["Lat"] = glob2.apply(lambda r: dict_lat[r["key"]],axis=1)
glob2["Long"] = glob2.apply(lambda r: dict_long[r["key"]],axis=1)

glob2[glob2["key"] == 'nan_Italy'].iloc[0]["Lat"]

def dist_k(k1,k2):
    return geopy.distance.geodesic( (dict_lat[k1],dict_long[k1]),(dict_lat[k2],dict_long[k2])).km
def generate_closest_distances(dict_lat,dict_long,output_dir):

    kys = list(dict_lat.keys())
    df = pd.DataFrame(columns=kys)
    print(kys[22])
    print(dict_lat[kys[22]],dict_long[kys[22]])
    print( [kkk for kkk in kys if str(dict_lat[kkk])=="nan"]) 
    print([kkk for kkk in kys if kkk[-3:] == "_US" ])
    bad_cord = ['US Military_US', 'Federal Bureau of Prisons_US', 'Veteran Hospitals_US']
    US_not_bad = [kkk for kkk in kys if kkk[-3:] == "_US"  and not kkk in bad_cord]
    bad_cord_lat = sum([dict_lat[x] for x in US_not_bad])/float(len(US_not_bad))
    bad_cord_long = sum([dict_long[x] for x in US_not_bad])/float(len(US_not_bad))
    for xx in bad_cord:
        dict_lat[xx] = bad_cord_lat
        dict_long[xx] = bad_cord_long


    for kk in kys:
        df.loc[kk] = [dist_k(kk,c) for c in df.columns]


    dict_5_closest = {k : '|'.join(list(df.loc[k].sort_values(axis=0)[1:6].index.values)) for k in kys}
    dict_10_closest = {k : '|'.join(list(df.loc[k].sort_values(axis=0)[1:11].index.values)) for k in kys}
    dict_30_closest = {k : '|'.join(list(df.loc[k].sort_values(axis=0)[1:31].index.values)) for k in kys}
    
    print("Total time : %0.2f"%(time.time()-st))
    D_nc = {"ClosestCountries5": dict_5_closest,"ClosestCountries10":dict_10_closest,"ClosestCountries30":dict_30_closest}
    closest = pd.DataFrame(columns=["ClosestCountries5","ClosestCountries10","ClosestCountries30"])
    for kk in kys:
        closest.loc[kk] = [D_nc[c][kk] for c in closest.columns]
    closest.to_csv("closest.csv")

if __name__ == "__main__":
    



