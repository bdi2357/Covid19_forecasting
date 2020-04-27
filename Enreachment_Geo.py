import pandas as pd
import os,sys,re
from functools import reduce
import time

from tqdm import tqdm

def from_gf_to_df(frame,gf,col,key="key"):
	"""
	Name : from_gf_to_df
	Description : turn dictionary of dataframe with specific column to column in big dataframe

	"""
	
	frame[col] = frame.apply(lambda r: gf[r[key]].loc[r["Date"],col],axis=1)

def processing(df,key = "key",ind = "Date"):
	"""
	name : processing
	Description: turn dataframe to dictionary of dataframe indexed by prespecified column
	"""
    key_engineering(df,key_cols)
    gf = {k:v for k,v in df.groupby(key)} 
    for k in gf.keys():
        gf[k] =gf[k].set_index(ind)
    return gf

def get_cross_sectional_features(features,d_similar,gf,DF,nm,sep = "|"):
	"""
	name: 
	"""
	for f in tqdm(features):
		col = nm+"|"+f
		#print(list(gf.keys())[:10])
		#print("*"*22)
		gf_keys = list(gf.keys())
		for cur in tqdm(gf_keys):
			keys = d_similar[cur].split("|")
			#print("keys :",keys)
			dfs = [gf[k][f] for k in keys if k in gf.keys()]
			gf[cur][col]  = reduce(lambda x, y: x.add(y, fill_value=0), dfs) #sum(DF[keys])
		from_gf_to_df(DF,gf,col)
	return
def get_all(features,gf,DF,nm,sep = "|"):
	for f in tqdm(features):
		col = nm+"|"+f
		gf_keys = list(gf.keys())
		dfs =  [gf[k][f] for k in  gf.keys()]
		all_res = reduce(lambda x, y: x.add(y, fill_value=0), dfs)
		for cur in gf_keys:
			gf[cur][col] = all_res
		from_gf_to_df(DF,gf,col)

if __name__ == "__main__":
	file_name = "train_with_features_fwd_looking_28.csv" #"train_with_features_9.csv"
	closest = pd.read_csv("closest.csv")
	print(closest.columns)
	closest = closest.rename(columns = {'Unnamed: 0':'key'})
	closest = closest.set_index('key')

	#features_d["ConfirmedCases_lag_1__1"]
	DF = pd.read_csv(file_name)
	fwd = 27
	look_fwd = "__%d"%fwd
	features = [c for c in DF.columns if len(re.findall(look_fwd,c))>0] #["ConfirmedCases_lag_1__1"]#,"ConfirmedCases_lag_2__1","ConfirmedCases_lag_3__1"]
	
	print( "important columns len is %d" %len([c for c in DF.columns if len(re.findall('__[0-9]+',c))>0]))
	print( "important %s columns len is %d" %(look_fwd,len([c for c in DF.columns if len(re.findall('__1',c))>0])))

	gf = {k:v for k,v in DF.groupby("key")} 
	for k in gf.keys():
		gf[k] =gf[k].set_index("Date")
	print(set(closest.index.values) - set(gf.keys()))
	print("#"*40)
	print(set(gf.keys()) - set(closest.index.values) )
	print("-"*40)
	print(closest.head())
	
	d_similar = closest["ClosestCountries30"].to_dict()
	nm = "ClosestCountries30" #"All"
	st = time.time()
	get_cross_sectional_features(features,d_similar,gf,DF,nm,sep = "|")
	#get_all(features,gf,DF,nm,sep = "|")
	print("get_features_directional_features %d feature time is %0.2f"%(len(features),time.time()-st))
	print(DF[ ["key",nm+"|"+features[0]]].tail())
	DF.to_csv(file_name.split(".")[0] +"__%d_%s.csv"%(fwd,nm))










