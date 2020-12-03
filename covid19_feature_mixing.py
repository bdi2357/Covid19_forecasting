import pandas as pd
import os,sys,re
import numpy as np
from collections import OrderedDict
import random,time
from dateutil.parser import parse
from FeatureGenerator import *

def covid_19_feature_mixing(DFC_complete,operators,prefix1,prefix2,filter_strings1,filter_strings2,groupby_column = "Country_Province"):	
	gfn = OrderedDict([(k,v) for k,v in DFC_complete.groupby(groupby_column)])
	dfn = DFC_complete 
	start_t = time.time()
	for k in gfn.keys():
	    
	    filter_strings1 =[]
	    filter_strings2 =[]
	    operators = operators
	    a_c =feature_mixing(gfn[k], prefix1,prefix2, filter_strings1,filter_strings1,operators)
	print("a_c is : ",a_c)
	print("calc features time is %0.2f"%(time.time()-start_t))
	from_gf_to_df(dfn,gfn,a_c)
	return dfn
def extd_feature_mixing(DFC_complete,varbs):
	for params in varbs:
		DFC_complete = covid_19_feature_mixing(DFC_complete,**params)
	return DFC_complete






if __name__ == "__main__":
	prefix1 = "daily_deaths_sum_7"
	prefix2 = "daily_confirmed_sum_7"
	groupby_column = "Country_Province"

	########################################################
	for k in gfn.keys():
		prefix1 = "daily_deaths_sum_28"
		prefix2 = "daily_deaths_sum_28"
		filter_strings1 =["lag_1","lag_28","lag_14"]
		filter_strings2 =["lag_1","lag_7"]
		operators = operators_l
		a_c =feature_mixing(gfn[k], prefix1,prefix2, filter_strings1,filter_strings2,operators)
	from_gf_to_df(dfn,gfn,a_c)
	gfn = OrderedDict([(k,v) for k,v in dfn.groupby("Country_Province")])


	for k in gfn.keys():
		prefix1 = "daily_confirmed_sum_28"
		prefix2 = "daily_confirmed_sum_28"
		filter_strings1 =["lag_1","lag_28","lag_14"]
		filter_strings2 =["lag_1","lag_7"]
		operators = operators_l
		a_c =feature_mixing(gfn[k], prefix1,prefix2, filter_strings1,filter_strings2,operators)
	print("calc features time is %0.2f"%(time.time()-start_t))
	from_gf_to_df(dfn,gfn,a_c)
	dfn.to_csv(os.path.join(output_path,"test_feature_mixing_covid19_wUSM2.csv"),index_label="index")
	print("TOTAL including feature mixing %0.2f"%(time.time() - start_main))
	#dict_indexes.update(**{str(k): v for k, v in dict_indexes2.items()})
	for k in dict_indexes2.keys():
		dict_indexes[k] = dict_indexes2[k]
	DF_ind = pd.DataFrame(list(dict_indexes.items()),columns=["index","val"])
	DF_ind.to_csv(os.path.join(output_path,"test_covid19_wUSN3_index.csv"),index=False)
