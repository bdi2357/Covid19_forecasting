from Age_Urban_Data import *
from FeatureGenerator import *
from stringency_prev import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time
from collections import OrderedDict


if __name__ == "__main__":
	exit(0)
	start_stringency_features = time.time()
    file_name = "../covid-policy-tracker/data/OxCGRT_latest.csv"
    stringency_out_name = "test_stringency2"
    output_path = "/Users/itaybd/output_covid"
    create_stringency_features(file_name,output_path,stringency_out_name)
    print("total stringency_features %0.2f"%(time.time() - start_stringency_features))
	exit(0)
	start = time.time()
	strg = pd.read_csv("/Users/itaybd/output_covid/test_stringency2.csv",index_col="index")
	dc  = pd.read_csv("../output_covid/test_feature_mixing_covid19_wUSM2.csv",index_col="index")
	ind_dc = pd.read_csv("../output_covid/test_covid19_wUSN3_index.csv", index_col ="index")
	ind_strg = pd.read_csv("../output_covid/test_stringency_index2.csv",index_col = "index")
	print("download time is %0.2f" %(time.time()-start))
	integ_st = time.time()
	dc,ind_dc = integrate_frames(strg,ind_strg,dc,ind_dc)
	ind_dc = ind_dc.set_index("index")
	print("integrate_frames time is %0.2f" %(time.time()-integ_st))
	age_us_file_name = "data/state_demographics.csv"
	key_col = "Country_Province"
	cnct_all =create_urban_age_df(age_us_file_name,key_col)
	print(cnct_all.shape,cnct_all.columns)
	s1 =set(cnct_all.index.values)
	dc = update_existing_df(cnct_all,dc,ind_dc,list(cnct_all.columns))
	samp = random.sample(list(dc.index.values),10)
	print(dc.loc[samp][["Country_Province"]+list(cnct_all.columns)])
	print(dc.shape)
