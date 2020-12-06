from FeatureGenerator import *
from CovidFeatureGenerator import *
from stringency_prev import *
from stringency import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time
from collections import OrderedDict

if __name__ == "__main__" :
	start_all = time.time()
	df_complete,df_ind = main_CovidFeatureGen()
	main_CovidFeatureGen_time = time.time() - start_all

	file_name = "../covid-policy-tracker/data/OxCGRT_latest.csv"
	stringency_out_name = "test_stringency2"
	output_path = "/Users/itaybd/output_covid"
	strng,ind_strng = create_stringency_features(file_name,output_path,stringency_out_name)
	stringency_time = time.time()-start_all-main_CovidFeatureGen_time
	print("main_CovidFeatureGen_time is %0.2f"%main_CovidFeatureGen_time)
	print("stringency_time is %0.2f"%stringency_time)
	df_c,df_ind = integrate_frames(strng,ind_strng,df_complete,df_ind)
	integrate_frames_time = time.time()-start_all-main_CovidFeatureGen_time - stringency_time
	print("total time all is  %0.2f"%(time.time() - start_all))
	print("main_CovidFeatureGen_time is %0.2f"%main_CovidFeatureGen_time)
	print("stringency_time is %0.2f"%stringency_time)
	print("integrate_frames_time  is  %0.2f"%integrate_frames_time)
