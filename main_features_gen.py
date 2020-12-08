from FeatureGenerator import *
from CovidFeatureGenerator import *
from stringency_prev import *
from stringency import *
import pandas as pd
import re,os,sys
from dateutil.parser import parse
import time
from collections import OrderedDict
from pathlib import Path
import git
HOME = str(Path.home())
if __name__ == "__main__" :
	
	repo = git.Repo(".")
	commit = repo.head.commit 
	last_commit_dt =  commit.committed_datetime.strftime("%Y-%m-%d")

	start_all = time.time()
	covid_path = "/Users/itaybd/COVID-19/"
	output_path = os.path.join(HOME,"output_covid")
	df_complete,df_indN = main_CovidFeatureGen(covid_path,output_path)
	main_CovidFeatureGen_time = time.time() - start_all

	file_name = "../covid-policy-tracker/data/OxCGRT_latest.csv"
	stringency_out_name = "stringency_features_%s"%(last_commit_dt)
	strng,ind_strng = create_stringency_features(file_name,output_path,stringency_out_name)
	stringency_time = time.time()-start_all-main_CovidFeatureGen_time
	print("main_CovidFeatureGen_time is %0.2f"%main_CovidFeatureGen_time)
	print("stringency_time is %0.2f"%stringency_time)
	df_c,df_ind = integrate_frames(strng,ind_strng,df_complete,df_indN)
	#last_dt = df_ind["index"].max()[1]
	df_c.to_csv(os.path.join(output_path,"Complete_git_%s.csv"%(last_commit_dt)))
	#df_ind.to_csv(os.path.join(output_path,"Complete_index_git_%s_dt_%s.csv"%(last_commit_dt,last_dt)))
	integrate_frames_time = time.time()-start_all-main_CovidFeatureGen_time - stringency_time
	print("total time all is  %0.2f"%(time.time() - start_all))
	print("main_CovidFeatureGen_time is %0.2f"%main_CovidFeatureGen_time)
	print("stringency_time is %0.2f"%stringency_time)
	print("integrate_frames_time  is  %0.2f"%integrate_frames_time)
