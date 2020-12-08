from FeatureGenerator import *
from covid19_feature_mixing import *
import git
from pathlib import Path
import inspect
HOME = str(Path.home())
def last_git_commit_date():
	repo = git.Repo(".")
	commit = repo.head.commit 
	return commit.committed_datetime.strftime("%Y-%m-%d")
def main_CovidFeatureGen(covid_path,output_path = os.path.join(HOME,"output_covid") ):
	if not os.path.isdir(output_path):
		os.mkdir(output_path)
	start_all = time.time()
	def c_func(cols):
		#str(r[country_col])+("_"+str(r[province_col])).replace("_nan","")
		def lm(r):
			s = str(r[cols[0]])
			for c in cols[1:]:
				s+= ("_"+str(r[c])).replace("_nan","")
			return s
		return lm

	#vars
	##### Global ########
	key_cols_func = c_func
	key_cols = ['Country/Region','Province/State']
	key_cols2 = ['Country_Region','Province_State']
	start_main = time.time()
	orig_path = covid_path #"/Users/itaybd/covid19/COVID-19/new_covid/COVID-19/"
	file_name = os.path.join(orig_path,"csse_covid_19_data","csse_covid_19_time_series","time_series_covid19_deaths_global.csv")
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	add=0
	DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func,prep_data=prep_data_dt_cols)
	
	last_data_date = max(list(dict_indexes.keys()))[1]
	git_commit_date = last_git_commit_date()
	file_name = os.path.join(orig_path,"csse_covid_19_data","csse_covid_19_time_series","time_series_covid19_confirmed_global.csv")
	col_name = "confirmed"
	col_tar = "confirmed"
	DF5,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func,prep_data=prep_data_dt_cols)
	DF6 = DF5[[c for c in DF5.columns if not c in DF4.columns ]]

	DFC = pd.concat([DF4,DF6],axis=1)

	print("total time %0.2f"%(time.time() - start_all))
	start3 = time.time()
	
	if not os.path.isdir(output_path):
	    os.mkdir(output_path)

	DF_ind = pd.DataFrame(list(dict_indexes.items()),columns=["index","val"])
	DFC.to_csv(os.path.join(output_path,"test_deathsANDconfirmed_covid19_%s_%s.csv"%(last_data_date,git_commit_date)),index_label="index")
	print("save time %0.2f"%(time.time() - start3))
	###########################################################################
	### USA ###
	add_covid = max(list(dict_indexes.values()))+1


	file_name = os.path.join(orig_path,"csse_covid_19_data","csse_covid_19_time_series","time_series_covid19_deaths_US.csv")
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	country_col = 'Country_Region'
	province_col= 'Province_State'



	DF7,dict_indexes2 = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add = add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes,prep_data=prep_data_dt_cols)
	file_name = os.path.join(orig_path,"csse_covid_19_data","csse_covid_19_time_series","time_series_covid19_confirmed_US.csv")
	col_name = "confirmed"
	col_tar = "confirmed"

	DF8,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add= add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes,prep_data=prep_data_dt_cols)

	DF9 = DF8[[c for c in DF8.columns if not c in DF7.columns ]]

	DFC2 = pd.concat([DF7,DF9],axis=1)

	DFC_complete = pd.concat([DFC,DFC2],axis=0)



	DFC_complete.to_csv(os.path.join(output_path,"test_deathsANDconfirmed_covid19_wUS_%s_%s.csv"%(last_data_date,last_git_commit_date)),index_label="index")


	print("TOTAL including US %0.2f"%(time.time() - start_main))

	
	varbs = []
	varbs.append( {"prefix1": "daily_deaths_sum_7",
	    "prefix2": "daily_confirmed_sum_7",
	    "filter_strings1":[],
	    "filter_strings2":[],
	    "operators": operators_l,
	    "groupby_column": "Country_Province"}
	    )
	varbs.append( {"prefix1": "daily_deaths_sum_7",
	    "prefix2": "daily_confirmed_sum_7",
	    "filter_strings1":[],
	    "filter_strings2":[],
	    "operators": operators_l,
	    "groupby_column": "Country_Province"}
	    )
	varbs.append( {"prefix1": "daily_deaths_sum_7",
	    "prefix2": "daily_confirmed_sum_7",
	    "filter_strings1":[],
	    "filter_strings2":[],
	    "operators": operators_l,
	    "groupby_column": "Country_Province"}
	    )
	DF_ind = pd.DataFrame(list(dict_indexes2.items()),columns=["index","val"])
	DF_ind.to_csv("../index_%s_%s.csv"%(os.path.basename(__file__),inspect.stack()[0][3]))
	return extd_feature_mixing(DFC_complete,varbs),DF_ind


	
	########################################################################################	
if __name__ == "__main__":
	main_CovidFeatureGen("/Users/itaybd/COVID-19")
	exit(0)


	orig_path = "/Users/itaybd/test11/COVID-19/"

	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	start_col = 12
	#country_col = 'Country/Region'
	#province_col= 'Province/State'
	key_cols = ['Country/Region','Province/State']
	def c_func(cols):
		#str(r[country_col])+("_"+str(r[province_col])).replace("_nan","")
		def lm(r):
			s = str(r[cols[0]])
			for c in cols[1:]:
				s+= ("_"+str(r[c])).replace("_nan","")
			return s
		return lm
	key_cols2 = ['Country_Region','Province_State']


	key_cols_func = c_func
	start_main = time.time()
	orig_path = "/Users/itaybd/covid19/COVID-19/new_covid/COVID-19/"

	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	add=0
	#DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,country_col=country_col,province_col=province_col)
	DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func,prep_data=prep_data_dt_cols)
	
	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF5,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func,prep_data=prep_data_dt_cols)
	
	DF6 = DF5[[c for c in DF5.columns if not c in DF4.columns ]]

	DFC = pd.concat([DF4,DF6],axis=1)

	print("total time %0.2f"%(time.time() - start_all))
	start3 = time.time()
	output_path = "/Users/itaybd/output_covid"
	if not os.path.isdir(output_path):
	    os.mkdir(output_path)


	DFC.to_csv(os.path.join(output_path,"test_deathsM4confirmed_covid19.csv"),index_label="index")
	print("save time %0.2f"%(time.time() - start3))
	

	add_covid = max(list(dict_indexes.values()))+1


	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	country_col = 'Country_Region'
	province_col= 'Province_State'



	DF7,dict_indexes2 = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add = add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes,prep_data=prep_data_dt_cols)
	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF8,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add= add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes,prep_data=prep_data_dt_cols)

	DF9 = DF8[[c for c in DF8.columns if not c in DF7.columns ]]

	DFC2 = pd.concat([DF7,DF9],axis=1)

	DFC_complete = pd.concat([DFC,DFC2],axis=0)



	DFC_complete.to_csv(os.path.join(output_path,"test_deathsNconfirmed_covid19_wUSM.csv"),index_label="index")


	print("TOTAL including US %0.2f"%(time.time() - start_main))
	#{k:v for k,v in dfn.groupby("Country_Province")}
	### features mixing
	
	gfn = OrderedDict([(k,v) for k,v in DFC_complete.groupby("Country_Province")])
	dfn = DFC_complete 
	start_t = time.time()
	for k in gfn.keys():
	    prefix1 = "daily_deaths_sum_7"
	    prefix2 = "daily_confirmed_sum_7"
	    filter_strings1 =[]
	    filter_strings2 =[]
	    operators = operators_l
	    a_c =feature_mixing(gfn[k], prefix1,prefix2, filter_strings1,filter_strings2,operators)
	print("a_c is : ",a_c)
	print("calc features time is %0.2f"%(time.time()-start_t))
	from_gf_to_df(dfn,gfn,a_c)

	gfn = OrderedDict([(k,v) for k,v in dfn.groupby("Country_Province")])



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
