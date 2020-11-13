from FeatureGenerator import *
if __name__ == "__main__":
	start_all = time.time()

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
	DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func)

	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF5,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func)
	
	DF6 = DF5[[c for c in DF5.columns if not c in DF4.columns ]]

	DFC = pd.concat([DF4,DF6],axis=1)

	print("total time %0.2f"%(time.time() - start_all))
	start3 = time.time()
	output_path = "/Users/itaybd/output_covid"
	if not os.path.isdir(output_path):
	    os.mkdir(output_path)


	DFC.to_csv(os.path.join(output_path,"test_deathsN4confirmed_covid19.csv"),index_label="index")
	print("save time %0.2f"%(time.time() - start3))
	

	add_covid = max(list(dict_indexes.values()))+1


	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	country_col = 'Country_Region'
	province_col= 'Province_State'



	DF7,dict_indexes2 = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add = add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes)
	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF8,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add= add_covid,key_cols=key_cols2,key_cols_func=key_cols_func,dict_indexes=dict_indexes)

	DF9 = DF8[[c for c in DF8.columns if not c in DF7.columns ]]

	DFC2 = pd.concat([DF7,DF9],axis=1)

	DFC_complete = pd.concat([DFC,DFC2],axis=0)



	DFC_complete.to_csv(os.path.join(output_path,"test_deathsNconfirmed_covid19_wUSN.csv"),index_label="index")


	print("TOTAL including US %0.2f"%(time.time() - start_main))
	#{k:v for k,v in dfn.groupby("Country_Province")}
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
	dfn.to_csv(os.path.join(output_path,"test_feature_mixing_covid19_wUSN2.csv"),index_label="index")
	print("TOTAL including feature mixing %0.2f"%(time.time() - start_main))
	#dict_indexes.update(**{str(k): v for k, v in dict_indexes2.items()})
	for k in dict_indexes2.keys():
		dict_indexes[k] = dict_indexes2[k]
	DF_ind = pd.DataFrame(list(dict_indexes.items()),columns=["index","val"])
	DF_ind.to_csv(os.path.join(output_path,"test_covid19_wUSN3_index.csv"),index=False)
