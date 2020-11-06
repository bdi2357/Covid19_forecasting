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
	country_col = 'Country/Region'
	province_col= 'Province/State'

	start_main = time.time()
	orig_path = "/Users/itaybd/covid19/COVID-19/new_covid/COVID-19/"

	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	add=0
	DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,country_col=country_col,province_col=province_col)
	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF5,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,country_col=country_col,province_col=province_col)

	DF6 = DF5[[c for c in DF5.columns if not c in DF4.columns ]]

	DFC = pd.concat([DF4,DF6],axis=1)

	print("total time %0.2f"%(time.time() - start_all))
	start3 = time.time()
	output_path = "/Users/itaybd/output_covid"
	if not os.path.isdir(output_path):
	    os.mkdir(output_path)


	DFC.to_csv(os.path.join(output_path,"test_deathsN4confirmed_covid19.csv"),index_label="index")
	print("save time %0.2f"%(time.time() - start3))
	exit(0)

	add_covid = max(list(dict_indexes.values()))+1


	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
	col_name = "deaths"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "deaths"
	country_col = 'Country_Region'
	province_col= 'Province_State'



	DF7,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add = add_covid,country_col=country_col,province_col=province_col,dict_indexes=dict_indexes)
	file_name = orig_path+"csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
	col_name = "confirmed"
	lags = [1,7,14,28,56]
	lags2 = [7,14,28]
	col_tar = "confirmed"

	DF8,_ = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add= add_covid,country_col=country_col,province_col=province_col,dict_indexes=dict_indexes)

	DF9 = DF8[[c for c in DF8.columns if not c in DF7.columns ]]

	DFC2 = pd.concat([DF7,DF9],axis=1)

	DFC_complete = pd.concat([DFC,DFC2],axis=0)

	DFC_complete.to_csv(os.path.join(output_path,"test_deathsNconfirmed_covid19_wUS.csv"),index_label="index")


	print("TOTAL including US %0.2f"%(time.time() - start_main))
