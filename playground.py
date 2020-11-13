from FeatureGenerator import *
key_cols = ['Country']
def c_func(cols):
    #str(r[country_col])+("_"+str(r[province_col])).replace("_nan","")
    def lm(r):
        s = str(r[cols[0]])
        for c in cols[1:]:
            s+= ("_"+str(r[c])).replace("_nan","")
        return s
    return lm
test_path ="../covid-policy-tracker/data/timeseries/index_stringency.csv"
file_name = test_path
col_name = "stringency"
lags = [1,7,14,28,56]
lags2 = [7,14,28]
key_cols = ['Unnamed: 0']
key_cols_func = c_func
col_tar = "stringency"
add =0
DF4,dict_indexes = main_generator(file_name=file_name,col_name=col_name,lags=lags,lags2=lags2,col_tar=col_tar,add=add,key_cols=key_cols,key_cols_func=key_cols_func)
output_path = "/Users/itaybd/output_covid"
DF4.to_csv(os.path.join(output_path,"test_stringency.csv"),index_label="index")
DF_ind = pd.DataFrame(list(dict_indexes.items()),columns=["index","val"])
DF_ind.to_csv(os.path.join(output_path,"test_stringency_index.csv"),index=False)
