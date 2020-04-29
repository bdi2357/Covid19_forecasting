import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from collections import OrderedDict
# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory
import time
import os
for dirname, _, filenames in os.walk('week2'):
    for filename in filenames:
        print(os.path.join(dirname, filename))


from tqdm import tqdm
import math
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error

import lightgbm as lgb
from sklearn.linear_model import Ridge
from sklearn.externals import joblib
from datetime import datetime
import argparse
# defining constants
PATH_TRAIN = "/kaggle/input/covid19-global-forecasting-week-3/train.csv"
PATH_TEST = "/kaggle/input/covid19-global-forecasting-week-3/test.csv"

PATH_SUBMISSION = "submission.csv"
PATH_OUTPUT = "output.csv"

PATH_REGION_METADATA = "/kaggle/input/covid19-forecasting-metadata/region_metadata.csv"
PATH_REGION_DATE_METADATA = "/kaggle/input/covid19-forecasting-metadata/region_date_metadata.csv"

VAL_DAYS = 7
MAD_FACTOR = 0.5
DAYS_SINCE_CASES = [1, 10, 50, 100, 500, 1000, 5000, 10000]



def rmsle(y, y_pred):
        assert len(y) == len(y_pred)
        terms_to_sum = [(math.log(y_pred[i] + 1) - math.log(y[i] + 1)) ** 2.0 for i,pred in enumerate(y_pred)]
        return (sum(terms_to_sum) * (1.0/len(y))) ** 0.5
    
  
def get_data_by_key(dataframe, key, key_value, fields=None):
    mini_frame=dataframe[dataframe[key]==key_value]
    if not fields is None:                
        mini_frame=mini_frame[fields].values
        
    return mini_frame
def get_lags(rate_array, current_index, size=20):
    lag_confirmed_rate=[-1 for k in range(size)]
    for j in range (0, size):
        if current_index-j>=0:
            lag_confirmed_rate[j]=rate_array[current_index-j]
        else :
            break
    return lag_confirmed_rate
def lag(df,col,lag,fillna_val = 0.0):
	return df[col].shift(lag).fillna(0.0)
def ma(df,col,window):
	return df[col].rolling(window).mean()
def std(df,col,window):
	return df[col].rolling(window).std()
def pct_change(df,col,back):
	return df[col].pct_change(back).fillna(0.0)
def diff(df,col,back):
    return df[col].diff(back).fillna(0.0)
def local_cvt(df,col,back):
    return df[col].diff(back).fillna(0.0) -df[col].shift(back).fillna(0.0).diff(back).fillna(0.0)



divd = lambda x,y: x/float(max(y,1))

def two_cols_func(df,col1,col2,fnc):
	return df.apply(lambda r: fnc(r[col1],r[col2]),axis=1)

def fatal2confirmed(df,col1,col2):
    return two_cols_func(df,col1,col2,divd).fillna(0.0)

def first_greater(df, n, col):
    m = df[col].ge(n)
    if m.any(): 
        return m.idxmax()
    else:
        return -1
def bet_dt(dt1,dt2):
    return (datetime.strptime(dt2,"%Y-%m-%d") - datetime.strptime(dt1,"%Y-%m-%d")).days
def from_event(df,n,col):
    fg = first_greater(df,n,col)
    if fg!= -1:
        return df.apply(lambda r: bet_dt(fg,r.name) if bet_dt(fg,r.name) >=0  else -1,axis=1 )
    else:
        return df.apply(lambda r:  -1,axis=1 )





def days_ago_thresold_hit(full_array, indx, thresold):
        days_ago_confirmed_count_10=-1
        if full_array[indx]>thresold: # if currently the count of confirmed is more than 10
            for j in range (indx,-1,-1):
                entered=False
                if full_array[j]<=thresold:
                    days_ago_confirmed_count_10=abs(j-indx)
                    entered=True
                    break
                if entered==False:
                    days_ago_confirmed_count_10=100 #this value would we don;t know it cross 0      
        return days_ago_confirmed_count_10 
    
    
def ewma_vectorized(data, alpha):
    sums=sum([ (alpha**(k+1))*data[k] for  k in range(len(data)) ])
    counts=sum([ (alpha**(k+1)) for  k in range(len(data)) ])
    return sums/counts

def generate_ma_std_window(rate_array, current_index, size=20, window=3):
    ma_rate_confirmed=[-1 for k in range(size)]
    std_rate_confirmed=[-1 for k in range(size)] 
    
    for j in range (0, size):
        if current_index-j>=0:
            ma_rate_confirmed[j]=np.mean(rate_array[max(0,current_index-j-window+1 ):current_index-j+1])
            std_rate_confirmed[j]=np.std(rate_array[max(0,current_index-j-window+1 ):current_index-j+1])           
        else :
            break
    return ma_rate_confirmed, std_rate_confirmed

def generate_ewma_window(rate_array, current_index, size=20, window=3, alpha=0.05):
    ewma_rate_confirmed=[-1 for k in range(size)]

    
    for j in range (0, size):
        if current_index-j>=0:
            ewma_rate_confirmed[j]=ewma_vectorized(rate_array[max(0,current_index-j-window+1 ):current_index-j+1, ], alpha)           
        else :
            break
    
    #print(ewma_rate_confirmed)
    return ewma_rate_confirmed
def get_target(rate_col, indx, horizon=33, average=3, use_hard_rule=False):
    target_values=[-1 for k in range(horizon)]
    cou=0
    for j in range(indx+1, indx+1+horizon):
        if j<len(rate_col):
            if average==1:
                target_values[cou]=rate_col[j]
            else :
                if use_hard_rule and j +average <=len(rate_col) :
                     target_values[cou]=np.mean(rate_col[j:j +average])
                else :
                    target_values[cou]=np.mean(rate_col[j:min(len(rate_col),j +average)])
                   
            cou+=1
        else :
            break
    return target_values

def monot(df,col):
    df[col+"_1"] = df[col].shift(1).fillna(0.0)
    df[col] = df.apply(lambda r: r[col+"_1"] if r[col] < r[col+"_1"] else r[col],axis=1)

def key_engineering(train,key_cols):
    print(train.columns)
    print("key_cols\n",key_cols)
    train["key"]=train[key_cols].apply(lambda row: "_".join([str(row[i]) for  i in range(len(key_cols))]),axis=1)
	#test["key"]=test[["Province_State","Country_Region"]].apply(lambda row: str(row[0]) + "_" + str(row[1]),axis=1)

def processing(df,cols,key_cols):
    key_engineering(df,key_cols)
    gf = {k:v for k,v in df.groupby("key")} 
    for k in gf.keys():
        gf[k] =gf[k].set_index("Date")
    for col in cols:
    	for k in gf.keys():
    		monot(gf[k],col)
    	from_gf_to_df(df,gf,col)

    return gf




def from_gf_to_df(frame,gf,col,key="key"):
     frame[col] = frame.apply(lambda r: gf[r[key]].loc[r["Date"],col],axis=1)


def genetate_directional_features(frame,gf,func_dict,key,start_fwd_looking=27,fwd_looking = 28):
    func_dict_keys = list(func_dict.keys())
    for func_name in tqdm(func_dict_keys): #func_dict.keys():
        for kk in tqdm(gf.keys()):
             params_d = func_dict[func_name]["params"].copy()
             params_d["df"] = gf[kk]
             gf[kk][func_name] = func_dict[func_name]["func"](**params_d)
             for jj in range(start_fwd_looking,fwd_looking):
                gf[kk][func_name+"__%d"%jj] = gf[kk][func_name].shift(jj).fillna(0)   
        for jj in range(start_fwd_looking,fwd_looking):              
            from_gf_to_df(frame,gf,func_name+"__%d"%jj,key)
    for cc in tqdm([1,10,100,1000,10000]):
        for kk in gf.keys():
            gf[kk]["From_CC_%d"%cc] = from_event(gf[kk],cc,"ConfirmedCases")
            gf[kk]["From_F_%d"%cc] = from_event(gf[kk],cc,"Fatalities")
            for jj in range(1,fwd_looking):
                gf[kk]["From_CC_%d__%d"%(cc,jj)] = gf[kk]["From_CC_%d"%cc].shift(jj)
                gf[kk]["From_F_%d__%d"%(cc,jj)] = gf[kk]["From_F_%d"%cc].shift(jj)                
        for jj in range(1,fwd_looking): 
            from_gf_to_df(frame,gf,"From_CC_%d__%d"%(cc,jj),key)
            from_gf_to_df(frame,gf,"From_F_%d__%d"%(cc,jj),key)






def derive_features(frame,gf,directional_func_dict,key):
	genetate_directional_features(frame,gf,func_dict,key)

def train_model_lgbm(df_train,gap,subset):
	#df_train.dropna(subset = ["target_cc", "target_ft", f"lag_{gap}_cc", f"lag_{gap}_ft"], inplace = True)
	df_train.dropna(subset = ["target_cc", "target_ft"], inplace = True)
	target_cc = df_train.target_cc
	target_ft = df_train.target_ft
	df_train.drop(["target_cc", "target_ft"], axis = 1, inplace = True)
	dtrain_cc = lgb.Dataset(df_train, label = target_cc, categorical_feature = categorical_features)
	dtrain_ft = lgb.Dataset(df_train, label = target_ft, categorical_feature = categorical_features)

	model_cc = lgb.train(LGB_PARAMS, train_set = dtrain_cc, num_boost_round = 200)
	model_ft = lgb.train(LGB_PARAMS, train_set = dtrain_ft, num_boost_round = 200)

	return model_cc,model_ft

def predict_lgbm(model):

	#y_pred_cc = #np.expm1(model_cc.predict(df_test, num_boost_round = 200)) + test_lag_cc
    y_pred_ft = np.expm1(model_ft.predict(df_test, num_boost_round = 200)) + test_lag_ft
    
    return y_pred_cc, y_pred_ft, model_cc, model_ft

    
    

def build_predict_lgbm(df_train, df_test, gap):
    
    df_train.dropna(subset = ["target_cc", "target_ft", f"lag_{gap}_cc", f"lag_{gap}_ft"], inplace = True)
    
    target_cc = df_train.target_cc
    target_ft = df_train.target_ft
    
    test_lag_cc = df_test[f"lag_{gap}_cc"].values
    test_lag_ft = df_test[f"lag_{gap}_ft"].values
    
    df_train.drop(["target_cc", "target_ft"], axis = 1, inplace = True)
    df_test.drop(["target_cc", "target_ft"], axis = 1, inplace = True)
    
    categorical_features = ["continent"]
    
    dtrain_cc = lgb.Dataset(df_train, label = target_cc, categorical_feature = categorical_features)
    dtrain_ft = lgb.Dataset(df_train, label = target_ft, categorical_feature = categorical_features)

    model_cc = lgb.train(LGB_PARAMS, train_set = dtrain_cc, num_boost_round = 200)
    model_ft = lgb.train(LGB_PARAMS, train_set = dtrain_ft, num_boost_round = 200)
    
    # inverse transform from log of change from last known value
    y_pred_cc = np.expm1(model_cc.predict(df_test, num_boost_round = 200)) + test_lag_cc
    y_pred_ft = np.expm1(model_ft.predict(df_test, num_boost_round = 200)) + test_lag_ft
    
    return y_pred_cc, y_pred_ft, model_cc, model_ft


def initialize_features_func(start_fwd_looking,fwd_looking):
    funcs_dict = OrderedDict()
    funcs_dict["fatal2confirmed"] = {"func":fatal2confirmed,"params":{"col1":"Fatalities","col2":"ConfirmedCases"}}
    
    for ii in range (1,fwd_looking):
        funcs_dict["ConfirmedCases_ma_%d"%ii] = {"func":ma,"params" : {"col":"ConfirmedCases","window":ii}}
        funcs_dict["ConfirmedCases_std_%d"%ii] = {"func":std,"params" : {"col":"ConfirmedCases","window":ii}}
        funcs_dict["ConfirmedCases_pct_%d"%ii] = {"func":pct_change,"params" : {"col":"ConfirmedCases","back":ii}}
        funcs_dict["Fatalities_ma_%d"%ii] = {"func":ma,"params" : {"col":"Fatalities","window":ii}}
        funcs_dict["Fatalities_std_%d"%ii] = {"func":std,"params" : {"col":"Fatalities","window":ii}}
        funcs_dict["Fatalities_pct_%d"%ii] = {"func":pct_change,"params" : {"col":"Fatalities","back":ii}}
        funcs_dict["ConfirmedCases_diff_%d"%ii] = {"func":diff,"params" : {"col":"ConfirmedCases","back":ii}}
        funcs_dict["ConfirmedCases_cvt_%d"%ii] = {"func":local_cvt,"params" : {"col":"ConfirmedCases","back":ii}}
        funcs_dict["Fatalities_diff_%d"%ii] = {"func":diff,"params" : {"col":"Fatalities","back":ii}}
        funcs_dict["Fatalities_cvt_%d"%ii] = {"func":local_cvt,"params" : {"col":"Fatalities","back":ii}}
        funcs_dict["ConfirmedCases_lag_%d"%ii] = {"func":lag,"params" : {"col":"ConfirmedCases","lag":ii}}
        funcs_dict["Fatalities_lag_%d"%ii] = {"func":lag,"params" : {"col":"Fatalities","lag":ii}}
    return funcs_dict


def main_out(file_path,key_cols,cols_of_interest,start_fwd_looking,fwd_looking,output_file):
    df_train = pd.read_csv(file_path)
    gf = processing(df_train,cols_of_interest,key_cols)
    funcs_dict = OrderedDict()
    #funcs_dict["fatal2confirmed"] = {"func":fatal2confirmed,"params":{"col1":"Fatalities","col2":"ConfirmedCases"}}
    
    funcs_dict = initialize_features_func(start_fwd_looking,fwd_looking)
    genetate_directional_features(df_train,gf,funcs_dict,"key",start_fwd_looking,fwd_looking)
    df_train.to_csv(output_file,index = False)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Interface to feature generation')
    parser.add_argument('--TrainFile', dest='input_file',  help='<Required> The file destination if the input file')
    parser.add_argument('--Dest', dest='dest_dir',  help='<Required> destination directory' )
    args = parser.parse_args()
    if args.dest_dir   :
        print("dest_dir %s"%args.dest_dir )
        
    else:
        print("ERROR !!! BAD INPUT ")
        exit(0)
    if not args.input_file:
        args.input_file = os.path.join("week4","train.csv")
    if not os.path.isdir(args.dest_dir):
        os.mkdir(args.dest_dir)
    

    st = time.time()
    #file_path = os.path.join(home_dir,"train.csv")
    key_cols =  ['Province_State', 'Country_Region']
    cols_of_interest = ["ConfirmedCases","Fatalities"]
    start_fwd_looking = 1
    fwd_looking =2
    output_file = os.path.join(args.dest_dir,"train_with_featuresNN_fwd_looking_%d.csv"%fwd_looking)

    main_out(args.input_file,key_cols,cols_of_interest,start_fwd_looking,fwd_looking,output_file)
    exit(0)
    home_dir = "week4"
    df_train = pd.read_csv(os.path.join(home_dir,"train.csv"))
    print(df_train.columns)

    file_path = os.path.join(home_dir,"train.csv")
    key_cols =  ['Province_State', 'Country_Region']
    cols_of_interest = ["ConfirmedCases","Fatalities"]
    start_fwd_looking = 1
    fwd_looking =2
    output_file = "train_with_featuresNN_fwd_looking_%d.csv"%fwd_looking

    main_out(file_path,key_cols,cols_of_interest,start_fwd_looking,fwd_looking,output_file)
    exit(0)

    
    key_cols = ['Province_State', 'Country_Region']
    old_key_cols = ['Province/State', 'Country/Region']
    gf = processing(df_train,["ConfirmedCases","Fatalities"],key_cols)
    funcs_dict = OrderedDict()
    funcs_dict["fatal2confirmed"] = {"func":fatal2confirmed,"params":{"col1":"Fatalities","col2":"ConfirmedCases"}}
    
    
    funcs_dict = initialize_features_func(start_fwd_looking,fwd_looking)
    genetate_directional_features(df_train,gf,funcs_dict,"key",start_fwd_looking,fwd_looking)
    

    df_train.to_csv("train_with_featuresN_fwd_looking_%d.csv"%fwd_looking,index = False)
    print("total running time is %0.2f"%(time.time() - st))

	#train_model_lgbm(df_train,gap,subset)



