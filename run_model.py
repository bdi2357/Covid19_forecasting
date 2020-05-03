import joblib
import argparse
import pandas as pd
import numpy as np
import os,re,sys
def rmsle(y, y_pred):
        assert len(y) == len(y_pred)
        terms_to_sum = [(math.log(y_pred[i] + 1) - math.log(y[i] + 1)) ** 2.0 for i,pred in enumerate(y_pred)]
        return (sum(terms_to_sum) * (1.0/len(y))) ** 0.5

def RMSLE(pred,actual):
    return np.sqrt(np.mean(np.power((np.log(np.maximum(pred+1,0.01))-np.log(np.maximum(actual+1,0.01))),2)))

def pred1(model_path,pred_file,excluded_cols):
	model = joblib.load(model_path)
	df_for_prediction = pd.read_csv(pred_file)
	#df_for_prediction = pd.read_csv(features_file)
	df_for_prediction.drop(excluded_cols, axis = 1, inplace = True)
	
	pred_res =  model.predict(df_for_prediction,num_boost_round = 200)
	return pred_res
def eval_res(pred_res,tar):
	rmsle = RMSLE(tar.values,pred_res)
	print("RMSLE ",rmsle)
	return rmsle


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Interface to feature generation')
    parser.add_argument('--ModelPath', dest='model_file',  help='<Required> The modle destination ')
    parser.add_argument("--FeaturesFiles",dest = 'features_file',help = '<Required> ')
    parser.add_argument("--Target",dest='target_col',help='<Optional>')
    parser.add_argument("--Lag",dest='lag',help='<Optional>')
    parser.add_argument('--Dest', dest='dest_dir',  help='<Required> destination directory' )
    args = parser.parse_args()
    if args.dest_dir   :
        print("dest_dir %s"%args.dest_dir )
        
    else:
        print("ERROR !!! BAD destination ")
        exit(0)
    if not args.model_file:
        print("ERROR !!! No ModelPath ")
        exit(0)
    if not os.path.isdir(args.dest_dir):
        os.mkdir(args.dest_dir)
    lag_str = "__%s"%args.lag
    target_col = args.target_col
    df_for_prediction = pd.read_csv(args.features_file)
    excluded_cols = [c for c in df_for_prediction.columns if c.find(lag_str) == -1]
    pred_res = pred1(args.model_file,args.features_file,excluded_cols)
    tar = df_for_prediction[target_col]
    eval_res(pred_res,tar)


