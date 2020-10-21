import math
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error

import lightgbm as lgb
from sklearn.linear_model import Ridge
from sklearn.externals import joblib
import time
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import argparse


def plotImp(model, X , num = 50):
    feature_imp = pd.DataFrame({'Value':model.feature_importance(importance_type='gain'),'Feature':X.columns})
    plt.figure(figsize=(30, 30))
    sns.set(font_scale = 0.5)
    sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", 
                                                        ascending=False)[0:num])
    feature_imp.sort_values(by="Value",ascending=False).to_csv("Feature_imp.csv")
    plt.title('LightGBM Features (avg over folds)')
    plt.tight_layout()
    plt.savefig('lgbm_importances-01.png')
    plt.show()

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

SEED = 2357

LGB_PARAMS = {"objective": "regression",
              "num_leaves": 300,
              "n_estimators" : 1000,
              "learning_rate": 0.013,
              #"bagging_fraction": 0.999,
              "feature_fraction": 0.9,
              "reg_alpha": 0.13,
              "reg_lambda": 0.13,
              "metric": "rmse",
              "seed": SEED
             }
LGB_PARAMS_F = {"objective": "regression",
              "num_leaves": 128,
              "n_estimators" : 2000,
              "learning_rate": 0.013,
              #"bagging_fraction": 0.999,
              "feature_fraction": 0.9,
              "reg_alpha": 0.13,
              "reg_lambda": 0.13,
              "metric": "rmse",
              "seed": SEED
             }

def rmsle(y, y_pred):
        assert len(y) == len(y_pred)
        terms_to_sum = [(math.log(y_pred[i] + 1) - math.log(y[i] + 1)) ** 2.0 for i,pred in enumerate(y_pred)]
        return (sum(terms_to_sum) * (1.0/len(y))) ** 0.5

def RMSLE(pred,actual):
    return np.sqrt(np.mean(np.power((np.log(np.maximum(pred+1,0.01))-np.log(np.maximum(actual+1,0.01))),2)))
def lgb_train(features_file,params,excluded_cols,target_col):
  df_train = pd.read_csv(features_file)
  target = df_train[target_col]
  df_train.drop(excluded_cols, axis = 1, inplace = True)
  
  #target_ft = df_train.target_ft
  train = lgb.Dataset(df_train, label = target)

  model = lgb.train(params, train_set = train, num_boost_round = 500)
  lgb.plot_importance(model,importance_type='gain',max_num_features=50)
  plt.show()
  return model
def predict(model,df_for_prediction):

  return model.predict(df_for_prediction, num_boost_round = 20000)

def feature_importance(clf,X):
  feature_imp = pd.DataFrame(sorted(zip(clf.feature_importances_,X.columns)), columns=["Value","Feature"])

  plt.figure(figsize=(20, 10))
  sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", ascending=False))
  plt.title("LightGBM Features (avg over folds)")
  plt.tight_layout()
  plt.show()
  plt.savefig('lgbm_importances-01.png')

def generate_model(data_file_name,lag,dest_dir,target_col,params):
  df_for_prediction = pd.read_csv(file_name)
  lag_str = "__%d"%lag
  excluded_cols = [c for c in df_for_prediction.columns if c.find(lag_str) == -1]
  df_for_prediction[target_col] = df_for_prediction[target_col].fillna(0.0)
  df_for_prediction[target_col] = df_for_prediction.apply(lambda r: 0 if r[target_col]<0 else r[target_col],axis=1)
  model = lgb_train(features_file,params,excluded_cols,target_col)
  print("total train running time is %0.2f"%(time.time()-st))
  joblib.dump(model, os.path.join(args.dest_dir,"model_lag_%d.pkl"%lag))


if __name__ == "__main__":
  
  parser = argparse.ArgumentParser(description='Interface to model generation')
  parser.add_argument('--TrainFile', dest='input_file',  help='<Required> The file destination if the input file')
  parser.add_argument('--Dest', dest='dest_dir',  help='<Required> destination directory' )
  args = parser.parse_args()
  if args.dest_dir   :
      print("dest_dir %s"%args.dest_dir )
      
  else:
      print("ERROR !!! BAD INPUT ")
      exit(0)
  if not args.input_file:
      print("ERROR !!! BAD INPUT ")
      exit(0)
  if not os.path.isdir(args.dest_dir):
      os.mkdir(args.dest_dir)

  st = time.time()


  features_file = args.input_file #"out1/train_with_featuresNN_fwd_looking_2.csv"
  params = LGB_PARAMS
  df_for_prediction = pd.read_csv(features_file)
  lag = 1#27
  lag_str = "__%d"%lag
  excluded_cols = [c for c in df_for_prediction.columns if c.find(lag_str) == -1]
  target_col = "ConfirmedCases"#"Fatalities" #"Fatalities" #
  df_for_prediction[target_col] = df_for_prediction[target_col].fillna(0.0)
  df_for_prediction[target_col] = df_for_prediction.apply(lambda r: 0 if r[target_col]<0 else r[target_col],axis=1)
  
  model = lgb_train(features_file,params,excluded_cols,target_col)
  print("total train running time is %0.2f"%(time.time()-st))
  joblib.dump(model, os.path.join(args.dest_dir,"model_lag_%d.pkl"%lag))
