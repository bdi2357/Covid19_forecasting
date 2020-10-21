
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