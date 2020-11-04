import pandas as pd
import os,sys,re
import numpy as np
import argparse
from collections import OrderedDict
from dateutil.parser import parse
os.getcwd()
from datetime import datetime,date
days_diff =2
covid_data_path= "/Users/itaybd/covid19/COVID-19/new_covid/"
test_path = "COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

"https://github.com/CSSEGISandData/COVID-19.git"

parse("2/5/2019", dayfirst=True)
def dates_expr(d):
    return len(re.findall('[0-9]+',d)) >=2
def add_st(s):
    if len(s)==1:
        return "0"+s
    return s
"""
def convert_dates(dt):
    spl = dt.split("/")
    return spl[2]+"-"+add_st(spl[0])+"-"+add_st(spl[1])
"""
def convert_dates(dt):
    spl = parse(dt,dayfirst=False)
    return str(spl.year)+"-"+add_st(str(spl.month))+"-"+add_st(str(spl.day))
def check_data_git_rep(covid_data_path,test_path,git_path):
    git_main = git_path.split("/")[-1].split(".")[0]
    dt1 = datetime.now().date()
    test_file_path = os.path.join(covid_data_path,test_path)
    if os.path.isfile(test_file_path):
        print("file exists")
        df_test = pd.read_csv(test_file_path)
        date_cols = [c for c in df_test.columns if dates_expr(c)]
        dates_cnvrtd = [convert_dates(d) for d in date_cols]
        print(max(dates_cnvrtd))
        last_d = max(dates_cnvrtd)
        spl=last_d.split("-")
        ldt= date( 2000 +(int(spl[0])%100),int(spl[1]),int(spl[2]))
        if days_diff < ( (dt1-ldt).days):
            #os.system("cd %s"%os.path.join(covid_data_path,"COVID-19"))
            os.chdir(os.path.join(covid_data_path,git_main))
            #print(os.getcwd())
            #print(os.listdir("."))
            os.system("git pull")
        else:
            print("up2date")
    else:
        print("HERE %s"%git_path)
        print(covid_data_path)
        os.chdir(covid_data_path)

        os.system("git clone %s"%git_path)


if __name__ == "__main__":
  """
  parser = argparse.ArgumentParser(description='Interface to test git')
  parser.add_argument('--data_path', dest='git_repo_local_path',  help='<Required> destination of local git data repo')
  parser.add_argument('--git_repo', dest='git_repo_address',  help='<Required> destination directory' )
  args = parser.parse_args()
  if args.git_repo_local_path   :
    print("dest_dir %s"%args.dest_dir )
  else:
    print("Error no git_repo_local_path input")ֿֿ
    exit(0)
  if args.git_repo_address   :
    print("dest_dir %s"%args.git_repo_address )
  else:
    print("Error no git_repo_address input")ֿֿ
    exit(0)
  check_data_git_rep
  """
  covid_data_path = ".."
  test_path = "COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

  git_path = "https://github.com/CSSEGISandData/COVID-19.git"

  check_data_git_rep(covid_data_path,test_path,git_path)

  covid_data_path = ".."
  git_path = "https://github.com/OxCGRT/covid-policy-tracker.git"
  test_path ="covid-policy-tracker/data/timeseries/index_stringency.csv"
  check_data_git_rep(covid_data_path, test_path, git_path)
  
  
      


    
