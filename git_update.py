import pandas as pd
import os,sys,re
import numpy as np
from collections import OrderedDict
os.getcwd()
from datetime import datetime,date
days_diff =2
covid_data_path= "/Users/itaybd/covid19/COVID-19/new_covid/"
test_path = "COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
def dates_expr(d):
    return len(re.findall('[0-9]+/[0-9]+/[0-9]+',d)) >0 and d == re.findall('[0-9]+/[0-9]+/[0-9]+',d)[0]
def add_st(s):
    if len(s)==1:
        return "0"+s
    return s
def convert_dates(dt):
    spl = dt.split("/")
    return spl[2]+"-"+add_st(spl[0])+"-"+add_st(spl[1])
def check_data_git_rep(covid_data_path,test_path,git_path):
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
        ldt= date( 2000 +int(spl[0]),int(spl[1]),int(spl[2]))
        if days_diff < ( (dt1-ldt).days):
            #os.system("cd %s"%os.path.join(covid_data_path,"COVID-19"))
            os.chdir(os.path.join(covid_data_path,"COVID-19"))
            #print(os.getcwd())
            #print(os.listdir("."))
            os.system("git pull")
        else:
            print("up2date")
    else:
        os.chdir(covid_data_path)
        os.system("git clone %s"%git_path)
    
