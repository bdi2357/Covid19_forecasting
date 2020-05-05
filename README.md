# Covid19_prediction

Covid19_prediction offer predicing models for covid19 
## Installation

git clone 

## Basic Explanation:
The code is self explanatory where each of the file is documeneted.
The main parts are:
1. Data preparation based on https://github.com/CSSEGISandData/COVID-19
2. Directional (per item) Feature generation done in covid19_feature_generation.py
3. Cross Sectional (Geographic distance based) Feature generation done in Enreachment_Geo.py
4. Model Generation done in covid19_fit.py
5. Model selection creating prediction and error estimation done in run_model.py



## Data

https://www.kaggle.com/c/covid19-global-forecasting-week-4/data

## Usage

```
python covid19_feature_generation.py --TrainFile tmp_train_files/merged_global.csv --Dest ../out_Covid19_forecasting
python Enreachment_Geo.py --TrainFile ../out_Covid19_forecasting/train_with_featuresNN_fwd_looking_2.csv --Dest ../out_Covid19_forecasting
python covid19_fit.py  --TrainFile ../out_Covid19_forecasting/train_with_featuresNN_fwd_looking_2__1_ClosestCountries30.csv --Dest ../out_Covid19_forecasting/model
python run_model.py --ModelPath ../out_Covid19_forecasting/model/model_lag_1.pkl  --FeaturesFiles ../out_Covid19_forecasting/train_with_featuresNN_fwd_looking_2__1_ClosestCountries30.csv --Target ConfirmedCases --Dest ../out_Covid19_forecasting/prediction
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)