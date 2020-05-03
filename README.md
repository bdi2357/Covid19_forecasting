# Covid19_prediction

Covid19_prediction offer predicing models for covid19 
## Installation

git clone 

## Data

https://www.kaggle.com/c/covid19-global-forecasting-week-4/data

## Usage

```
python covid19_feature_generation.py --TrainFile /Users/itaybd/covid19/COVID-19/week4/train.csv --Dest ../out_Covid19_forecasing
python Enreachment_Geo.py --TrainFile ../out_Covid19_forecasing/train_with_featuresNN_fwd_looking_2.csv --Dest ../out_Covid19_forecasing
python covid19_fit.py  --TrainFile ../out_Covid19_forecasing/train_with_featuresNN_fwd_looking_2__1_ClosestCountries30.csv --Dest ../out_Covid19_forecasing/model
python run_model.py --ModelPath ../out_Covid19_forecasing/model/model_lag_1.pkl  --FeaturesFiles ../out_Covid19_forecasing/train_with_featuresNN_fwd_looking_2__1_ClosestCountries30.csv --Target ConfirmedCases --Dest ../out_Covid19_forecasing/prediction
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)