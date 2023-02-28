"""Module for cleaning data"""

import pathlib
import argparse
import pandas as pd

PROJECT_DIR = pathlib.Path(__file__).parent
DATA_DIR = PROJECT_DIR / 'data'

RAW_DATA_DIR = DATA_DIR / 'eu_life_expectancy_raw.tsv'
CLEAN_DATA_DIR = DATA_DIR / 'pt_life_expectancy.csv'

def validate_types(data: pd.DataFrame) -> pd.DataFrame:

    """ Ensures year is an int and value is float. """

    data = data.astype({'year':'int'})
    data = data.astype({'value':'float'})
    return data

def clean_nans(data: pd.DataFrame) -> pd.DataFrame:

    """ Cleans NAN's out of the year and value columns. """

    data['year'] = data['year'].apply(pd.to_numeric, errors='coerce')

    data['value'] = data['value'].str.extract(r'(\d*.?\d*)',
                                    expand=False).apply(pd.to_numeric, errors='coerce')

    cleaned_data = data.dropna()
    return cleaned_data

def data_to_long_format(data: pd.DataFrame) -> pd.DataFrame:
    """ Unpivots the date to long format, so that we have the following columns: 
            unit, sex, age, region, year, value. """

    unpivot_data = data.melt(id_vars = data.columns[:4], var_name = 'year', value_name = 'value')
    unpivot_data.rename(columns = {'geo\\time':'region'}, inplace = True)
    return unpivot_data

def load_data() -> pd.DataFrame:
    """ Loads Raw Data from tsv file and converts it to Pandas Dataframe. """  

    return pd.read_csv(RAW_DATA_DIR, sep= r'[\t,]', engine = 'python', index_col=False)

def clean_data(data: pd.DataFrame, region_filter: str) -> pd.DataFrame:
    """ Cleans data referent to life expectancy in Europe grouped by 
    Country, Age, Sex, Region and Year. """

    unpivot_data = data_to_long_format(data)

    cleaned_data = clean_nans(unpivot_data)

    cleaned_data = validate_types(cleaned_data)

    # Filters the data based on the region filter
    filtered_data = cleaned_data[cleaned_data['region'] == region_filter]

    return filtered_data

def save_data(data: pd.DataFrame) -> None:
    """Saves cleaned data to csv file."""

    data.to_csv(CLEAN_DATA_DIR, index=False)

def main():
    """Main Function"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--region_filter', type=str, required=False,
                        default='PT', help='Enter region filter, default \'PT\'')
    args = parser.parse_args()


    data = load_data()
    cleaned_data = clean_data(data, args.region_filter)
    save_data(cleaned_data)


if __name__ == "__main__": #pragma: no cover
    main()
