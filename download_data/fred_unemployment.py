from full_fred.fred import Fred

from time import sleep
from tqdm import tqdm
import pandas as pd

# utility functions to go around FED's daily request limit
def nap(n):
    for _ in tqdm(range(n), desc = 'zzz.. napping'):
        sleep(1)

def keep_trying(func, x, y, no_subscription = False):
    error = True
    n_trial = 0
    if not no_subscription:
        while(error):
            try:
                return func(x)[y]
            except:
                nap(1 + n_trial * 1)
                n_trial += 1
    else:
        while(error):
            try:
                response = func(x)
                if type(response) == pd.DataFrame:
                    return response
                else:
                    raise Exception
            except:
                nap(1 + n_trial * 1)
                n_trial += 1




if __name__ == '__main__':
    # instantiate a Fred obj for queries
    fred = Fred('api_key')

    # read in serie ids
    id_name_list = []
    with open('id', 'r') as file:
        for row in file:
            splitted_str = row.split(',')
            if len(splitted_str) == 3:
                id, county, state = splitted_str
                state = state[: - 1]  # remove line break
                county = county[22:]  # remove 'unemployment rate in'
                id_name_list.append([id, f'{county}_{state}'])
            else:
                id, county = splitted_str
                county = county[22:]  # remove 'unemployment rate in'
                id_name_list.append([id, f'{county}'])

    # query monthly unemployment data by county
    for id, name in tqdm(id_name_list):
        temp_df = keep_trying(fred.get_series_df, id, '', no_subscription = True)
        name = name.replace('/', '_')
        temp_df.to_csv(f'fred_unemployment_rate/{name}', index = False)