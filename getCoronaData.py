import itertools
import json
import locale
import os
import sys
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

locale.setlocale(category=locale.LC_ALL, locale='')

DATA_BASE_PATH = 'data'
RKI_DATA_PATH = f'{DATA_BASE_PATH}/RKI-Data'
WHO_DATA_PATH = f'{DATA_BASE_PATH}/WHO-Data'
RAW_DATA_PATH = f'{DATA_BASE_PATH}/rawData'

if not os.path.isdir(DATA_BASE_PATH):
    os.mkdir(DATA_BASE_PATH)
if not os.path.isdir(RKI_DATA_PATH):
    os.mkdir(RKI_DATA_PATH)
if not os.path.isdir(f'{RKI_DATA_PATH}/Bundeslaender'):
    os.mkdir(f'{RKI_DATA_PATH}/Bundeslaender')
if not os.path.isdir(WHO_DATA_PATH):
    os.mkdir(WHO_DATA_PATH)
if not os.path.isdir(RAW_DATA_PATH):
    os.mkdir(RAW_DATA_PATH)


class RKIData:
    _BASE_URL = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/'
    _RKI_CORONA_URL = _BASE_URL + 'RKI_COVID19/FeatureServer/0/query'

    _HEADERS = {
        'Accept-Language': 'en-US,en;q=0.5',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'DNT': '1',
    }

    def get_latest_data(self):
        cached_data = []

        params = {
            'where': '1=1',
            'outFields': '*',
            'outSR': '4326',
            'f': 'json',
            'resultType': 'standard',
            'resultRecordCount': 32000,
            'resultOffset': 0
        }

        while True:
            # print(f'Request for offset={params["resultOffset"]}')
            _response = requests.get(self._RKI_CORONA_URL, headers=self._HEADERS, params=params)
            if _response.status_code != 200:
                raise Exception("Failed to receive new data -> no internet connection?")
            _json_response = _response.json()
            cached_data.append(_json_response['features'])

            if 'exceededTransferLimit' not in _json_response.keys():
                break
            if _json_response['exceededTransferLimit'] == 'false':
                break

            params['resultOffset'] += 32000
            # time.sleep(1)

        data = []
        for big_result_list in cached_data:
            for single_result in big_result_list:
                data.append(single_result['attributes'])

        del cached_data

        return pd.DataFrame(data)

class WHOData:
    
    _BASE_URL = 'https://covid19.who.int'
    
    _HEADERS = {
        'Accept-Language': 'en-US,en;q=0.5',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'DNT': '1',
    }


    def get_latest_data(self):
        _url = f'{self._BASE_URL}/WHO-COVID-19-global-data.csv'
        return pd.read_csv(_url)

    def convert_data(self, df: pd.DataFrame):

        

        _region_total = 7
        _region_counter = 0

        for region in df.WHO_region.unique():
            _region_counter += 1
            print(f'\nConverting region: {region} ({_region_counter}/{_region_total})')

            _combined_region_data = []

            _region_path = f'{WHO_DATA_PATH}/{region}'
            if not os.path.isdir(_region_path):
                os.mkdir(_region_path)
            
            _region_df = df[df.WHO_region == region]
            _countrys = _region_df.Country.unique()
            del _region_df

            _country_total = len(_countrys)
            _country_counter = 0
            

            for _country in _countrys:

                _country_path = f'{_region_path}/{_country}.csv'

                _country_counter += 1
                sys.stdout.write(f'\rConverting country: {_country_counter}/{_country_total}')
                sys.stdout.flush()

                _country_df = df[df.Country == _country]

                _combined_country_data = []
                _day_counter = 0

                for i in range(1, 3):
                    _day_counter += 1
                    _current_date = datetime.strptime(f'2020-01-0{i}', '%Y-%m-%d')
                    _combined_country_data.append([_day_counter, _current_date.day, _current_date.month, _current_date.year, _current_date.strftime('%u'), 0, 0, 0, 0])

                for _row in _country_df.itertuples():
                    _day_counter += 1
                    _current_date = datetime.strptime(_row.Date_reported, '%Y-%m-%d')
                    _combined_country_data.append([_day_counter, _current_date.day, _current_date.month, _current_date.year, _current_date.strftime('%u'), _row.New_cases, _row.Cumulative_cases, _row.New_deaths, _row.Cumulative_deaths])
                
                del _country_df

                _to_save_df = pd.DataFrame(_combined_country_data, columns=['#DayCounter', 'Day', 'Month', 'Year', 'Weekday', 'NewCases', 'CumulativeCases', 'NewDeaths', 'CumulativeDeaths'])

                _to_save_df.to_csv(_country_path, index=False)
        
        print('\n')
            
            

            




def raw_data_to_daily_data(raw_df, accumulated=False, death=False, state_id=99):
    if state_id != 99:
        _sorted_df = raw_df[raw_df.IdBundesland == state_id].sort_values('Refdatum', ignore_index=True, kind='stable')
    else:
        _sorted_df = raw_df.sort_values(by='Refdatum', ignore_index=True, kind='stable')

    _combined_data = []
    _current_date = _sorted_df.head(1)['Refdatum'][0]
    _current_cases = 0
    _day_counter = 0

    for _row in _sorted_df.itertuples():
        if death and _row.NeuerTodesfall >= 0 or (not death) and _row.NeuerFall >= 0:
            if death:
                _caseCount = _row.AnzahlTodesfall
            else:
                _caseCount = _row.AnzahlFall
            _refDate = _row.Refdatum

            _current_datetime = datetime.utcfromtimestamp(_current_date / 1000)
            _ref_datetime = datetime.utcfromtimestamp(_refDate / 1000)

            while True:
                if _current_datetime < _ref_datetime:
                    _day_counter += 1
                    _combined_data.append([_day_counter, _current_datetime.day, _current_datetime.month, _current_datetime.year,
                                           _current_datetime.strftime('%u'), _current_cases])
                    if not accumulated:
                        _current_cases = 0
                    # _current_date = _refDate

                    _current_datetime = _current_datetime + timedelta(days=1)
                else:
                    _current_date = _refDate
                    break

            _current_cases += _caseCount

    _day_counter += 1
    _current_datetime = datetime.utcfromtimestamp(_current_date / 1000)
    _combined_data.append([_day_counter, _current_datetime.day, _current_datetime.month, _current_datetime.year,
                           _current_datetime.strftime('%u'), _current_cases])
    _combinedDf = pd.DataFrame(_combined_data, columns=['#DayCounter', 'Day', 'Month', 'Year', 'Weekday', 'AnzahlFall'])

    if state_id != 99:
        if death:
            if accumulated:
                _save_path = f'{RKI_DATA_PATH}/Bundeslaender/Covid-19-{state_id}-Accumulated-Deaths.csv'
            else:
                _save_path = f'{RKI_DATA_PATH}/Bundeslaender/Covid-19-{state_id}-Deaths.csv'
        else:
            if accumulated:
                _save_path = f'{RKI_DATA_PATH}/Bundeslaender/Covid-19-{state_id}-Accumulated-Cases.csv'
            else:
                _save_path = f'{RKI_DATA_PATH}/Bundeslaender/Covid-19-{state_id}-Cases.csv'
    else:
        if death:
            if accumulated:
                _save_path = f'{RKI_DATA_PATH}/Covid-19-Accumulated-Deaths.csv'
            else:
                _save_path = f'{RKI_DATA_PATH}/Covid-19-Deaths.csv'
        else:
            if accumulated:
                _save_path = f'{RKI_DATA_PATH}/Covid-19-Accumulated-Cases.csv'
            else:
                _save_path = f'{RKI_DATA_PATH}/Covid-19-Cases.csv'

    _combinedDf.to_csv(_save_path, index=False)


print('◄►◄►◄►◄►◄►◄►◄►◄ RKI Data ►◄►◄►◄►◄►◄►◄►◄►\n')

getNewData = True
rkiData = RKIData()

if getNewData:
    print('Getting new raw data from RKI (can take some time) ...\n')
    rawDf = rkiData.get_latest_data()
    print('Saving new data ...\n')
    rawDf.to_csv(f'{RAW_DATA_PATH}/Covid-19-Raw-Data-RKI.csv')
else:
    print('Loading old data from disk ...\n')
    rawDf = pd.read_csv(f'{RAW_DATA_PATH}/Covid-19-Raw-Data-RKI.csv')



if __name__ == '__main__':
    print('Converting raw data ...\n')
    _to_convert_total = 0
    _converted_counter = 0
    for i in range(1, 17):
        for j in range(0, 2):
            for k in range(0, 2):
                _to_convert_total += 1

    _to_convert_total += 4

    for state_id in range(1, 17):
        for death in range(0, 2):
            for accumulated in range(0, 2):
                raw_data_to_daily_data(rawDf, bool(accumulated), bool(death), state_id)
                _converted_counter += 1
                sys.stdout.write(f'\rConverted dataset {_converted_counter} from {_to_convert_total}')
                sys.stdout.flush()

    for i in range(0,2):
        for j in range(0,2):
            _converted_counter += 1
            raw_data_to_daily_data(rawDf, bool(i), bool(j))
            sys.stdout.write(f'\rConverted dataset {_converted_counter} from {_to_convert_total}')
            sys.stdout.flush()

print('\n')


print('Creating Id-to-State.csv file...\n')

id_to_state = []

for state_id in range(1, 17):
    state_name = rawDf[rawDf.IdBundesland == state_id].head(1).reset_index()['Bundesland'][0]
    id_to_state.append({'IdBundesland': state_id, 'Bundesland': state_name})

pd.DataFrame(id_to_state).to_csv(f'{RKI_DATA_PATH}/Id-to-State.csv', index=False)

del rawDf
print('RKI-Data updated\n\n\n')


print('◄►◄►◄►◄►◄►◄►◄►◄ WHO Data ►◄►◄►◄►◄►◄►◄►◄►\n')

whoNewData = True

whoData = WHOData()

if whoNewData:
    print('Getting new raw data from WHO (can take some time) ...\n')
    whoRawDf = whoData.get_latest_data()
    print('Saving new data ...\n')
    whoRawDf.to_csv(f'{RAW_DATA_PATH}/Covid-19-Raw-Data-WHO.csv')
else:
    print('Loading old data from disk ...\n')
    whoRawDf = pd.read_csv(f'{RAW_DATA_PATH}/Covid-19-Raw-Data-WHO.csv')

print('Converting raw data ...\n')
whoData.convert_data(whoRawDf)

print('◄►◄►◄►◄►◄►◄►◄►◄ Done ►◄►◄►◄►◄►◄►◄►◄►')