import datetime
import json
import os
import re
import pandas as pd
import requests

DAILY_REPORT_DIR = '../data/daily_reports'
DAILY_REPORT = os.path.join(DAILY_REPORT_DIR, "il_county_results_{}.csv")


def get_latest_data():
    try:
        #dph_url = 'http://www.dph.illinois.gov'
        # find latest dataset URL from IL DPH
        #covid_path = '/topics-services/diseases-and-conditions/diseases-a-z-list/coronavirus'
        #raw_page = requests.get(dph_url + covid_path).text
        #json_pattern = '/sites/default/files/COVID19/COVID19CountyResults[0-9_]+.json'
        #json_path = re.findall(json_pattern, raw_page)[0]
        # fetch and return latest dataset
        #data = json.loads(requests.get(dph_url + json_path).text)
        data = json.loads(requests.get('https://www.dph.illinois.gov/sitefiles/COVIDTestResults.json').text)
        last_update = datetime.date(**data['LastUpdateDate'])
        df = pd.DataFrame(data['characteristics_by_county']['values'])
        df.columns = df.columns.str.lower()
        df['date'] = pd.to_datetime(last_update)
        df.to_csv(DAILY_REPORT.format(datetime.date.strftime(last_update, '%Y%m%d')), index=False)    
        return True
    except:
        return False


def get_history():
    dfs = []
    for f in os.listdir(DAILY_REPORT_DIR):
        if f.endswith('.csv'):
            dfs.append(pd.read_csv(os.path.join(DAILY_REPORT_DIR, f)))
    df = (pd.concat(dfs, ignore_index=True)
          .assign(date = lambda x: pd.to_datetime(x['date']))
          .drop(['lat','lon'], axis=1))
    return df


def get_population_map():
    # from https://data.illinois.gov/dataset/131dceo_county_population_projections
    df = (
        pd.read_csv('../data/dceo_county_population_projections.csv')
        .query("Race == 'All' and `Age Group` == 'All'")
        [['State/County', '2020']])
    df.columns = ['county', 'pop']
    # remove spaces from county name for a clean join key
    df['county'] = df.county.str.replace(' ','')
    pop = dict(zip(df['county'], df['pop']))    
    # add Chicago and CookSuburbs for latest reports that have that breakout
    # from https://data.illinois.gov/dataset/436idph_population_projections_for_chicago_by_age_and_sex_2010_to_2025
    pop['Chicago'] = 2562913
    pop['SuburbanCook'] = pop['Cook'] - pop['Chicago']
    return pop


def get_cleaned_data():
    df = get_history()
    pop_map = get_population_map()
    
    ## split out Suburban Cook from Chicago

    cook_df = (df.query("county in ['Cook', 'Suburban Cook', 'Chicago']")
               .pivot(index='date', columns='county', values=['confirmed_cases','deaths']))

    def split_cook2(x):
        index_date = x[('date','')]
        if index_date < datetime.date.fromisoformat('2020-03-22'):
            chicago_ratio = 0.65
            cook_cases = x[('confirmed_cases','Cook')]
            chicago_cases = int(cook_cases * chicago_ratio + 0.5)
            suburb_cases = cook_cases - chicago_cases
            cook_deaths = x[('deaths','Cook')]
            chicago_deaths = int(cook_deaths * chicago_ratio + 0.5)
            suburb_deaths = cook_deaths - chicago_deaths
        else:
            chicago_cases = x['confirmed_cases','Chicago']
            suburb_cases = x[('confirmed_cases','Cook')]
            chicago_deaths = x['deaths','Chicago']
            suburb_deaths = x[('deaths','Cook')]

        index = pd.MultiIndex.from_tuples(
            [('date',''),
             ('confirmed_cases','Chicago'),
             ('confirmed_cases','Suburban Cook'),
             ('deaths', 'Chicago'),
             ('deaths', 'Suburban Cook')])

        return pd.Series([index_date, chicago_cases, suburb_cases, chicago_deaths, suburb_deaths], index=index)

    cook_updated_df = cook_df.reset_index().apply(split_cook2, axis=1).set_index('date')
    cook_updated_df = (
        (cook_updated_df['confirmed_cases']
         .reset_index()
         .melt(id_vars='date', var_name='county', value_name='confirmed_cases'))
        .merge(
            (cook_updated_df['deaths']
             .reset_index()
             .melt(id_vars='date', var_name='county', value_name='deaths')),
            on=['date','county']))
    cook_updated_df['total_tested'] = 0
    cook_updated_df['negative'] = 0
    cook_updated_df = cook_updated_df[['county', 'confirmed_cases', 'total_tested', 'negative', 
                                       'deaths', 'date']]
    # merge corrected Cook county results
    df = pd.concat([
        df.query("county not in ['Cook', 'Suburban Cook', 'Chicago']"),
        cook_updated_df], ignore_index=True)
    
    ## check if county level counts aggregate to state level results
    off_balance = (df.query('county == "Illinois"').reset_index().set_index('date').drop(['county'], axis=1)
                   - df.query('county != "Illinois"').groupby('date').sum().reset_index().set_index('date'))
    
    # drop unassigned
    df = df.query("county != 'Unassigned'")
    
    # merge in county level population projections
    df['pop'] = df['county'].apply(lambda x: pop_map[x.replace(' ','')])

    df = df.reset_index()
    
    # add off-balance cases and deaths to Chicago
    df.loc[df['county'] == 'Chicago', ['confirmed_cases', 'deaths']] =\
        df.loc[df['county'] == 'Chicago', ['confirmed_cases', 'deaths']] \
        + off_balance[['confirmed_cases', 'deaths']].values
    
    return df, off_balance[['confirmed_cases','deaths']]
