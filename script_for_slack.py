# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 21:19:29 2022

@author: Karan Chilwal
"""

import pandas as pd
import time


def read_file(path):
    #reads the input file and adds date, month_id, month_name column to the dataframe
    try:
        df = pd.read_csv(path, 
                 header = 0, infer_datetime_format = True, usecols=[1,2,3,4,5])
        df["date"]= pd.to_datetime(df["date"], errors='raise', format='%Y-%m-%d')
        df['month_id'] = df['date'].dt.strftime('%Y%m')
        df['month_name'] = df['date'].dt.month_name()
        df['year'] = df['date'].dt.strftime('%Y')
        return df
    except Exception as e:
        raise e

def monthly_aggregation_for_each_state(df):
    #grouping by state, month_id and aggregation of daily data to monthly data.
    monthly_df = df.groupby(['state', 'month_name', 'year','month_id'], as_index=False)[['fips','cases','deaths']].sum()
    return monthly_df
    
def cumulative_aggregation_for_each_month(df):
    #cumulative sum of deaths in each month
    total_monthly_deaths_df = df.groupby(['month_name', 'year','month_id'], as_index=False)[['deaths']].sum().sort_values(by = ['month_id'])
    total_monthly_deaths_df['cumulative_deaths'] = total_monthly_deaths_df['deaths'].cumsum(axis = 0)
    return total_monthly_deaths_df

'''

#max_death_states = monthly_df.loc[monthly_df.groupby(['month_name','year','month_id'])["deaths"].idxmax()]

id= monthly_df.groupby(['month_name','year','month_id'])["deaths"].transform(max) == monthly_df['deaths']
c= monthly_df[id]
'''

def sort_monthly_df(monthly_df):
    #sorting data in descending order by deaths after grouping by (to extract the top 3 states for highest deaths)
    sorted_death_states_df = monthly_df.groupby(['month_name','year','month_id'], as_index=False ).apply(lambda x : x.sort_values(by = ['deaths'], ascending = False))
    return sorted_death_states_df


#for group, data in  sorted_death_states_df.groupby(['month_name','year','month_id'], as_index=False ):
#    print (int(group[2]))
    
   

def prepare_slack_message(month_id, df, cumulative_df):
    for group, data in  df.groupby(['month_name','year','month_id'], as_index=False ):
        if (int(group[2]) == month_id): 
            message = '''Top 3 states with the highest number of covid deaths for the month of {}-{}
            Month - {}-{}
            1. {} - Number of deaths = {}, % of Total US deaths = {}
            2. {} - Number of deaths = {}, % of Total US deaths = {}
            3. {} - Number of deaths = {}, % of Total US deaths = {}'''.format(group[0],group[1], 
            group[0],group[1],
            data[0:1]['state'].to_list()[0],  data[0:1]['deaths'].to_list()[0],int(data[0:1]['deaths'].to_list()[0])/cumulative_df.loc[cumulative_df['month_id'] == str(month_id)]['cumulative_deaths'].to_list()[0]*100,
            data[1:2]['state'].to_list()[0],  data[1:2]['deaths'].to_list()[0],int(data[1:2]['deaths'].to_list()[0])/cumulative_df.loc[cumulative_df['month_id'] == str(month_id)]['cumulative_deaths'].to_list()[0]*100,
            data[2:3]['state'].to_list()[0],  data[2:3]['deaths'].to_list()[0],int(data[2:3]['deaths'].to_list()[0])/cumulative_df.loc[cumulative_df['month_id'] == str(month_id)]['cumulative_deaths'].to_list()[0]*100)
            #print((int(data[0:1]['deaths'].to_list()[0])/cumulative_df.loc[cumulative_df['month_id'] == str(month_id)]['cumulative_deaths'].to_list()[0])*100)
            return message
        
        
def send_slack_message(message: str):
    import requests
    
    payload = '{"text":"%s"}' % message
    response = requests.post("https://hooks.slack.com/services/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", 
                             data = payload)
    print(response.text)
 
    
def slack_message_month_range(startmonth_id, endmonth_id,df,cumulative_df):
    for i in range(startmonth_id, endmonth_id, 1):
        if str(i) in df.month_id.unique():
            message = prepare_slack_message(i, df, cumulative_df)
            time.sleep(5)
            send_slack_message(message)

def main():
    df = read_file("C:/Users/Karan Chilwal/Downloads/covid-19-state-level-data.csv")
    monthly_df = monthly_aggregation_for_each_state(df)
    total_monthly_deaths_df = cumulative_aggregation_for_each_month(df)
    sorted_death_states_df = sort_monthly_df(monthly_df)
    slack_message_month_range(202003,202007, sorted_death_states_df,total_monthly_deaths_df)


if __name__ == '__main__':
    main()
