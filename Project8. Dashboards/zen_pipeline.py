#!/usr/bin/python
# -*- coding: utf-8 -*-

#python3 zen_pipeline.py --start_dt='2019-09-24 18:00:00' --end_dt='2019-09-24 19:00:00'

import sys

import getopt
from datetime import datetime, timedelta

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":

    #Задаем входные параметры
    unixOptions = "s:e"
    gnuOptions = ["start_dt=", "end_dt="]
    
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:] #excluding script name
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:
        print (str(err))
        sys.exit(2)
    
    start_dt = ''
    end_dt = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue
    
    db_config = {'user': 'my_user',         
                 'pwd': 'my_user_password', 
                 'host': 'localhost',       
                 'port': 5432,              
                 'db': 'zen'}             
    
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                             db_config['pwd'],
                                                             db_config['host'],
                                                             db_config['port'],
                                                             db_config['db'])
    
    engine = create_engine(connection_string)

    query = ''' SELECT event_id,
                       age_segment,
                       event,
                       item_id,
                       item_type,
                       item_topic,
                       source_id,
                       source_type,
                       source_topic,
                       ts,
                       TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC' as dt,
                       user_id
                FROM log_raw
                WHERE TO_TIMESTAMP(ts/1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
            '''.format(start_dt, end_dt)

    raw = pd.io.sql.read_sql(query, con = engine, index_col = 'event_id')
    raw['dt'] = pd.to_datetime(raw['dt']).dt.round('min')

    dash_visits = (raw.groupby(['item_topic','source_topic','age_segment','dt'])
    	              .agg({'user_id':'count'})
    	              .rename(columns={'user_id':'visits'}))
    dash_visits = dash_visits.reset_index()

    dash_engagement = (raw.reset_index()
    	                  .groupby(['dt','item_topic','event','age_segment'])
    	                  .agg({'user_id':'sum'})
    	                  .reset_index()
    	                  .rename(columns={'user_id':'unique_users'}))

    tables = {'dash_engagement' : dash_engagement,
              'dash_visits' : dash_visits}

    for table_name, table_data in tables.items():

    	query = '''
    	           DELETE FROM {} WHERE dt BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
    	        '''.format(table_name, start_dt, end_dt)
    	engine.execute(query)

    	table_data.to_sql(name = table_name,
    		              con=engine,
    		              if_exists = 'append',
    		              index = False)

    print('All done.')