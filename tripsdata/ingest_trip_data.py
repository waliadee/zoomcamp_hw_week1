import argparse 
import pandas as pd 
from sqlalchemy import create_engine
import sys 
import os
import wget

def main(params):
    print('start')
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    output_file='tripdata.csv.gz'

    if os.path.isfile(output_file):
        os.remove(output_file)
    wget.download(url,output_file)

    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    df_iter=pd.read_csv(output_file,compression='gzip',chunksize=10000,iterator=True)
    ctr=0
    for df in df_iter:
        ctr+=df.shape[0]
        print(ctr)
        df['lpep_dropoff_datetime']=pd.to_datetime(df['lpep_dropoff_datetime'])
        df['lpep_pickup_datetime']=pd.to_datetime(df['lxspep_pickup_datetime'])
        df.to_sql(name=table_name,con=engine,if_exists='append',index=False)
    print('done')


    print(sys.argv)
    day = sys.argv[1]

    print('job finished successfully for day = ', day)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description="ingest csv data into Postgres")
    parser.add_argument('--user',help='username for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='db for postgres')
    parser.add_argument('--table_name',help='table for postgres')
    parser.add_argument('--url',help='data source url')

    args=parser.parse_args()
    main(args)