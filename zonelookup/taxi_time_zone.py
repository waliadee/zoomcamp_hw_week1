#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd


# In[25]:


from sqlalchemy import create_engine


# In[50]:


engine=create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


# In[2]:


import wget


# In[9]:


df = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')


# In[10]:


df.head()


# In[63]:


df.to_sql(name='taxi_zone_lookup',con=engine,if_exists='append',index=False)


# In[64]:


sql="""select count(*) from public.taxi_zone_lookup;"""
pd.read_sql(sql,con=engine)


# In[65]:


sql="""select * from public.taxi_zone_lookup limit 10;"""
pd.read_sql(sql,con=engine)


# In[68]:


sql="""select * from public.green_taxi_trips limit 10;"""
pd.read_sql(sql,con=engine)


# In[78]:


sql="""select count(*) from public.green_taxi_trips
where 
lpep_pickup_datetime > to_date('2019-01-15','YYYY-MM-DD')
and lpep_dropoff_datetime < to_date('2019-01-16','YYYY-MM-DD')
;"""
pd.read_sql(sql,con=engine)


# In[79]:


sql="""with cte as (select lpep_pickup_datetime,trip_distance,row_number() over(order by trip_distance desc) as rk from public.green_taxi_trips)

select * from cte
where rk=1
;"""
pd.read_sql(sql,con=engine)


# In[84]:


sql="""select  passenger_count,count(*) from public.green_taxi_trips 
where to_char(lpep_pickup_datetime,'YYYY-MM-DD') = '2019-01-01'
and passenger_count in (2,3)
group by passenger_count
"""
pd.read_sql(sql,con=engine)


# In[110]:


sql="""with cte as (select  do2."Zone",tip_amount,row_number() over (order by tip_amount desc) as rk
from public.green_taxi_trips  t1
inner join public.taxi_zone_lookup pu
on t1."PULocationID" = pu."LocationID"
inner join public.taxi_zone_lookup do2
on t1."DOLocationID" = do2."LocationID"
where lower(pu."Zone") = 'astoria')

select * from cte
where rk=1
"""
pd.read_sql(sql,con=engine)


# In[ ]:




