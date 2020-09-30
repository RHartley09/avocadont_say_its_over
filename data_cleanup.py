#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta
from config import password
password = password


# In[2]:


transportData = "Refrigerated_Truck_Rates_and_Availability_Full.csv"
AvoTransportData = "Refrigerated_Truck_Rates_and_Availability_Avo.csv"
avo_data = "avo_prices.csv"
gas_data = "gas_prices.csv"
loadweather = "SanDiegoWeatherData.csv"
bananaprices = "banana_prices.csv"


# In[3]:


banana_prices_df = pd.read_csv(bananaprices)
# banana_prices_df.head()


# In[4]:


initSDweather_df = pd.read_csv(loadweather)
# initSDweather_df


# In[5]:


initial_transport_df = pd.read_csv(transportData)
# initial_transport_df.head()


# In[6]:


initial_avo_transport_df = pd.read_csv(AvoTransportData)
# initial_avo_transport_df.head()


# In[7]:


avo_data_df = pd.read_csv(avo_data)
# avo_data_df.head()


# In[8]:


gas_data_df = pd.read_csv(gas_data)
# gas_data_df.head()


# In[9]:


# Create a filtered dataframe from specific columns
tot_transport_columns = ["Date", "Origin", "Destination", "Distance", "Commodity", "Week Low", "Week High", "Midpoint", "Rate Per Mile", "Availability"]
tot_transport_df = initial_transport_df[tot_transport_columns].copy()

# Rename the column headers
tot_transport_df = tot_transport_df.rename(columns={"Date": "date",
                                                        "Origin": "origin",
                                                        "Destination": "destination",
                                                       "Distance": "transport_distance",
                                                       "Commodity": "commodity",
                                                       "Week Low": "low_weekly_rate",
                                                       "Week High": "high_weekly_rate",
                                                       "Midpoint": "average_weekly_rate",
                                                       "Rate Per Mile": "avg_rate_per_mile_ratio",
                                                       "Availability": "availability_score"
                                                   })

# tot_transport_df.head()


# In[10]:


tot_transport_df['date'] = pd.to_datetime(tot_transport_df['date'])
# tot_transport_df

tot_transport_df['date'] = tot_transport_df['date'] - timedelta(days=1)
# tot_transport_df


# In[11]:


# Create a filtered dataframe from specific columns
avo_transport_columns = ["Date", "Origin", "Destination", "Distance", "Commodity", "Week Low", "Week High", "Midpoint", "Rate Per Mile", "Availability"]
avo_transport_df = initial_avo_transport_df[avo_transport_columns].copy()

# Rename the column headers
avo_transport_df = avo_transport_df.rename(columns={"Date": "date",
                                                        "Origin": "origin",
                                                        "Destination": "destination",
                                                       "Distance": "transport_distance",
                                                       "Commodity": "commodity",
                                                       "Week Low": "low_weekly_rate",
                                                       "Week High": "high_weekly_rate",
                                                       "Midpoint": "average_weekly_rate",
                                                       "Rate Per Mile": "avg_rate_per_mile_ratio",
                                                       "Availability": "availability_score"
                                                   })

# avo_transport_df.head()


# In[12]:


avo_transport_df['date'] = pd.to_datetime(avo_transport_df['date'])
# avo_transport_df

avo_transport_df['date'] = avo_transport_df['date'] - timedelta(days=2)
# avo_transport_df


# In[13]:


avo_transport_df['date'] = avo_transport_df['date'].dt.strftime('%m/%d/%Y')
tot_transport_df['date'] = tot_transport_df['date'].dt.strftime('%m/%d/%Y')


# In[14]:


avo_transport_df.to_csv("avo_transport_df.csv", index=False, header=True)
tot_transport_df.to_csv("tot_transport_df.csv", index=False, header=True)


# In[14]:


# Create a filtered dataframe from specific columns
avo_columns = ["New Date", "AveragePrice", "Total Volume", "4046", "4225", "4770", "type", "year", "region"]
avo_transformed_df = avo_data_df[avo_columns].copy()

# Rename the column headers
avo_transformed_df = avo_transformed_df.rename(columns={"New Date": "date",
                                                        "AveragePrice": "average_price",
                                                        "Total Volume": "total_volume",
                                                       "4046": "small_avocados_sold",
                                                       "4225": "large_avocados_sold",
                                                       "4770": "xl_avocados_sold",
                                                       "type": "type",
                                                       "year": "year",
                                                       "region": "region"})

# avo_transformed_df.head()


# In[15]:


# Create a filtered dataframe from specific columns
gas_columns = ["Date", "Gasoline - All Grades", "Regular", "Midgrade", "Premium", "Diesel (On-Highway) - All Types"]
gas_transformed_df = gas_data_df[gas_columns].copy()

# Rename the column headers
gas_transformed_df = gas_transformed_df.rename(columns={"Date": "date",
                                                        "Gasoline - All Grades": "gas_all_grades",
                                                        "Regular": "regular",
                                                        "Midgrade": "midgrade",
                                                        "Premium": "premium",
                                                        "Diesel (On-Highway) - All Types": "diesel"})

# gas_transformed_df.head()


# In[16]:


# Ensure dates have the same formatting
avo_transformed_df['date'] = pd.to_datetime(avo_transformed_df.date)
gas_transformed_df['date'] = pd.to_datetime(gas_transformed_df.date)

avo_transformed_df['date'] = avo_transformed_df['date'].dt.strftime('%m/%d/%Y')
gas_transformed_df['date'] = gas_transformed_df['date'].dt.strftime('%m/%d/%Y')

# gas_transformed_df.head() 


# In[17]:


# Filter data based on region and type
avo_transformed_df = avo_transformed_df.loc[avo_transformed_df['region'] == 'TotalUS']
avo_transformed_df = avo_transformed_df.loc[avo_transformed_df['type'] == 'conventional']
# from datetime import datetime, timedelta

# for date in avo_transformed_df['date']:
#     date += timedelta(days=1)
# avo_transformed_df.head(3)


# In[18]:


ban_columns = ["Date", "Banana Price per Pound", "Percent Change"]
bananas_df = banana_prices_df[ban_columns].copy()

# Rename the column headers
bananas_df = bananas_df.rename(columns={"Date": "date",
                                        "Banana Price per Pound": "price_per_pound",
                                        "Percent Change": "percent_change",
                                        })

# bananas_df.head()


# In[19]:


SDweather_df = initSDweather_df[["dt", "temp", "rain_1h", "rain_3h", "snow_1h", "snow_3h", "weather_description"]]
# SDweather_df


# In[20]:


SDweather_df["dt"] = pd.to_datetime(SDweather_df["dt"], unit="s")
# SDweather_df


# In[21]:


SDweather_df['date'] = [d.date() for d in SDweather_df['dt']]
SDweather_df['time'] = [d.time() for d in SDweather_df['dt']]
# SDweather_df


# In[22]:


weather_columns = ["date", "temp", "rain_1h", "rain_3h", "snow_1h", "snow_3h", "weather_description"]
sanDiegoWeather_df = SDweather_df[weather_columns].copy()
# sanDiegoWeather_df


# In[24]:


avo_transformed_df = avo_transformed_df.dropna()
gas_transformed_df = gas_transformed_df.dropna()
tot_transport_df = tot_transport_df.dropna()
avo_transport_df = avo_transport_df.dropna()
bananas_df = bananas_df.dropna()
sanDiegoWeather_df = sanDiegoWeather_df.dropna()


# In[25]:


# Create database connection
app.config['DATABASE_URL'] = os.environ.get('postgres://hqvlqzjiivxhbq:2939ea5341bc0ffdd6b426d3b5dce6b2530fffa968c8171db36b48fc24d71d0d@ec2-52-200-82-50.compute-1.amazonaws.com:5432/d14jp5o6rlp0b8', '')


# In[26]:


avo_transformed_df.to_sql(name='avocado', con=engine, if_exists='append', index=False)
gas_transformed_df.to_sql(name='gas', con=engine, if_exists='append', index=False)


# In[27]:


tot_transport_df.to_sql(name='tot_transport', con=engine, if_exists='append', index=False)
avo_transport_df.to_sql(name='avo_transport', con=engine, if_exists='append', index=False)


# In[28]:


bananas_df.to_sql(name='banana_prices', con=engine, if_exists='append', index=False)


# In[29]:


sanDiegoWeather_df.to_sql(name='san_diego', con=engine, if_exists='append', index=False)


# In[30]:


pd.read_sql_query('select * from avocado', con=engine).head()


# In[31]:


pd.read_sql_query('select * from gas', con=engine).head()


# In[32]:


pd.read_sql_query('select * from tot_transport', con=engine).head()


# In[33]:


pd.read_sql_query('select * from avo_transport', con=engine).head()


# In[34]:


pd.read_sql_query('select * from banana_prices', con=engine).head()


# In[35]:


pd.read_sql_query('select * from san_diego', con=engine).head()


# In[36]:


SDgroup = sanDiegoWeather_df.groupby(['date']).sum()
SDgroup


# In[37]:


SDgroup2 = SDgroup.reset_index()
SDgroup2


# In[39]:


SDgroup2 = SDgroup2.dropna()


# In[40]:


SDgroup2.to_sql(name='san_diego2', con=engine, if_exists='append', index=False)


# In[41]:


pd.read_sql_query('select * from san_diego2', con=engine).head()

