# import dependencies
from flask import Flask, render_template
from flask_sqlalchemy import sqlalchemy
import psycopg2
import pandas as pd
# from bson import json_util
import json
import datetime

# import my password from config.py
from config import password

# create engine and connection to postgres
from sqlalchemy import create_engine

db_name = "avocado_db"
engine = create_engine(f'postgresql://postgres:{password}@localhost:5432/{db_name}')
connection = engine.connect()
connection

app = Flask(__name__)

@app.route("/")
def index():
    avo_data = get_data()
    transport = get_transport()
    # weather = get_weather()
    # bananas = get_bananas()
    # gas = get_gas()

    return render_template("index.html", avo_data=avo_data, transport=transport) #weather=weather, bananas=bananas, gas=gas

@app.route("/api/v1.0/data")
def get_data():
    avo_data = {}
    
    # AVOCADO PRICES DATA ---
    # import SQL table as pandas dataframe
    avocado_df = pd.read_sql('select * from avocado', connection)
    
    # convert pandas dataframe to json
    avocado_json = json.dumps(avocado_df.to_dict('records'), default=str)
    
    avo_data['avocado_prices'] = avocado_json
    # --- AVOCADO PRICES DATA ---

    # GAS PRICES DATA ---
    # import SQL table as pandas dataframe
    gas_df = pd.read_sql('select * from gas', connection)
    
    # convert pandas dataframe to json
    gas_json = json.dumps(gas_df.to_dict('records'), default=str)
    
    avo_data['gas_prices'] = gas_json
    # --- GAS PRICES DATA ---

    # TOT TRANSPORT PRICES DATA ---
    # import SQL table as pandas dataframe
    tot_tr_df = pd.read_sql('select * from tot_transport', connection)
    
    # convert pandas dataframe to json
    tot_tr_json = json.dumps(tot_tr_df.to_dict('records'), default=str)
    
    avo_data['tot_transport'] = tot_tr_json
    # --- TOT TRANSPORT PRICES DATA ---

    #  AVO TRANSPORT PRICES DATA ---
    # import SQL table as pandas dataframe
    avo_tr_df = pd.read_sql('select * from avo_transport', connection)
    
    # convert pandas dataframe to json
    avo_tr_json = json.dumps(avo_tr_df.to_dict('records'), default=str)
    
    avo_data['avo_transport'] = avo_tr_json
    # --- AVO TRANSPORT PRICES DATA ---

    #  WEATHER DATA ---
    # import SQL table as pandas dataframe
    weather_df = pd.read_sql('select * from san_diego', connection)
    weather_df2 = pd.read_sql('select * from san_diego2', connection)

    # convert pandas dataframe to json
    weather_json = json.dumps(weather_df.to_dict('records'), default=str)
    weather_json2 = json.dumps(weather_df2.to_dict('records'), default=str)

    avo_data['weather'] = weather_json
    avo_data['weather2'] = weather_json2
    # --- WEATHER DATA ---

    #  BANANA PRICES DATA ---
    # import SQL table as pandas dataframe
    bananas_df = pd.read_sql('select * from banana_prices', connection)
    
    # convert pandas dataframe to json
    bananas_json = json.dumps(bananas_df.to_dict('records'), default=str)
    
    avo_data['bananas'] = bananas_json
    # --- BANANA PRICES DATA ---

    return avo_data

@app.route("/api/v1.0/transport")
def get_transport():

    transport = {}
    # TRANSPORT & PRICES DATA ---
    # import SQL table as pandas dataframe
    tot_tr_df = pd.read_sql('select * from tot_transport', connection)
    avo_tr_df = pd.read_sql('select * from avo_transport', connection)
    avocado_df = pd.read_sql('select * from avocado', connection)

    # convert pandas dataframe to json
    tot_tr_json = json.dumps(tot_tr_df.to_dict('records'), default=str)
    # avo_tr_json = json.dumps(avo_tr_df.to_dict('records'), default=str)
    # avocado_json = json.dumps(avocado_df.to_dict('records'), default=str)
    
    # transport["tot"] = tot_tr_json
    # transport["avo"] = avo_tr_json
    # transport["price"] = avocado_json
    

    return tot_tr_json

    # cursor = connection.execute('SELECT * FROM tot_transport;')
    # tot_tr_df = cursor.fetchall() 
    # tot_tr_json = tot_tr_df.to_dict('records')
    # return tot_tr_df
    # ---- check into alternatives to pd.read_sql, execute command, don't use pandas, run sql query

if __name__ == "__main__":
    app.run(debug = True)