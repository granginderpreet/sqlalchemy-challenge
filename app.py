import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify, make_response

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy import desc
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurement = Base.classes.measurement
station = Base.classes.station
inspector = inspect(engine)
columns = inspector.get_columns('station')
for c in columns:
    print(c['name'], c["type"])

# Function to calculate the min and max date ranges for the database
#This function should be added before any app.routes are added 
def date_range():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Find the recent most date by querying the database
    results = session.query(measurement).order_by(measurement.date.desc()).limit(1)
    for result in results:
        recent_date= (result.date)
    print(f"Most recent date is {recent_date}")
    #Find the oldest date by querying the database
    results = session.query(measurement).order_by(measurement.date.asc()).limit(1)
    for result in results:
        oldest_date= (result.date)
    print(f"Oldest date is {recent_date}")   
    session.close()
    # Construct the response with range and results
    text= "Dates should be between " + oldest_date + " and " + recent_date + ". " 
    return((text))

# Function to calculate the min and max date ranges for the database
#This function should be added before any app.routes are added 

def start_date():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Find the recent most date by querying the database
    results = session.query(measurement).order_by(measurement.date.desc()).limit(1)
    for result in results:
        recent_date= (result.date)
    print(f"Most recent date is {recent_date}")
    #Find the oldest date by querying the database
    session.close()
    # Construct the response with range and results
    text= "Start Date should be earlier than " + recent_date + ". " 
    return((text))


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################



@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/><br/>"
        f"/api/v1.0/stations - Return station details for all stations <br/><br/>"
        f"/api/v1.0/precipitation - Returns precipation data for all stations <br/><br/>"
        f"/api/v1.0/tobs - Returns past year of temp data for most active station <br/><br/>"
        f"/api/v1.0/startdate in YYYY-MM-DD format -  Returns min/max/ave since the startdate {start_date()}<br/> <br/> "
        f"/api/v1.0/startdate/enddate in YYYY-MM-DD/YYYY-MM-DD format - Returns min/max/ave between startdate and enddate. Start date should be earlieer than end date. {date_range()}"
    )

#Reports the precipitation results in the database
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all prcp data
    results = session.query(measurement.date, measurement.prcp).all()

    session.close()
    prcp_results = []
    #Create a dictionary of prcp results. This is done generating a list and append to the dictionary
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        prcp_results.append(prcp_dict)

    return jsonify(prcp_results)
#Below command gives the same results as one above
#   return jsonify(list(np.ravel(prcp_results)))

#Query all the stations
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query station table
    results = session.query(station.name).all()

    session.close()
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

#App route for temperature
# Query the dates and temperature observations of the most active station for the last year of data.

# Return a JSON list of temperature observations (TOBS) for the previous year.

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Query for most active station 
    query = sqlalchemy.select([
    measurement.station,
    sqlalchemy.func.count( measurement.station)
]).group_by( measurement.station)
    result = engine.execute(query).fetchall()
    df=pd.DataFrame(result)
    #Sort with most count first
    df=df.sort_values(by='count_1', ascending=False)
    df.reset_index()
    station_with_max_measurements = df.iloc[0,0]\
    
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    query = sqlalchemy.select(measurement).filter( measurement.station == station_with_max_measurements).order_by(measurement.date.desc()).limit(1)
    results = engine.execute(query).fetchall()
    for result in results:
        recent_date= (result.date)
    print(recent_date)
    twelve_month_old_date = (datetime.strptime(recent_date, "%Y-%m-%d") - timedelta(weeks=52)).strftime("%Y-%m-%d")
    results = session.query(measurement.date).filter((
    measurement.station == station_with_max_measurements) & (measurement.date>=twelve_month_old_date)).with_entities(measurement.date,measurement.tobs)
    df=pd.DataFrame(results)
    #Drop the empty cells if any 
    df.dropna(subset=['tobs'], inplace=True)
    dict1=dict(zip(df.date, df.tobs))
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    #all_names = list(np.ravel(df))

#    return ( df.to_dict(orient="split",index=false) )  
    return ( jsonify(dict1) )  
@app.route("/api/v1.0/<start>")

def start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Query to check for dates > start date
    query = sqlalchemy.select([measurement,
    sqlalchemy.func.min( measurement.tobs), 
    sqlalchemy.func.max( measurement.tobs), 
    sqlalchemy.func.avg( measurement.tobs)]).filter( measurement.date > start)
    result = engine.execute(query).fetchall()
    df=pd.DataFrame(result)
    #Strip out rest of the columns to get the min, max, average
    df=df.loc[:,["min_1", "max_1","avg_1"]]
    #Rename the columns
    df=df.rename(columns={"min_1":"minimum", "max_1":"maximum", "avg_1":"average"})
    dict1=(df.to_dict("records"))
    #Find the range of dates
    date_check= start_date();
    print(date_check)
    session.close()
    #Tried to use make_response but gave up on it
    # text= "Dates should be between " + oldest_date + " and " + recent_date
    # response = make_response(dict1)
    # response.headers['Date Ranges'] = text
    # return make_response(response)
# Construct the response with range and results
    text= date_check + "Here are results:" + str(dict1)
    return(jsonify(text))
 

@app.route("/api/v1.0/<start>/<end>")
def window(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Query to check for dates greater than start and less than end date
    query = sqlalchemy.select([measurement,
    sqlalchemy.func.min( measurement.tobs), 
    sqlalchemy.func.max( measurement.tobs), 
    sqlalchemy.func.avg( measurement.tobs)]).filter((measurement.date > start) & (measurement.date < end))
    result = engine.execute(query).fetchall()
    df=pd.DataFrame(result)
    df=df.loc[:,["min_1", "max_1","avg_1"]]
    df=df.rename(columns={"min_1":"minimum", "max_1":"maximum", "avg_1":"average"})
    dict1=df.to_dict("records")
    session.close()
    date_check= date_range();
    #Add the date check string and return the results
    text= date_check + "Here are results:" + str(dict1)
    return(jsonify(text))


if __name__ == '__main__':
    app.run(debug=True)
