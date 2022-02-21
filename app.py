import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify

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
engine = create_engine("sqlite:///hawaii.sqlite")

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
        f"Available Routes:<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/tobs"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all passenger names"""
    # Query all passengers
    results = session.query(measurement.date, measurement.prcp).all()

    session.close()
    prcp_results = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        prcp_results.append(prcp_dict)

    return jsonify(prcp_results)
    # Convert list of tuples into normal list
    # all_names = list(np.ravel(results))

    # return jsonify(all_names)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(station.name).all()

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_names = list(np.ravel(results))

    return jsonify(all_names)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    query = sqlalchemy.select([
    measurement.station,
    sqlalchemy.func.count( measurement.station)
]).group_by( measurement.station)
    result = engine.execute(query).fetchall()
    df=pd.DataFrame(result)
    df=df.sort_values(by='count_1', ascending=False)
    df.reset_index()

    station_with_max_measurements = df.iloc[0,0]
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    query = sqlalchemy.select(measurement).filter( measurement.station == station_with_max_measurements).order_by(desc(measurement.date)).limit(1)
    results = engine.execute(query).fetchall()
    for result in results:
        recent_date= (result.date)
    print(recent_date)
    twelve_month_old_date = (datetime.strptime(recent_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
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
    return ( dict1 )  

if __name__ == '__main__':
    app.run(debug=True)
