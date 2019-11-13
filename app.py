import os

import pandas as pd
import numpy as np

from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine


from config import PGHOST, PGDATABASE, PGUSER, PGPASSWORD

app = Flask(__name__)

#################################################
# Database Setup
#################################################


app.config['SQLALCHEMY_DATABASE_URI'] = (f'postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}/{PGDATABASE}')
db = SQLAlchemy(app)

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(db.engine, reflect=True)

# Save references to each table
account_apprl_year_2019 = Base.classes.account_apprl_year_2019
account_apprl_year_2018 = Base.classes.account_apprl_year_2018
account_apprl_year_2017 = Base.classes.account_apprl_year_2017
account_apprl_year_2016 = Base.classes.account_apprl_year_2016
account_apprl_year_2015 = Base.classes.account_apprl_year_2015
account_info_2019=Base.classes.account_info_2019
account_info_2018=Base.classes.account_info_2018
account_info_2017=Base.classes.account_info_2017
account_info_2016=Base.classes.account_info_2016
account_info_2015=Base.classes.account_info_2015
predictions=Base.classes.predictions



# engine
engine = create_engine(f'postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}/{PGDATABASE}')

# render index.html on homepage
@app.route("/")
def index():
    """Return the homepage."""
    return render_template("index.html")


@app.route("/data/<account_num>")
def data(account_num):

    # query: join tables
    query = (f"select ai.account_num, street_num, street_half_num, full_street_name, unit_id, property_city, left(property_zipcode,5) as Zipcode, aay.tot_val \
            from account_info_2019 as ai \
            INNER JOIN account_apprl_year_2019 as aay on ai.account_num = aay.account_num \
            where ai.account_num = '{account_num}'")

    results = engine.execute(query).fetchall()  

    test_dict = {}
    for result in results:
        test_dict["account_number"] = result[0]
        test_dict["street_number"] = result[1]
        test_dict["street_half_num"] = result[2]
        test_dict["full_street_name"] = result[3]
        test_dict["unit_id"] = result[4]
        test_dict["property_city"] = result[5]
        test_dict["property_zipcode"] = result[6]
        test_dict["total_value"] = int(result[7])

    return jsonify(test_dict)


@app.route("/addresses")
def address():

    # query: join tables
    query_1 = "select ai.account_num, concat(street_num, ' ', ai.full_street_name, ' ', ai.unit_id, ' ', ai.property_city, ' ', left(ai.property_zipcode,5))\
    from account_info_2019 as ai where ai.division_cd = 'RES'"

    results = engine.execute(query_1).fetchall()  

    address_list =[]

    addresses_dict = {} 
    for result in results:
        addresses_dict = {
        'account_number':result[0],
        'address':result[1]
        }

        address_list.append(addresses_dict)

    return jsonify(address_list)


@app.route("/attributes/<account_num>")
def attributes(account_num):
    """Return attribute data"""

    # query: join tables
    query_2 = (f"SELECT rd.account_num, rd.act_age, rd.tot_living_area_sf, rd.num_kitchens,\
                rd.num_full_baths, rd.num_half_baths, rd.num_bedrooms, aay.tot_val\
                FROM res_detail_2019 as rd\
                INNER JOIN account_apprl_year_2019 as aay on rd.account_num = aay.account_num\
                where rd.account_num = '{account_num}'")

    results = engine.execute(query_2).fetchall()  

    attribute_list =[]

    attribute_dict = {} 
    for result in results:
        attribute_dict = {
        'account_number':result[0],
        'property_age':result[0],
        'total_living_square_feet':result[0],
        'number_kitchens':result[0],
        'number_full_baths':result[0],
        'number_half_baths':result[0],
        'number_bedrooms':result[0],
        'total_value':result[0],
        }

        attribute_list.append(attribute_dict)

    return jsonify(attribute_list)



@app.route("/prediction/<account_num>")
def prediction(account_num):
    """Return prediction data"""
    sel = [
        predictions.account_num,
        predictions.appraisal_yr,
        predictions.prediction,
        predictions.Confidence,
        predictions.Uncertainty,
    ]
    results = db.session.query(
        *sel).filter(predictions.account_num == account_num).all()
    # Create a dictionary entry for each row of math data information
    prediction_dict = {}
    for result in results:
        prediction_dict["account_number"] = result[0]
        prediction_dict["appraisal_year"] = result[1]
        prediction_dict["prediction"] = result[2]
        prediction_dict["Confidence"] = result[3]
        prediction_dict["Uncertainty"] = result[4]
   # Return Jsonified data ()
    print(prediction_dict)
    return jsonify(prediction_dict)

if __name__ == "__main__":
    app.run(debug=True)
