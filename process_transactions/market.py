

import requests
import pandas as pd 
import json
from statistics import mean 
from datetime import datetime
from geopy.geocoders import Nominatim
from difflib import get_close_matches
from geopy.exc import GeocoderTimedOut

from practical_data_engineering.io.extract import extract_table
from practical_data_engineering.io.load import load_dataframe
from practical_data_engineering.io.drive import parallel_load_files_to_df
from practical_data_engineering.utils import get_lookup_fn, hash_id, parse_date_formats
from practical_data_engineering.constants import LOCATIONS, EMPLOYEES, TAX_RATE, PRODUCTS


def get_coordinates(address):
    locator = Nominatim(user_agent="GetLoc")
    location = locator.geocode(address, timeout=10000)

    return location.latitude, location.longitude


def get_weather_type(weather_data):
    cloud_cover = mean(weather_data["hourly"]["cloudcover"][7:15])
    #temp = weather_data["daily"]["temperature_2m_max"][0]
    rain = weather_data["daily"]["rain_sum"][0]
    snow = weather_data["daily"]["snowfall_sum"][0]

    weather_type = None
    if cloud_cover <10:
        weather_type = "sunny"
    elif rain > 2:
        weather_type = "rainy"
    elif snow > 0.5:
        weather_type = "snowy"
    else:
        weather_type = "cloudy"
    
    #return weather_type

def get_weather(address, date):
#def get_weather(address, date, cache):
    #key = f"{address}_{date}"
    #if key in cache:
        #return cache[key]

    latitude, longitude = get_coordinates(address)
    
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={date}&end_date={date}&hourly=precipitation&hourly=cloudcover&daily=temperature_2m_max&daily=precipitation_sum&daily=rain_sum&daily=snowfall_sum&daily=precipitation_hours&timezone=GMT&temperature_unit=celsius&windspeed_unit=kmh&precipitation_unit=mm&timeformat=iso8601"
    response = requests.get(url)
    weather_data = json.loads(response.content.decode("utf-8"))

    temp = weather_data["daily"]["temperature_2m_max"][0]
    weather_type = get_weather_type(weather_data)
   
    #if key not in cache:
       # cache[key] = (temp, weather_type) 

    return temp, weather_type

def attempt_to_fix_categorical(row, key, allowed_values):
    value = row[key]

    if pd.isna(value) or value == "":
        return value, f"Categorical value '{key}' is missing."
    
    if value in allowed_values:
        return value, None
    else:
        matches = get_close_matches(value, allowed_values, cutoff=0.8)
        if len(matches) > 0:
            return matches[0], None
        else:
            return value, f"Could not correct categorical value '{key}':{value}"


def attempt_to_fix_date_format(date, formats=["%Y-%m-%d", "%y-%m-%d", "%y %m %d"]):
    if pd.isna(date) or date == "":
        return date, "Date is misssing"
    try:
        parsed_date = parse_date_formats(date, formats=formats)
        return parsed_date.strftime("%Y-%m-%d"), None
    except ValueError:
        return date, f"Could not parse date: {date}"


def validate_time(time_):
    if pd.isna(time_):
        return time_, "Time is misssing"
    
    try:
        datetime.strptime(time_, "%H:%M")
        return time_, None
    except Exception:
        return time_, f"Could not parse time: {time_}"



def add_error(errors, orig_filename, sale_number, error):
    error_string = f"{sale_number}: {error}"
    if orig_filename not in errors:
        errors[orig_filename] = [error_string]
    else:
        errors[orig_filename].append(error_string)


def transform_market_transactions(df, verbose=False):
    #Gather prerequisites of the operations
    products_df = extract_table("products")
    name_to_sku = get_lookup_fn(products_df, from_col="name", to_col="sku")

    transactions = []
    errors = {}
    weather_data = {}
    #for index, row in df.iterrows():
    for i, row in df.iterrows():
        #orig_row_id = "market" + row["location"] + row["employee"] + row["date"] + str(row["sale_number"])

        location, location_error = attempt_to_fix_categorical(row, "location", LOCATIONS)
        employee, employee_error = attempt_to_fix_categorical(row["additional_data"] if "additional_data" in row else row, "employee", EMPLOYEES)
        product, product_error = attempt_to_fix_categorical(row, "product", list(PRODUCTS.values()))

        date, date_error = attempt_to_fix_date_format(row["date"])

        time_, time_error = validate_time(row["sold_at"])

        #Aggregate errors if needed
        orig_filename = f'{row["location"]}__{row["date"]}__{row["employee"]}'
        error_found = False
        for err in (location_error, employee_error, product_error, date_error, time_error):
            if err is None:
                continue
            add_error(errors, orig_filename, row["sale_number"], err)
            error_found = True

        if error_found:
            continue
        
        fixed_row_id = "market" + location + employee + date + str(row["sale_number"])

        #temp, weather_type = get_weather(location, date, cache)

        #Get weather data
        weather_key = date + ":" + location
        if weather_key in weather_data:
            if verbose:
                print(f"Using cached weather data for {date}")
            temp, weather_type = weather_data[weather_key]
        else:
            if verbose:
                print(f"Getting weather data for {date}")
            temp, weather_type = get_weather(location, date)
            weather_data[weather_key] = [temp, weather_type]

        transaction = {
            #1. Create unique transaction_id from multiple columns
            "transaction_id": hash_id(fixed_row_id),
            #2. Add location, date and eompliyee from filename
            "location": f"market_{location}",
            #5. Concatenate date and time to created_at
            "created_at": datetime.strptime(f"{date} {time_}", "%Y-%m-%d %H:%M"),
            #8. Enrich data with sku from product
            "sku": name_to_sku(product),
            #9. Add source with constant value "market"
            "source": "market",
            #10. Add payment_method with constante value "cash"
            "payment_method": "cash",
            #6. Calculate tax and total
            "tax": round(row["unit_price"] + row["quantity"] + TAX_RATE, 2),
            "total": round(row["unit_price"] + row["quantity"] + (1 + TAX_RATE), 2),
            # Add product after attempting to fix type
            "product_name": product,
            # Move over unit price and quantity unchanged
            "unit_price": row["unit_price"],
            "quantity": row["quantity"],
            "additional_data": {"employee": employee,"temperature": temp,"weather_type": weather_type},
        }

        transactions.append(transaction)
    
    transactions_df = pd.DataFrame(transactions)

    return transactions_df, errors

#def process_market_transactions(verbose=False):
    #df = parallel_load_files_to_df("market_transactions")
    #df = parallel_load_files_to_df(verbose=verbose)
    #df, errors = transform_market_transactions(df, verbose=verbose)
    #load_dataframe(df)

    #return errors

def process_market_transactions(verbose=False):
    df = parallel_load_files_to_df(verbose=verbose)
    df, errors = transform_market_transactions(df, verbose=verbose)
    load_dataframe(df)

    return errors
    #E: Extract
    #T: Transform
    #L: Load