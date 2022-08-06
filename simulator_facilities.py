import json
from matplotlib.font_manager import json_dump
import psycopg2
from config import config
import pandas as pd
from tqdm import tqdm
import numpy as np
import concurrent.futures
import json 


def get_percents(counts):
    percents = dict()
    total = 1
    
    while total > 0.1:
        if "fitness_centre" in counts:
            new_percs = total/3
            if "fitness_centre" in percents: 
                percents["fitness_centre"] += new_percs
            else:
                percents["fitness_centre"] = new_percs
            total -= new_percs

        if "swimming_pool" in counts:
            new_percs = total/3
            if "swimming_pool" in percents:
                percents["swimming_pool"] += new_percs
            else:
                percents["swimming_pool"] = new_percs
            total -= new_percs
            
        if "stadium" in counts:
            new_percs = total/3
            if "stadium" in percents:
                percents["stadium"] += new_percs
            else:
                percents["stadium"] = new_percs
            total -= new_percs

        if "swimming_area" in counts:
            new_percs = total*3/4
            if "swimming_area" in percents:
                percents["swimming_area"] += new_percs
            else:
                percents["swimming_area"] = new_percs
            total -= new_percs

        if "ice_rink" in counts:
            new_percs = total/5
            if "ice_rink" in percents:
                percents["ice_rink"] += new_percs
            else:
                percents["ice_rink"] = new_percs
            total -= new_percs
            
    carry = total / len(percents)
    percents = {k:v+carry for k,v in percents.items()}
    return percents


def get_curr_pop(typology, counter, population):
    if typology == "fitness_centre":
        curr_pop = population["gym"]//counter["fitness_centre"]
    elif typology == "swimming_pool":
        curr_pop = population["pool"]//counter["swimming_pool"]
    elif typology == "stadium":
        curr_pop = population["stadium"]//counter["stadium"]
    elif typology == "swimming_area":
        curr_pop = population["swim"]//counter["swimming_area"]
    elif typology == "ice_rink":
        curr_pop = population["rink"]//counter["ice_rink"]
    return curr_pop


def get_facilities_data():
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
                
        cur = conn.cursor()

        sql = """SELECT municipality, id, typology
                 FROM places
                 GROUP BY municipality, id"""
        cur.execute(sql)
        
        facilities = cur.fetchall()
        print("Got facilities!")
        
        sql = """SELECT istat, (population*sporty_pop/2)
                 FROM municipalities"""
        cur.execute(sql)
        print("Got municipalities!")
        
        municipalities = cur.fetchall()
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    facilities = pd.DataFrame(facilities, columns=["code", "id", "typology"])
    return facilities, municipalities


def get_daily_access_to_facilities(facilities, municipalities):
    new_fac = []
    for istat, pop in tqdm(municipalities):
        tmp_pop = float(pop)
        tmp_pop -= np.random.normal(0, 0.05)*tmp_pop

        munic_facilities = facilities[facilities["code"] == istat]
        if munic_facilities.shape[0] == 0:
            continue
        facilities_count = munic_facilities.groupby("typology").count().id.to_dict()
        
        scales = get_percents(facilities_count)
        pop_for_typology = dict()
        
        if "fitness_centre" in facilities_count:
            pop_for_typology["gym"] = tmp_pop * scales["fitness_centre"]
        
        if "swimming_pool" in facilities_count:
            pop_for_typology["pool"] = tmp_pop * scales["swimming_pool"]
        
        if "stadium" in facilities_count:
            pop_for_typology["stadium"] = tmp_pop * scales["stadium"]
        
        if "swimming_area" in facilities_count:
            pop_for_typology["swim"] = tmp_pop * scales["swimming_area"]
        
        if "ice_rink" in facilities_count:
            pop_for_typology ["rink"]= tmp_pop * scales["ice_rink"]
        
        for facility in munic_facilities.itertuples():
            curr_pop = get_curr_pop(facility.typology, facilities_count, pop_for_typology)
            new_access = {"code": facility.code,
                          "id": facility.id,
                          "typology": facility.typology,
                          "accesses": curr_pop}
            new_fac.append(new_access)

    return new_fac


def send_access(fac_info):
    #TODO: send to kafka
    access, timestamp = fac_info
    json_to_be = dict()
    json_to_be["timestamp"] = timestamp
    json_to_be["city_code"] = access["code"]
    json_to_be["id"] = access["id"]
    json_to_be["typology"] = access["typology"]
    json_to_be["accesses"] = access["accesses"]
    json_to_send = json.dumps(json_to_be)
    if json_to_send is None:
        raise Exception
    print(json_to_send)


def main():
    facilities, municipalities = get_facilities_data()
    timestamp_range = pd.date_range(start="2017-01-01 00:00:00", end="2022-12-31 00:00:00")
    timestamp_range = [x.timestamp() for x in timestamp_range]
    
    for timestamp in timestamp_range:
        daily_access_to_facilities = get_daily_access_to_facilities(facilities, municipalities)
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            results = list(pool.map(send_access, 
                                    zip(daily_access_to_facilities,
                                        [timestamp for _ in range(len(daily_access_to_facilities))])))
            print(results)

if __name__ == '__main__':
    main()