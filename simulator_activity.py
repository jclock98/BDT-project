import concurrent.futures
import json

import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm

from config import config
from confluent_kafka import Producer, KafkaError

import sys
from config import config
import time


def get_activities():
    try:
        params = config()

        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        sql = """SELECT istat, 
                ROUND((population*sporty_pop/2)), 
                m.region,
                daily_steps, 
                steps_stdev, 
                daily_pushups, 
                pushups_stdev, 
                daily_squats, 
                squats_stdev
                FROM municipalities AS m JOIN sport_activity AS s
                ON m.region = s.region"""
        cur.execute(sql)

        activities = cur.fetchall()
        print("Got activities!")

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
    columns = ["code", 
               "pop",
               "region",
               "daily_steps", 
               "steps_std", 
               "daily_pushups", 
               "pushups_std", 
               "daily_squats", 
               "squats_std"]
    activities = pd.DataFrame(activities, columns=columns)
    
    return activities


def get_devices(dim):
    devices_list = [f"D_{i}_{np.random.randint(0, dim)}" for i in range(dim)]
    return devices_list


def get_daily_activities(activities):
    total_daily_steps = []
    total_daily_squats = []
    total_daily_pushups = []

    scale = 0.156/2

    for x in tqdm(activities.itertuples()):
        pop = float(x.pop) 
        pop -= np.random.normal(0, 0.05)*pop
        
        new_steps = np.random.normal(float(x.daily_steps), float(x.steps_std), size=round(pop))
        new_steps = [int(num) for num in np.around(new_steps)]
        device_names = get_devices(round(pop))
        tmp_dict = dict()
        tmp_dict["code"] = x.code
        tmp_dict["type"] = "steps"
        tmp_dict["region"] = x.region
        tmp_dict["activity"] = [{"device": device, "quantity": steps} for device, steps in zip(device_names, new_steps)]
        total_daily_steps.append(tmp_dict)
        
        norm_pop = round(pop * scale)
        new_squats = np.random.normal(float(x.daily_squats), float(x.squats_std), size=norm_pop)
        new_squats = [int(num) for num in np.around(new_squats)]
        device_names = get_devices(norm_pop)
        tmp_dict = dict()
        tmp_dict["code"] = x.code
        tmp_dict["type"] = "squats"
        tmp_dict["region"] = x.region
        tmp_dict["activity"] = [{"device": device, "quantity": squats} for device, squats in zip(device_names, new_squats)]
        total_daily_squats.append(tmp_dict)
        
        new_pushups = np.random.normal(float(x.daily_pushups), float(x.pushups_std), size=norm_pop)
        new_pushups = [int(num) for num in np.around(new_pushups)]
        device_names = get_devices(norm_pop)
        tmp_dict = dict()
        tmp_dict["code"] = x.code
        tmp_dict["type"] = "pushups"
        tmp_dict["region"] = x.region
        tmp_dict["activity"] = [{"device": device, "quantity": pushups} for device, pushups in zip(device_names, new_pushups)]
        total_daily_pushups.append(tmp_dict)
    
    return total_daily_steps, total_daily_squats, total_daily_pushups


def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))


def send_activity(act_info):
    kafka_params = config(section="kafka")
    p = Producer(**kafka_params)
    topic = "activities"
    
    json_to_be = dict()
    json_to_be["timestamp"] = act_info["timestamp"]
    json_to_be["city_code"] = act_info["code"]
    json_to_be["region"] = act_info["region"]
    json_to_be["activity"] = act_info["activity"]
    json_to_be["type"] = act_info["type"]
    json_to_send = json.dumps(json_to_be)
    try:
        try:
            p.produce(topic, json_to_send.encode('utf-8'), callback=delivery_callback)
        except:
            pass
    except BufferError as e:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                            len(p))
    p.poll(0)
    p.flush()
    time.sleep(2)


def send_activity_concurr(act_info):
    with concurrent.futures.ThreadPoolExecutor() as pool:
            pool.map(send_activity, act_info)


def get_timestamped_list(curr_list, timestamp):
    for x in curr_list:
        x["timestamp"] = timestamp
    return curr_list


def main():
    activities = get_activities()
    timestamp_range = pd.date_range(start="2022-08-01 00:00:00", end="2022-09-01 00:00:00")
    timestamp_range = [x.timestamp() for x in timestamp_range]
    
    for timestamp in timestamp_range:
        total_daily_steps, total_daily_squats, total_daily_pushups = get_daily_activities(activities)
        print("Got daily activities!")
        new_total_daily_steps = get_timestamped_list(total_daily_steps, timestamp)
        new_total_daily_squats = get_timestamped_list(total_daily_squats, timestamp)
        new_total_daily_pushups = get_timestamped_list(total_daily_pushups, timestamp)
        activity_list = [new_total_daily_steps, new_total_daily_squats, new_total_daily_pushups]
        with concurrent.futures.ProcessPoolExecutor(3) as proc_pool:
            proc_pool.map(send_activity_concurr, activity_list)

if __name__ == '__main__':
    main()
