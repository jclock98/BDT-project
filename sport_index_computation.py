import psycopg2 as psy
import pandas as pd
from config import config
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
import pymongo
from pyspark.sql.functions import col, lit
import json

def get_facilities_per_pop():
    try:
        params = config()

        conn = psy.connect(**params)

        cur = conn.cursor()

        sql = """SELECT region, sum(population), count(p.id) 
                FROM municipalities as m join places as p on m.istat = p.municipality
                GROUP BY region"""
        cur.execute(sql)
        print("Got municipalities!")

        fac_per_pop = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psy.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    fac_per_pop = [(x[0], float(x[1]), x[2]) for x in fac_per_pop]
    fac_per_pop = pd.DataFrame(fac_per_pop, columns=["region", "population", "facilities"])
    fac_per_pop["ratio"] = round(fac_per_pop["facilities"]/fac_per_pop["population"]*100000,2)
    fac_per_pop.drop(["population", "facilities"], axis=1, inplace=True)
    fac_per_pop["norm_ratio"] = (fac_per_pop["ratio"]-fac_per_pop["ratio"].min())/(fac_per_pop["ratio"].max()-fac_per_pop["ratio"].min())
    return fac_per_pop

def get_trends():
    from pytrends.request import TrendReq

    pytrends = TrendReq(hl='it-IT', geo='IT')
    kw_list = ["sport"]
    pytrends.build_payload(kw_list,timeframe = 'now 1-d' )
    sport_interest = pytrends.interest_by_region(resolution='Italy', inc_low_vol=True, inc_geo_code=False)
    sport_interest = sport_interest.reset_index()
    sport_interest.columns = ["region", "interest"]
    sport_interest["norm_interest"] = round((sport_interest["interest"]-sport_interest["interest"].min())/(sport_interest["interest"].max()-sport_interest["interest"].min()),2)
    return sport_interest

def get_activity_info(spark):
    mongo_params = config(section="mongo")
    mongo_client = pymongo.MongoClient("mongodb+srv://{}:{}@·{}/?retryWrites=true&w=majority".format(mongo_params["user"], mongo_params["password"], mongo_params["host"]))

    steps = mongo_client.Cluster0.activities.find({"type":"steps"})
    pushups = mongo_client.Cluster0.activities.find({"type":"pushups"})
    squats = mongo_client.Cluster0.activities.find({"type":"squats"})
    facilities = mongo_client.Cluster0.facilities.find()

    new_fac = [] 
    for recv in facilities:
        ids = recv["_id"]
        accesses = recv["accesses"]
        code = recv["city_code"]
        fac = recv["id"]
        region = recv["region"]
        timestamp = recv["timestamp"]
        typology = recv["typology"]
        new_fac.append((ids, accesses, code, fac, region, timestamp, typology))
    df_facilities = pd.DataFrame(new_fac, columns=["id", "accesses", "city", "fac", "region", "timestamp", "typology"])
    facilitySchema = StructType([StructField("id", StringType(), True),\
                                StructField("accesses", IntegerType(), True),\
                                StructField("city", IntegerType(), True),\
                                StructField("fac", IntegerType(), True),\
                                StructField("region", StringType(), True),\
                                StructField("timestamp", FloatType(), True),\
                                StructField("typology", FloatType(), True)])
    facilities = spark.createDataFrame(df_steps,schema=facilitySchema)
    norm_fac = facilities.groupBy("region").sum("accesses").withColumnRenamed("sum(accesses)", "total_accesses").orderBy("region")

    new_steps = [] 
    for recv in steps:
        ids = recv["_id"]
        code = recv["code"]
        timestamp = recv["timestamp"]
        region = recv["region"]
        for tmp in recv["activity"]:
            device = tmp["device"]
            quantity = tmp["quantity"]
            new_steps.append((ids, code, timestamp, region, device, quantity))
    new_squats = [] 
    for recv in squats:
        ids = recv["_id"]
        code = recv["code"]
        timestamp = recv["timestamp"]
        region = recv["region"]
        for tmp in recv["activity"]:
            device = tmp["device"]
            quantity = tmp["quantity"]
            new_squats.append((ids, code, timestamp, region, device, quantity))
    new_pushups = [] 
    for recv in pushups:
        ids = recv["_id"]
        code = recv["code"]
        timestamp = recv["timestamp"]
        region = recv["region"]
        for tmp in recv["activity"]:
            device = tmp["device"]
            quantity = tmp["quantity"]
            new_pushups.append((ids, code, timestamp, region, device, quantity))

    df_steps = pd.DataFrame(new_steps, columns=["id","city", "timestamp", "region", "device", "quantity"])
    df_squats = pd.DataFrame(new_squats, columns=["id","city", "timestamp", "region", "device", "quantity"])
    df_pushups = pd.DataFrame(new_pushups, columns=["id","city", "timestamp", "region", "device", "quantity"])

    #Create User defined Custom Schema using StructType
    activitySchema = StructType([StructField("id", StringType(), True),\
                        StructField("city", IntegerType(), True),\
                        StructField("type", StringType(), True),\
                        StructField("region", StringType(), True),\
                        StructField("device", StringType(), True),\
                        StructField("quantity", IntegerType(), True)])

    
    #Create DataFrame by changing schema
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")
    spark_steps = spark.createDataFrame(df_steps,schema=activitySchema)
    spark_squats = spark.createDataFrame(df_squats,schema=activitySchema)
    spark_pushups = spark.createDataFrame(df_pushups,schema=activitySchema)
    norm_steps = spark_steps.groupBy("region").sum("quantity").withColumnRenamed("sum(quantity)", "total_steps").orderBy("region")
    norm_squats = spark_squats.groupBy("region").sum("quantity").withColumnRenamed("sum(quantity)", "total_squats").orderBy("region")
    norm_pushups = spark_pushups.groupBy("region").sum("quantity").withColumnRenamed("sum(quantity)", "total_pushups").orderBy("region")

    norm_df = norm_fac.join(norm_steps, norm_fac.region == norm_steps.region)\
                        .join(norm_squats, norm_fac.region == norm_squats.region)\
                        .join(norm_pushups, norm_fac.region == norm_pushups.region)
    columns_to_scale = ["total_accesses", "total_steps","total_squats","total_pushups"]
    from pyspark.ml.feature import MinMaxScaler, VectorAssembler
    from pyspark.ml import Pipeline
    assembler = [VectorAssembler(inputCols=[col], outputCol=col+"_vec") for col in columns_to_scale]
    scaler = [MinMaxScaler(inputCol=col+"_vec", outputCol="norm_"+col) for col in columns_to_scale]
    pipeline = Pipeline(stages=[assembler, scaler])
    scaler_model = pipeline.fit(norm_df)
    scaled_data = scaler_model.transform(norm_df)
    columns_to_drop = [col+"_vec" for col in columns_to_scale]
    scaled_data = scaled_data.drop(columns_to_drop)
    return scaled_data, timestamp

def write_on_mongo(sport_index, timestamp):
    mongo_params = config(section="mongo")
    mongo_client = pymongo.MongoClient("mongodb+srv://{}:{}@·{}/?retryWrites=true&w=majority".format(mongo_params["user"], 
                                                                                                     mongo_params["password"], 
                                                                                                     mongo_params["host"]))
    db = mongo_client["Cluster0"]
    collection = db["results"]
    sport_index_json = dict()
    sport_index_json["timestamp"] = timestamp
    tmp_list = [{x["region"]:x["mean"]} for x in sport_index]
    sport_index_json["result"] = tmp_list
    sport_index_json = json.dumps(sport_index_json).encode('utf8')
    result = collection.insert_one(sport_index_json)
    
    
def main():
    fac_per_pop = get_facilities_per_pop()
    trends = get_trends()
    sport_index_features = fac_per_pop.join(trends, rsuffix="_trends").drop("region_trends", axis=1)
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("BDT") \
        .getOrCreate()
    tmpSchema = StructType([StructField("region", StringType(), True),\
                                StructField("ratio", FloatType(), True),\
                                StructField("norm_ratio", FloatType(), True),\
                                StructField("interest", IntegerType(), True),\
                                StructField("norm_interest", FloatType(), True)])
    sport_index_df = spark.createDataFrame(sport_index_features, tmpSchema)
    activities_score, timestamp = get_activity_info(spark)
    sport_index_df = sport_index_df.join(activities_score, sport_index_df.region == activities_score.region)
    df1=sport_index_df.select(col("region"), ((col("norm_ratio") + col("norm_interest")+col("norm_total_accesses")+col("norm_total_steps")+col("norm_total_squats")+col("norm_total_pushups")) / lit(6)).alias("mean"))
    write_on_mongo(df1, timestamp)
    
    
if __name__ == '__main__':
    main()