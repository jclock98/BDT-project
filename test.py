import osmnx as ox
from time import time
import concurrent.futures
import psycopg2 as psypg


def get_leisures(municipality):
    istat = municipality[0]
    city = municipality[1]
    province = municipality[2]
    region = municipality[3]
    leisure_tags = {
        'leisure':[
            'fitness_centre',
            'sport_centre',
            'ice_rink',
            'stadium',
            'swimming_area',
            'swimming_pool',
        ]
    }
    address = f"{city}, {province}, {region}, Italy"
    start_time = time()
    print(f"Doing leisures for {city}...")
    try:
        leisures = ox.geometries_from_place(address, tags=leisure_tags)
    except:
        return
    print(f"Leisures done in {time()-start_time}s!")
    print(f"Leisures results => {leisures.shape}")
    if leisures.shape[0] > 0:
        try:
            conn = psypg.connect(
            host=HOST,
            database=DBNAME,
            user=USER,
            password=PSWD)
            
            cur = conn.cursor()
    
            sql = "INSERT INTO places(name, typology, municipality) VALUES"
            args_str = ','.join(cur.mogrify("(%s,%s,%s)",
                                    (leisure.name if hasattr(leisure, "name") else None,
                                     leisure.leisure,
                                     istat)).decode("utf-8") 
                                for leisure in leisures.itertuples())
            cur.execute(sql+args_str)
            # commit the changes to the database
            conn.commit()
            # close communication with the database
            cur.close()
        except (Exception, psypg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

if __name__ == '__main__':
    try:
        conn = psypg.connect(
            host=HOST,
            database=DBNAME,
            user=USER,
            password=PSWD)
        
        cur = conn.cursor()
        
        sql = """SELECT istat, municipality, m.province, region 
                FROM municipalities AS m JOIN provinces AS p 
                ON m.province=p.province"""
        cur.execute(sql)
        # commit the changes to the database
        municipalities = cur.fetchall()
        # close communication with the database
        cur.close()
    except (Exception, psypg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
    with concurrent.futures.ProcessPoolExecutor(5) as pool:
        results = list(pool.map(get_leisures, municipalities))