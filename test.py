import multiprocessing as mp
import osmnx as ox
from time import time
import concurrent.futures

def get_amenities(territory):
    amenity_tags = {
        'amenity':[
            'dive_centre',
            'bicycle_parking',
            'bicycle_rental',
        ]
    }
    start_time = time()
    print("Doing amenities...")
    amenities = ox.geometries_from_place(territory, tags=amenity_tags)
    print(f"Amenities done in {time()-start_time}s!")
    print(f"Amenities results => {amenities.shape}")

def get_buildings(territory):
    buildings_tags = {
        'buildings':[
            'grandstand',
            'pavillion',
            'riding_hall',
            'sports_hall',
            'stadium'
        ]
    }
    start_time = time()
    print("Doing buildings...")
    buildings = ox.geometries_from_place(territory, tags=buildings_tags)
    print(f"Buildings done in {time()-start_time}s!")
    
    print(f"Buildings results => {buildings.shape}")

def get_highways(territory):
    highway_tags = {
        'highway':[
            'path',
            'footway',
            'cycleway',
            'bridleway',
            'track',
        ]
    }
    start_time = time()
    print("Doing highways...")
    highways = ox.geometries_from_place(territory, tags=highway_tags)
    print(f"Higways done in {time()-start_time}s!")
    
    print(f"Highways results => {highways.shape}")

def get_leisures(territory):
    leisure_tags = {
        'leisure':[
            'fitness_centre',
            'sport_centre',
            'dance',
            'disc_golf_course',
            'fitness_station',
            'horse_riding',
            'ice_rink',
            'pitch',
            'stadium',
            'swimming_area',
            'swimming_pool',
            'track'
        ]
    }
    start_time = time()
    print("Doing leisures...")
    leisures = ox.geometries_from_place(territory, tags=leisure_tags)
    print(f"Leisures done in {time()-start_time}s!")
    
    print(f"Leisures results => {leisures.shape}")
    
def get_routes(territory):
    route_tags = {
        'route':[
            'bicycle',
            'canoe',
            'hiking',
            'inline_skates',
            'mtb',
            'piste',
            'running',
        ]
    }
    start_time = time()
    print("Doing routes...")
    routes = ox.geometries_from_place(territory, tags=route_tags)
    print(f"Routes done in {time()-start_time}s!")
    
    print(f"Routes results => {routes.shape}")

def exec_func(arguments):
    f,arg = arguments
    f(arg)

if __name__ == '__main__':
    italy_region = ["Valle D'Aosta", "Piemonte", "Liguria", "Lombardia", "Veneto", "Friuli-Venezia Giulia", "Trentino-Alto Adige", "Emilia-Romagna", "Toscana", "Lazio", "Umbria", "Marche", "Abruzzo", "Molise", "Campania", "Puglia", "Basilicata", "Calabria", "Sardegna", "Sicilia"]
    for region in italy_region:
        print(f"Starting {region}")
        list_func = [get_amenities, get_buildings, get_highways, get_leisures, get_routes]
        with concurrent.futures.ProcessPoolExecutor(len(list_func)) as pool:
            results = list(pool.map(exec_func, zip(list_func, [region for _ in range(len(list_func))])))
            print(results)