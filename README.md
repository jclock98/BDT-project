# A big data system for the classification of physical activity level in the Italian regions

Luigi Arminio - 215248

Jacopo Clocchiatti - 229701

Course: Big Data Technologies - UNITN (MSc in Data Science)


Data about sports activity have often been collected through survey methods, which, however, have some critical issues (e.g., they are subject to social desirability bias, and they do not allow daily monitoring of physical activity in a given territory). In this sense, data registered in mobile applications, as well as OpenStreetMap data and Google searches data, if integrated with data collected in more traditional ways, could represent a way to have more detailed information about people's physical activity level. For this reason, in this work, we projected a big data system to classify the physical activity level in the different Italian regions based on multiple data sources. The output of the system is a daily updating DataBase containing several indicators, and a composite statistical index, to monitor the physical activity level in the Italian regions.

The report is on the file **BDT_Report.pdf**
 

# IMPLEMENTATION - Execution of the pipeline
To run the pipeline, there is a set of scripts that must be executed in separated shells, in this order:

1) The simulators (*simulator_activity.py*, *simulator_facilities.py*), respectively used to simulate the intensity of physical activity and the accesses to the gyms

2) The consumer script (*consumer.py*), which starts the phase of ingestion and message queuing (with MongoDB and Kafka)

3) The Spark script (*sport_index_computation.py*), which starts the processing phase. This script allows to compute the Sportiness Index and to store the clean results of the processing phase (mean activities per region, sports facilities per region, people going to the facilities per region, Google searches per region, Sportiness Index per region) in a NoSQL Database, with MongoDB. The data, which are daily updating, are requirable through a front-end query to the MongoDB data.

*config.py* is a support file.

# Repository description

**FOLDERS**

**/data**

All the data used for the pipeline

*DCCV_AVQ_PERSONE_07062022160135090.csv* - ISTAT data: used to simulate a realistic ingestion of mobile data about physical activity of people

*italy_provincies.json* - Data about the italian provinces (initials, province name, population, number of municipalities, region ID)

*italy_geo.json* - Geodata about Italian municipalities (ISTAT code, city name, longitude, latitude)

*italy_cities.json* - Data about Italian municipalities (ISTAT code, city name, region, province, prefix, fiscal code, population)


