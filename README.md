# A big data system for the classification of physical activity level in the Italian regions

 Data about sport activity have often been connected through survey methods, which, however, have some critical issues (e.g., they are subject to social desirability bias and they do not allow daily monitoring of physical activity in a given territory). In this sense, spontaneously released data and data registered in mobile applications could represent a good alternative way to have more detailed information about physical activity of the people. For this reason, in this work, we designed a big data system for the classification of the physical activity level in the different Italian regions on the basis of multiple data sources. 
 
# Repository description

**IMPLEMENTATION**
The code written to build this data architecture is accessible on a GitHub repository5. It is composed of a set of scripts that must be executed in separated shells:

-The simulators (simulator_activity.py, simulator_facilities.py), respectively used to simulate the intensity of physical activity and the accesses to the gyms

-The consumer script (consumer.py), which starts the phase of ingestion and message queuing (with MongoDB and Kafka)

-The Spark script (sport_index_computation.py), which starts the processing phase. This script allows to compute the Sportiness Index and to store the clean results of the processing phase (mean activities per region, sports facilities per region, people going to the facilities per region, Google searches per region, Sportiness Index per region) in a NoSQL Database, with MongoDB. The data, which are daily updating, are requirable through a front-end query to the MongoDB data.


**FOLDERS**

**/data**

All the data used for the pipeline

*DCCV_AVQ_PERSONE_07062022160135090.csv* - ISTAT data: used to simulate a realistic ingestion of mobile data about physical activity of people
