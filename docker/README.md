# Docker container description
* start-dockers.sh create a docker network and starts containers.
* stop-dockers.sh stop containers and remove docker network.
  ## ##
  **hdfs_config**
* start-hdfs.sh start the distributed file system and create directory *data* for data input
  and *output* for query results.
    ## ##
    **hdfs_nifi**
* core-site.xml and core-site.xml the xml file that contains configurations for implement local
  host machine persistence 
  
  To view nifi template connect to http://localhost:9880/nifi/ and upload nifi-template.xml   