# Docker container description
* start-dockers.sh creates a docker network and starts containers.
* stop-dockers.sh stops containers and removes docker network.
  ## ##
  **hdfs_config**
* start-hdfs.sh start the distributed file system and creates directory *data* for data input
  and directory *output* for query results.
    ## ##
    **hdfs_nifi**
* core-site.xml and core-site.xml the xml files that contain configurations to implement local
  host machine persistence 
  
  To view nifi template connect to http://localhost:9880/nifi/ and upload nifi-template.xml
  ## ##
  In order to launch the application it is necessary to insert *127.0.0.1 hbase* in directory etc/hosts 
     