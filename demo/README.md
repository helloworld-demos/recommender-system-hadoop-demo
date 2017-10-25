# How?

1. setup docker network

   ```shell
   $ docker network create \
       --driver=bridge \
       --subnet=172.18.0.0/16 \
       --gateway=172.18.0.1 \
       hadoop
     
   # docker network ls
   # docker network inspect hadoop
   ```

2. `./prepare.sh`

   1. generate application jar
   2. move application jar, data seed files to shared folder
   3. delete hadoop containers

3. `~/dev/hadoop-cluster-docker/start-container.sh`

4. run application on hadoop master

   `(cd src/recommender-system-demo/ && ./demo.sh)`

5. check the result:

   `hdfs dfs -cat /expected-ratings/*`

   ​


