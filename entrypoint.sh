#!/bin/sh
echo "Making sure kafka and couchbase are up"
<<<<<<< HEAD
while ! nc -z couchbase 8091 || ! nc -z kafka 9092 ;
=======
while [ ! nc -z kafka 9092 ] && [ ! nc -z couchbase 8091 ];
>>>>>>> Updated environment variables and added docker files
do
    echo sleeping;
    sleep 1;
done;
<<<<<<< HEAD
echo "Kafka and Couchbase are up"
=======
>>>>>>> Updated environment variables and added docker files
# safe to run the service
/opt/service/frontendAPI
    