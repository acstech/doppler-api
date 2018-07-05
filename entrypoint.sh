#!/bin/sh
echo "Making sure kafka and couchbase are up"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
while ! nc -z couchbase 8091 || ! nc -z kafka 9092 ;
=======
while [ ! nc -z kafka 9092 ] && [ ! nc -z couchbase 8091 ];
>>>>>>> Updated environment variables and added docker files
=======
while [ ! nc -z kafka 9092 ] && [ ! nc -z couchbase 8091 ];
>>>>>>> Updated environment variables and added docker files
=======
while ! nc -z couchbase 8091 || ! nc -z kafka 9092 ;
>>>>>>> updated docker and readme
do
    echo sleeping;
    sleep 1;
done;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
echo "Kafka and Couchbase are up"
=======
>>>>>>> Updated environment variables and added docker files
=======
>>>>>>> Updated environment variables and added docker files
=======
echo "Kafka and Couchbase are up"
>>>>>>> updated docker and readme
# safe to run the service
/opt/service/frontendAPI
    