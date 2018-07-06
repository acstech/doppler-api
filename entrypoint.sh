#!/bin/sh
echo "Making sure kafka and couchbase are up"
<<<<<<< HEAD
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
=======
while ! nc -z couchbase 8091 || ! nc -z kafka 9092 ;
>>>>>>> bd5b7114c3c5f6c03617c8aaa788883aa77c5152
do
    echo sleeping;
    sleep 1;
done;
<<<<<<< HEAD
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
=======
echo "Kafka and Couchbase are up"
>>>>>>> bd5b7114c3c5f6c03617c8aaa788883aa77c5152
# safe to run the service
/opt/service/frontendAPI
    