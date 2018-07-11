## Doppler-Heatmap

This repository contains the second API layer supporting Doppler-Heatmap.

Doppler-API consumes data off Kafka served by Doppler-events and sends this data over a web connection in JSON format to be displayed by a front end client. Doppler-frontend is available to display geographic data in heat-map format [here](https://github.com/acstech/doppler-frontend).

#### Tools
* API-level language: Golang (1.10.2)
* Historical data storage: InfluxDB (1.4.3)
* Client and event identification data: Couchbase (Enterprise edition 5.1.1)


### Prerequisites

Doppler-api depends on [Doppler-Events](https://github.com/acstech/doppler-events) to serve its purpose. [Doppler-frontend](https://github.com/acstech/doppler-frontend) is necessary to display the data. Make sure both setup is complete for these repositories also.
[See these instructions](https://github.com/acstech/doppler-events#Setup)

### Setup

Clone this repository.

Follow all the steps listed [here](https://github.com/acstech/doppler-events#Setup)


### Testing

Run `docker build . -t acst/doppler-api:latest`

Then go to doppler-events/ and `run docker-compose up -d`

_Note: All the docker images (doppler-events, doppler-api, doppler-frontend) must have been built at least once in order for docker-compose up -d to work._

To receive messages and send over a web-socket connection, doppler-events and doppler-frontend must be running.

## Contributors

* [Ben Wornom](https://github.com/bwornom7)
* [Leander Stevano](https://github.com/deepmicrobe)
* [Matt Smith](https://github.com/mattsmith803)
* [Matt Harrington](https://github.com/Matt2Harrington)
* [Pranav Minasandram](https://github.com/PranavMin)
* [Peter Kaufman](https://github.com/pjkaufman)
