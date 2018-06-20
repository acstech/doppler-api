# Doppler-API
An API to consume data from Kafka and send it to the frontend via websockets.
## Setup
### General
- Clone the repository
- Run `go run doppler-api/cmd/doppler-api/main.go`
### Couchbase
- [Look here for Couchbase setup](https://github.com/acstech/doppler-events#couchbase)
- Then, copy and rename the .env.defualt to .env and fill out the appropriate envirnment variable to connect to couchbase.