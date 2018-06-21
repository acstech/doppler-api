# Doppler-API
An API to consume data from Kafka and send it to the frontend via websockets.
## Setup
### General
- Clone the repository
### Docker
- [Look at general setup steps for Doppler-Events and ensure the same services are running](https://github.com/acstech/doppler-events#Setup)
### Couchbase
- [Look here for Couchbase setup](https://github.com/acstech/doppler-events#couchbase)
- Then, copy and rename the .env.defualt in the root directory to .env and fill out the appropriate environment variables to connect to Couchbase. (Use "localhost" for "host" in development)

## Test
- Run `go run cmd/doppler-api/main.go`
- Then enact the these [steps](https://github.com/acstech/doppler-events#testing--not-completed-yet-)
