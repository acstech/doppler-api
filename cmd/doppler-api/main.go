package main

import (
	"fmt"
	"os"

	fx "github.com/acstech/doppler-api/internal/influx"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/joho/godotenv"

	"github.com/acstech/doppler-api/internal/liveupdate"
)

func main() {

	//get CB config values from .env file
	err := godotenv.Load()
	if err != nil {
		panic(fmt.Errorf("error loading .env file: %v", err))
	}
	cbCon := os.Getenv("COUCHBASE_CONN")

	// creates influx client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: "root",
		Password: "root",
	})
	if err != nil {
		panic(fmt.Errorf("error connecting to influx: %v", err))
	}
	defer c.Close()

	// queries influx for all points, query is temporarily hard coded
	influx := false
	if influx == true {
		res, err := fx.GetPoints(c, "select * from dopplerDataHistory")
		if err != nil {
			fmt.Println(err)
		} else {
			i := 0
			for i < len(res.ValArray) {
				fmt.Println(res.ValArray[i])
				i = i + 1
			}
		}
	}

	fmt.Println("Client exists and the eventID has been ensured.")

	//intialize websocket management and kafka consume
	go liveupdate.InitWebsockets(cbCon)
	err = liveupdate.Consume()
	if err != nil {
		panic(fmt.Errorf("error connecting to kafka: %v", err))
	}
}
