package main

import (
	"fmt"
	"os"

	cb "github.com/acstech/doppler-api/internal/couchbase"
	fx "github.com/acstech/doppler-api/internal/influx"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
		panic(err)
	}
	cbCon := os.Getenv("COUCHBASE_CONN")
	//var clnt client.Client
	//Consume()
	cbConn := &cb.Couchbase{Doc: &cb.Doc{}}
	cbConn.ConnectToCB(cbCon)
	fmt.Println("Created the db connection.")
	if !cbConn.ClientExists("test2") {
		fmt.Print("error")
	}
	//ensure that the eventID exists
	//cbConn.EventEnsure("test2", "")

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: "root",
		Password: "root",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer c.Close()

	fmt.Println("Client exists and the eventID has been ensured.")
	//res, err := GetPoints(c, "select * from dopplerDataHistory")
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

	fmt.Println("Client exists and the eventID has been ensured.")
}
