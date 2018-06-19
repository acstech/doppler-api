package main

import (
	"fmt"

	cb "github.com/acstech/doppler-api/internal/couchbase"
	//"github.com/influxdata/influxdb/client/v2"
)

func filterByEvent(eventid string) error {
	return nil
}

func main() {
	//var clnt client.Client
	//Consume()
	cbConn := &cb.Couchbase{Doc: &cb.Doc{}}
	cbConn.ConnectToCB("couchbase://validator:rotadilav@localhost/doppler1")
	fmt.Println("Created the db connection.")
	if !cbConn.ClientExists("test2") {
		fmt.Print("error")
	}
	//ensure that the eventID exists
	//cbConn.EventEnsure("test2", "")
	fmt.Println("Client exists and the eventID has been ensured.")
}
