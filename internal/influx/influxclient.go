package influx

import (
	"github.com/influxdata/influxdb/client/v2"
	//"github.com/influxdata/influxdb/client/v2"
)

func filterByEvent(eventid string) error {
	return nil
}

const (
	DB      = "doppler"
	Measure = "dopplerDataHistory"
)

type InfluxResponse struct {
	values []interface{}
}

type Responses struct {
	ValArray []InfluxResponse
}

/*
func GetPoints(clnt client.Client, query string) (res []client.Result, err error) {
	q := client.NewQuery(string, DB, "ms")
  if response, err := clnt.Query(q); err == nil && response.Error() == nil {
		fmt.Println(response.Results)
	}
	return res, nil
}*/

func GetPoints(clnt client.Client, query string) (res Responses, err error) {
	//var clnt client.Client
	//Consume()
	/*cbConn := &cb.Couchbase{Doc: &cb.Doc{}}
	cbConn.ConnectToCB("couchbase://validator:rotadilav@localhost/doppler1")
	fmt.Println("Created the db connection.")
	if !cbConn.ClientExists("test2") {
		fmt.Print("error")
	}
	//ensure that the eventID exists
	//cbConn.EventEnsure("test2", "")*/

	q := client.NewQuery(query, DB, "s")
	if response, err := clnt.Query(q); err == nil && response.Error() == nil {
		//fmt.Println(response.Results)

		for r := range response.Results {
			var ifx InfluxResponse
			ifx.values = response.Results[r].Series[r].Values[r]
			res.ValArray = append(res.ValArray, ifx)
		}
	} else {
		return res, err
	}

	return res, nil
}
