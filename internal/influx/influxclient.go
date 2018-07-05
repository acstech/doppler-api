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

//GetPoints recieves a query string and returns the results from the influx client and returns the results
func GetPoints(clnt client.Client, query string) (res Responses, err error) {

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
