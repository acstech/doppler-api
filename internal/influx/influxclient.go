package influx

import (
	"fmt"

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

type Point struct {
	Lat string `json:"lat"`
	Lng string `json:"lng"`
}

//GetPoints recieves a query string and returns the results from the influx client and returns the results
func GetPoints(clnt client.Client, query string) ([]Point, error) {
	// Create the query
	q := client.NewQuery(query, DB, "ns")
	// Run query
	var points []Point
	if response, err := clnt.Query(q); err == nil && response.Error() == nil {
		if len(response.Results[0].Series) != 0 {
			// Get lat and lng values from response
			data := response.Results[0].Series[0].Values
			// Set length of points
			points = make([]Point, len(data))
			// Iterate through response to add values to points
			for key := range data {
				points[key] = Point{
					Lat: fmt.Sprint(data[key][1]),
					Lng: fmt.Sprint(data[key][2]),
				}
			}
		}
	} else {
		fmt.Println("Query Error: ", err)
		return nil, err
	}
	return points, nil
}
