package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
)

// InfluxService is the instance of an influxDB query service
type InfluxService struct {
	client              influx.Client // connection to InfluxDB
	defaultTruncateSize int           // truncation size used in bucketing
	db                  string        // the name of the influx database (for doppler-api: "doppler")
	measure             string        // the measure in InfluxDB used by doppler-api
}

// request is the structure for the query's received from an ajax GET request from doppler-frontend
type request struct {
	clientID  string   // clientID of ajax request
	events    []string // slice of event filters
	startTime int      // Unix start time
	endTime   int      // Unix end time
	index     string   // the index of the ajax request

	truncateSize int    // int uesd to determine how much points are truncated during bucketing
	zeroTest     string // string used to compare to handle truncation edge case
}

// response is the structure for the reponse to a ajax request
type response struct {
	Index string            // represents the index of the ajax call
	Batch map[string]Latlng // the batch of points for the ajax call
}

// NewInfluxService creates an instance of an influxDB query service
func NewInfluxService(client influx.Client, tSize int) *InfluxService {
	return &InfluxService{
		client:              client,
		defaultTruncateSize: tSize,
		db:                  "doppler",
		measure:             "dopplerDataHistory",
	}
}

// ServeHTTP handles AJAX GET Requests from doppler-frontend
func (c *InfluxService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// get request query
	requestQuery := r.URL.Query()

	// create new request based on ajax request values
	request, err := c.newRequest(requestQuery)
	if err != nil {
		log.Println("New Request Error: ", err)
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			log.Println("Write AJAX Response Error Error: ", err)
		}
		return
	}

	// query InfluxDB
	influxData, err := c.queryInfluxDB(request)
	if err != nil {
		log.Println("Query InfluxDB Error: ", err)
	}

	// bucket
	batchMap := request.influxBucketPoints(influxData)

	// create response
	res := response{
		Index: request.index,
		Batch: batchMap,
	}

	// marshal the response for sending
	response, err := json.Marshal(res)
	if err != nil {
		log.Println("Marshal Response Error: ", err)
	}

	// set ajax response headers
	w.Header().Set("access-control-allow-methods", "GET")
	w.Header().Set("access-control-allow-origin", "*")

	// write response data
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(response)
	if err != nil {
		log.Println("Write AJAX Response Error: ", err)
	}
}

// newRequest creates an instance of a request based on the values from an AJAX GET request
func (c *InfluxService) newRequest(requestQuery url.Values) (*request, error) {
	// Parse Query Values
	// get clientID
	// check if request contains clientID
	_, contains := requestQuery["clientID"]
	if !contains {
		err := errors.New("Request Missing Client ID")
		return &request{}, err
	}
	// get clientID
	clientID := requestQuery["clientID"][0] // get clientID (index zero since only have one ID)

	// get list of events
	// check if request contains filters
	_, contains = requestQuery["filters[]"]
	if !contains {
		err := errors.New("Request Missing Filters")
		return &request{}, err
	}
	// get filters
	events := requestQuery["filters[]"] // get filters (.Query adds the "[]" to the key name)

	// get start time
	// check if request contains startTime
	_, contains = requestQuery["startTime"]
	if !contains {
		err := errors.New("Request Missing Start Time")
		return &request{}, err
	}
	// get startTime
	stringSec := requestQuery["startTime"][0] // get startTime as string of seconds (index zero since only have one ID)
	secs, err := strconv.Atoi(stringSec)      // convert string to int
	if err != nil {
		log.Println("Parse Start Time Error: ", err)
	}
	startTime := secs / 1000000000 // convert seconds to nanoseconds

	// get end time
	// check if request contains end time
	_, contains = requestQuery["endTime"]
	if !contains {
		err = errors.New("Request Missing End Time")
		return &request{}, err
	}
	// get endTime
	stringSec = requestQuery["endTime"][0] // get endTime as string of seconds (index zero since only have one ID)
	secs, err = strconv.Atoi(stringSec)    // convert string to int
	if err != nil {
		log.Println("Parse Start Time Error: ", err)
	}
	endTime := secs / 1000000000 // convert seconds to nanoseconds

	// get the ajax index
	// check if request contains index
	_, contains = requestQuery["index"]
	if !contains {
		err := errors.New("Request Missing Index")
		return &request{}, err
	}
	// // get index
	index := requestQuery["index"][0]

	// create zero test for bucketing
	zTest := createZeroTest(c.defaultTruncateSize)

	// create request instance
	return &request{
		clientID:  clientID,
		events:    events,
		startTime: startTime,
		endTime:   endTime,
		index:     index,

		zeroTest:     zTest,
		truncateSize: c.defaultTruncateSize,
	}, nil
}

// queryInfluxDB takes a request, creates a query string, queries InfluxDB, and returns the influx response
func (c *InfluxService) queryInfluxDB(request *request) ([]Point, error) {
	// create query string

	// simipler but less secure query creation
	// q := fmt.Sprintf("SELECT lat,lng FROM dopplerDataHistory WHERE time >= %s AND time <= %s AND clientID='%s' AND eventID =~ /(?:%s)/", request.startTime, request.endTime, request.clientID, strings.Join(request.events, "|"))

	// initialize query string without events
	q := "SELECT lat,lng FROM dopplerDataHistory WHERE time >= $startTime AND time <= $endTime AND clientID = $clientID AND ("

	// initialize parameters without events
	parameters := map[string]interface{}{
		"startTime": request.startTime, // time as int
		"endTime":   request.endTime,   // time as int
		"clientID":  request.clientID,  // clientID as string
	}

	// add eventID statements to query string
	// append eventID = $event[index] to query string and add events to parameters (regex wasnt working with binding parameters)
	for index, value := range request.events {
		// check if on last value in events map
		if index+1 == len(request.events) {
			eventID := fmt.Sprint("event", index)           // create a unique event parameter
			q = q + fmt.Sprint("eventID = $", eventID, ")") // concat eventID with placeholder and closing )
			parameters[eventID] = value                     // add eventID as string to parameters
			continue                                        // skip rest of loop
		}

		eventID := fmt.Sprint("event", index)              // create a unique event parameter
		q = q + fmt.Sprint("eventID = $", eventID, " OR ") // concat eventID with placeholder
		parameters[eventID] = value                        // add eventID as string to parameters
	}

	// create influxDB query
	query := influx.NewQueryWithParameters(q, c.db, "ns", parameters)

	// get points from influxDB
	response, err := c.getPoints(query)
	if err != nil {
		log.Println("Influx Query Error: ", err)
	}
	return response, nil
}

// getPoints takes an influx.Query, queries influx, creates a slice of Points based on influx data, and returns the results
func (c *InfluxService) getPoints(query influx.Query) ([]Point, error) {
	// initialize slice of Points (return variable)
	var points []Point

	// Run query
	// check if get response from InfluxDB
	if response, err := c.client.Query(query); err == nil && response.Error() == nil {
		// check if have any results
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
		log.Println("Query Error: ", err)
		log.Println("Influx Response: ", response)
		return nil, err
	}
	return points, nil
}

// influxBucketPoints takes a slice of Points, truncates each point, creates new buckets of Latlng when needed or increases the count within a Latlng when needed, returns the result
func (request *request) influxBucketPoints(data []Point) map[string]Latlng {

	// initialize map with hash as key and Latlng (a Point with a count) as value
	batchMap := make(map[string]Latlng)

	// iterate through data
	for i := range data {
		// Truncate each item in batch
		// Split float by decimal
		latSlice := strings.SplitAfter(data[i].Lat, ".")
		lngSlice := strings.SplitAfter(data[i].Lng, ".")

		// Truncate second half of slices
		latSlice[1] = truncate(latSlice[1], request.truncateSize)
		lngSlice[1] = truncate(lngSlice[1], request.truncateSize)

		//check for truncating edge case
		if strings.Contains(latSlice[0], "-0.") {
			latSlice = checkZero(latSlice, request.zeroTest)
		}
		if strings.Contains(lngSlice[0], "-0.") {
			lngSlice = checkZero(lngSlice, request.zeroTest)
		}

		// Combine the split strings together
		lat := strings.Join(latSlice, "")
		lng := strings.Join(lngSlice, "")

		//create bucket hash
		bucket := lat + ":" + lng

		//create Latlng (a point with a count)
		pt := Latlng{
			Coords: Point{
				Lat: lat,
				Lng: lng,
			},
			Count: 1,
		}

		// Bucketing
		// check if bucket exists
		// if it does exists, increase the count
		_, contains := batchMap[bucket]
		if contains {
			value := batchMap[bucket] //get the value of the bucket

			value.Count++ //increase the count

			batchMap[bucket] = value //add the new count to the point

		} else { //otherwise, add the point with the count
			batchMap[bucket] = pt
		}
	}
	return batchMap
}

// trucate takes a string and changes its length based on truncateSize
func truncate(s string, tSize int) string {
	if len(s) < tSize {
		//padding if smaller
		for i := len(s); i < tSize; i++ {
			s += "0"
		}
		return s
	}
	//truncate
	return s[0:tSize]
}

// checkZero determines if a "-0." edge case needs to remove the "-" and does so if necessary
func checkZero(coord []string, zeroTest string) []string {
	//compare the decimals of the "-0." case to the zeroTest
	//if they are equal, remove the "-"
	if strings.Compare(coord[1], zeroTest) == 0 {
		coord[0] = "0."
		return coord
	}
	return coord
}
