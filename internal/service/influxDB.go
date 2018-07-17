package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	infxHelper "github.com/acstech/doppler-api/internal/influx"
	influx "github.com/influxdata/influxdb/client/v2"
)

// InfluxService is the instance of an influxDB query service
type InfluxService struct {
	client              influx.Client // connection to InfluxDB
	defaultTruncateSize int           // truncation size used in bucketing
}

// request is the structure for the query's received from an ajax GET request from doppler-frontend
type request struct {
	clientID  string   // clientID of ajax request
	events    []string // slice of event filters
	startTime string   // Unix start time
	endTime   string   // Unix end time
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
	}
}

// ServeHTTP handles AJAX GET Requests from doppler-frontend
func (c *InfluxService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// get request query
	fmt.Println("Got Request: ", r.Host)
	requestQuery := r.URL.Query()

	// get query values
	// intialize query variables
	var clientID string
	var events []string
	var startTime string
	var endTime string
	var index string

	// get clientID
	// check if request contains clientID
	_, contains := requestQuery["clientID"]
	if !contains {
		fmt.Println("Request Missing Client ID")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Client ID"))
		return
	}
	// get clientID
	clientID = requestQuery["clientID"][0] // get clientID (index zero since only have one ID)

	// get list of events
	// check if request contains filters
	_, contains = requestQuery["filters[]"]
	if !contains {
		fmt.Println("Request Missing Filters")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Filters"))
		return
	}
	// get filters
	events = requestQuery["filters[]"] // get filters (.Query adds the "[]" to the key name)

	// get start time
	// check if request contains startTime
	_, contains = requestQuery["startTime"]
	if !contains {
		fmt.Println("Request Missing Start Time")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Start Time"))
		return
	}
	// get startTime
	startTime = requestQuery["startTime"][0] // get startTime (index zero since only have one ID)

	// get end time
	// check if request contains end time
	_, contains = requestQuery["endTime"]
	if !contains {
		fmt.Println("Request Missing End Time")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing End Time"))
		return
	}
	// get endTime
	endTime = requestQuery["endTime"][0] // get endTime (index zero since only have one ID)

	// get the ajax index
	// check if request contains index
	_, contains = requestQuery["index"]
	if !contains {
		fmt.Println("Request Missing Index")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing Index"))
		return
	}
	// get index
	index = requestQuery["index"][0]

	// create zero test for bucketing
	zTest := createZeroTest(c.defaultTruncateSize)

	// create influxQuery instance based on r's URL query
	request := &request{
		clientID:  clientID,
		events:    events,
		startTime: startTime,
		endTime:   endTime,
		index:     index,

		zeroTest:     zTest,
		truncateSize: c.defaultTruncateSize,
	}

	// query InfluxDB
	influxData, err := request.queryInfluxDB(c)
	if err != nil {
		fmt.Println("Query InfluxDB Error: ", err)
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
		fmt.Println(err)
	}

	// set ajax response headers
	w.Header().Set("access-control-allow-methods", "GET")
	w.Header().Set("access-control-allow-origin", "*")

	// write response data
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// queryInfluxDB takes an InfluxService and an ajaxQuery, creates a query string, queries InfluxDB, parses query response
// and returns the results
func (request *request) queryInfluxDB(c *InfluxService) ([]infxHelper.Point, error) {
	// create query string
	q := fmt.Sprintf("SELECT lat,lng FROM dopplerDataHistory WHERE time >= %s AND time <= %s AND clientID='%s' AND eventID =~ /(?:%s)/", request.startTime, request.endTime, request.clientID, strings.Join(request.events, "|"))

	// getPoints
	response, err := infxHelper.GetPoints(c.client, q)
	if err != nil {
		fmt.Println("Influx Query Error: ", err)
	}
	return response, nil
}

func (request *request) influxBucketPoints(data []infxHelper.Point) map[string]Latlng {

	batchMap := make(map[string]Latlng)

	for i := range data {
		if len(batchMap) < 8000 {
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

			//create point
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
		} else {
			break
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
