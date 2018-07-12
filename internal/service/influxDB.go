package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
)

type InfluxService struct {
	client       client.Client
	truncateSize int
}

type influxQuery struct {
	clientID  string
	events    []string
	startTime int
	duration  time.Duration
}

type batch struct {
	batchMap map[string]Latlng
}
type InfluxMsg struct {
	Response string `json:"Response"`
}

// NewInfluxService ...
func NewInfluxService(client client.Client, tSize int) *InfluxService {
	return &InfluxService{
		client:       client,
		truncateSize: tSize,
	}
}

func (c *InfluxService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// get request query
	fmt.Println("Got Request: ", r)
	requestQuery := r.URL.Query()

	// get query values
	// get clientID
	clientID := requestQuery["clientID"][0] // get clientID (index zero since only have one ID)

	// get list of events
	events := requestQuery["filters"] // used map directly since need []string

	// get start time
	startTime, err := strconv.Atoi(requestQuery["startTime"][0]) // get startTime (index zero since only have one ID), convert string of time to int
	if err != nil {
		fmt.Println("Start Time Parse Error: ", err)
	}
	// get duration
	ms, err := strconv.Atoi(requestQuery["duration"][0]) // get duration (index zero since only have one ID), convert string of milliseconds to int
	duration := time.Duration(ms)                        // convert int to duration
	if err != nil {
		fmt.Println("Duration Parse Error: ", err)
	}

	// create influxQuery instance based on r's URL query
	q := &influxQuery{
		clientID:  clientID,
		events:    events,
		startTime: startTime,
		duration:  duration,
	}
	fmt.Println("Query: ", q)

	// create query string

	// getPoints

	// bucket

	// Test response
	msg := InfluxMsg{
		Response: "Hello, got your request",
	}
	response, err := json.Marshal(msg)
	if err != nil {
		fmt.Println(err)
	}

	w.Header().Set("access-control-allow-methods", "GET")
	w.Header().Set("access-control-allow-origin", "*")
	w.Write(response)
}
