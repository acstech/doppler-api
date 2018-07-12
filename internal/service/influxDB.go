package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

type InfluxService struct {
	client       influx.Client
	truncateSize int
}

type ajaxQuery struct {
	clientID  string
	events    []string
	startTime int64
	duration  time.Duration
}

type batch struct {
	batchMap map[string]Latlng
}

// AjaxRsp is the struct for sending responses
type AjaxRsp struct {
	Response string `json:"Response"`
}

// NewInfluxService ...
func NewInfluxService(client influx.Client, tSize int) *InfluxService {
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
	events := requestQuery["filters[]"] // get filters (.Query adds the "[]" to the key name)

	// get start time
	tmp, err := strconv.ParseInt(requestQuery["startTime"][0], 10, 64)
	if err != nil {
		fmt.Println("Start Time Parse Error: ", err)
	}
	startTime := (tmp / 1000) - (86400 * 2) // parse time string to RFC3339 for influx

	// get duration
	ms, err := strconv.Atoi(requestQuery["duration"][0]) // get duration (index zero since only have one ID), convert string of milliseconds to int
	duration := time.Duration(ms)                        // convert int to duration
	if err != nil {
		fmt.Println("Duration Parse Error: ", err)
	}

	// create influxQuery instance based on r's URL query
	ajaxQ := &ajaxQuery{
		clientID:  clientID,
		events:    events,
		startTime: startTime,
		duration:  duration,
	}
	fmt.Println("Query: ", ajaxQ)

	// create query string
	fmt.Println("Start Time: ", startTime)
	q := fmt.Sprintf("SELECT lat,lng FROM dopplerDataHistory WHERE time >= %d AND clientID='%s' AND eventID =~ /(?:%s)/", ajaxQ.startTime, ajaxQ.clientID, strings.Join(ajaxQ.events, "|"))

	// getPoints
	// _, err = infxHelper.GetPoints(c.client, q)
	if err != nil {
		fmt.Println("Influx Query Error: ", err)
	}
	fmt.Println("Query: ", q)
	// fmt.Println("influx response: ", influxRsp)

	// bucket

	// Test response
	msg := AjaxRsp{
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
