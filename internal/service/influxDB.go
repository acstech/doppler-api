package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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
	endTime   int
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
	// create influxQuery instance based on r

	// create query string

	// getPoints

	// bucket

	// Test request
	fmt.Println("Got Request: ", r)
	fmt.Println(r.Body)

	// Test response
	msg := InfluxMsg{
		Response: "Hello, got your request",
	}
	response, err := json.Marshal(msg)
	if err != nil {
		fmt.Println(err)
	}

	w.Header().Set("Response Header", "This is the Response Header")
	w.Header().Set("access-control-allow-methods", "POST")
	w.Header().Set("access-control-allow-origin", "*")
	w.Header().Set("Content-type", "Access-Control-Allow-Headers")
	io.WriteString(w, string(response))
	return
}
