package service

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

//KafkaData is the JSON format messages from Kafka
type KafkaData struct {
	Latitude  string `json:"lat,omitempty"`
	Longitude string `json:"lng,omitempty"`
	ClientID  string `json:"clientID,omitempty"`
	EventID   string `json:"eventID,omitempty"`
}

//Latlng is the struct to hold data that is sent to the frontend
type Latlng struct {
	Coords Point `json:"latlng,omitempty"`
	Count  int   `json:"count,omitempty"`
}

// Point that holds the specific lat lng data
type Point struct {
	Lat string `json:"lat,omitempty"`
	Lng string `json:"lng,omitempty"`
}

// Consume consumes messages from Kafka
func (c *ConnectionManager) Consume(consumer sarama.PartitionConsumer) {
	fmt.Println("Kafka Consume Started")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit)

	//continually consumes messages from Kafka
Loop:
	for {
		select {
		// In case of error
		case err := <-consumer.Errors():
			fmt.Println(err)
			// Print consumer messages
		case msg := <-consumer.Messages():
			//initialize variable to hold data from kafka data
			var kafkaData KafkaData
			err := json.Unmarshal(msg.Value, &kafkaData) //unmarshal data to json
			if err != nil {
				fmt.Println(err)
			}
			// Check if ClientID exists
			c.mutex.RLock()
			_, contains := c.connections[kafkaData.ClientID]

			// if clientID exists, lock state, and send to client's connections
			if contains {
				// If client is connected, get map of connections
				clientConnections := c.connections[kafkaData.ClientID]
				// iterate over client connections
				for conn := range clientConnections {
					conn.mutex.Lock()

					// Check if consume message has a different filter than allfilters
					_, contains := conn.allFilters[kafkaData.EventID]
					if !contains {
						conn.updateAvailableFilters(kafkaData.EventID)
						fmt.Println("UPDATED AVAILABLE FILTERS")
					}

					// if connection filter has KafkaData eventID, send data
					_, hasEvent := conn.activeFilters[kafkaData.EventID]
					if hasEvent {
						// check if batchArray is full, if so, flush
						if len(conn.batchMap) == c.maxBatchSize {
							fmt.Println("SIZE FLUSH")
							conn.flush()
						}
						// add KafkaData of just eventID, lat, lng to batchArray
						conn.bucketPoints(Point{
							Lat: kafkaData.Latitude,
							Lng: kafkaData.Longitude,
						})
						conn.mutex.Unlock()
					}
				}
			}
			c.mutex.RUnlock()
		case <-quit:
			fmt.Println("Interrupt detected")
			break Loop
		}
	}

	// If receive quit signal, close consumer
	fmt.Println("Consumption closed")
}

// updateAvailableFilters adds a new filter found in consume messages to allFilters and sends the available filters to the client
func (conn *ConnWithParameters) updateAvailableFilters(newFilter string) {
	// Add new filter to map
	conn.allFilters[newFilter] = struct{}{}
	//initilize slice for sending to client
	var clientEvents []string
	//add all of the available filters to the slice
	for key := range conn.allFilters {
		clientEvents = append(clientEvents, key)
	}
	//send slice to client
	err := conn.ws.WriteJSON(clientEvents)
	if err != nil {
		fmt.Println(err)
	}
}

// bucketPoints takes a connection and a point and puts them in the batch of buckets as necessary
func (conn *ConnWithParameters) bucketPoints(rawPt Point) {

	// Truncate each item in batch
	// Split float by decimal
	latSlice := strings.SplitAfter(rawPt.Lat, ".")
	lngSlice := strings.SplitAfter(rawPt.Lng, ".")

	// Truncate second half of slices
	latSlice[1] = conn.truncate(latSlice[1])
	lngSlice[1] = conn.truncate(lngSlice[1])

	//check for truncating edge case
	if strings.Contains(latSlice[0], "-0.") {
		latSlice = conn.checkZero(latSlice)
	}
	if strings.Contains(lngSlice[0], "-0.") {
		lngSlice = conn.checkZero(lngSlice)
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
	_, contains := conn.batchMap[bucket]
	if contains {
		value := conn.batchMap[bucket] //get the value of the bucket

		value.Count++ //increase the count

		conn.batchMap[bucket] = value //add the new count to the point

	} else { //otherwise, add the point with the count
		conn.batchMap[bucket] = pt
	}
}

// trucate takes a string and changes its length based on truncateSize
func (conn *ConnWithParameters) truncate(s string) string {
	// get truncate size
	tSize := conn.truncateSize

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
func (conn *ConnWithParameters) checkZero(coord []string) []string {
	//compare the decimals of the "-0." case to the zeroTest
	//if they are equal, remove the "-"
	zeroTest := conn.zeroTest
	if strings.Compare(coord[1], zeroTest) == 0 {
		coord[0] = "0."
		return coord
	}
	return coord
}
