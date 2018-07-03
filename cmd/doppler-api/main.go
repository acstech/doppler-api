package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	_ "github.com/joho/godotenv/autoload"

	"github.com/acstech/doppler-api/internal/liveupdate"
)

func main() {
	// get environment variables
	cbCon := os.Getenv("COUCHBASE_CONN")
	kafkaCon, kafkaTopic, err := kafkaParse(os.Getenv("KAFKA_CONN"))
	// influxCon := os.Getenv("INFLUX_CONN")
	// // creates influx client
	// c, err := client.NewHTTPClient(client.HTTPConfig{
	// 	Addr:     "http://localhost:8086",
	// 	Username: "root",
	// 	Password: "root",
	// })
	// if err != nil {
	// 	panic(fmt.Errorf("error connecting to influx: %v", err))
	// }
	// defer c.Close()

	// // queries influx for all points, query is temporarily hard coded
	// influx := false
	// if influx == true {
	// 	res, err := fx.GetPoints(c, "select * from dopplerDataHistory")
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	} else {
	// 		i := 0
	// 		for i < len(res.ValArray) {
	// 			fmt.Println(res.ValArray[i])
	// 			i = i + 1
	// 		}
	// 	}
	// }

	//intialize websocket management and kafka consume
	go liveupdate.InitWebsockets(cbCon)
	err = liveupdate.Consume(kafkaCon, kafkaTopic)
	if err != nil {
		panic(fmt.Errorf("error connecting to kafka: %v", err))
	}
}

func kafkaParse(conn string) (string, string, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return "", "", err
	}
	if u.Host == "" {
		return "", "", errors.New("Kafka address is not specified, verify that your environment varaibles are correct")
	}
	address := u.Host
	// make sure that the topic is specified
	if u.Path == "" || u.Path == "/" {
		return "", "", errors.New("Kafka topic is not specified, verify that your environment varaibles are correct")
	}
	topic := u.Path[1:]
	return address, topic, nil
}
