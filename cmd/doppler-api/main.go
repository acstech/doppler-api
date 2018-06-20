package main

import (
	"github.com/acstech/doppler-api/internal/liveupdate"
)

func main() {
	go liveupdate.StartWebsocket()
	liveupdate.Consume()
}
