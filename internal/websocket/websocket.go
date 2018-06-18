package websocketService

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// func initWebsocket(clientID string, eventID string) {
// 	//[]ws := WebsocketList{}
// }

// Client, Event, Node, Connection
// type Websockets struct {
// 	list map[string]map[string]map[string]*websocket.Conn
// }

type Websockets struct {
	list map[string]map[uuid.UUID]*websocket.Conn
}

// type Connection struct {
// 	connections map[string]string
// }

// Start a websocket connection for client
func (*Websockets) StartWebsocket() {
	http.HandleFunc("/", point)
	log.Fatal(http.ListenAndServe(":8000", nil))
}

// Get a request for a point, then send coordinates back to front end
func point(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println(err)
			return
		}
		// checkConsumer(string(msg))
		// err = conn.WriteMessa
	}
}

func changeEvent(oldEvent string, newEvent string) {

}

// Assign values to websocket
// Filter is the event
// Node is the uuid to identify the connection so that we can modify connection
// in the future if necessary
func (w *Websockets) InitializeWebsocket(filter string, conn *websocket.Conn) {
	// Use satori uuid library to generate a uuid
	node, err := uuid.NewV4()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Filter is a map of connections
	w.list[filter] = make(map[uuid.UUID]*websocket.Conn)
	// Connection is set to this event and node
	w.list[filter][node] = conn
}

func NewWebsocket(filter string, node uuid.UUID, conn websocket.Conn) *Websockets {
	return &Websockets{
		make(map[string]map[uuid.UUID]*websocket.Conn),
	}
}
