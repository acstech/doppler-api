package couchbase

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/couchbase/gocb"
)

type Couchbase struct {
	Bucket *gocb.Bucket
	Doc    *Doc
}

type Doc struct {
	ID     string   `json:"ID,omitempty"`
	Events []string `json:"Events"`
}

func (c *Couchbase) collectEvents(clientID string) error {
	var err error
	var docFrag *gocb.DocumentFragment
	docFrag, err = c.Bucket.LookupIn(fmt.Sprintf("doppler:client:%s", clientID)).Get("Events").Execute()
	if err != nil {
		return err
	}
	// get the Events array into a slice
	docFrag.Content("Events", &c.Doc.Events) // Error, but why is Doc.Events null?
	if err != nil {

		return err
	}
	return nil
}

func (c *Couchbase) ClientExists(clientID string) bool {
	err := c.collectEvents(clientID)
	if err != nil {
		// check to see if the key exists
		if gocb.IsKeyNotFoundError(err) {
			return false
		}
		panic(err)
	}
	return true
}

func (c *Couchbase) ConnectToCB(conn string) error {
	// parse the connection string into a url for later use
	u, err := url.Parse(conn)
	if err != nil {
		return err
	}
	// make sure that the url is going to couchbase
	if u.Scheme != "couchbase" {
		return errors.New("Scheme must be couchbase")
	}
	// make sure that a username and password exist, this is required by couchbase 5 and higher
	username, password := "", ""
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	// make sure that the bucket to connect to is specified
	if u.Path == "" || u.Path == "/" {
		return errors.New("Bucket not specified")
	}
	// get the proper connection format (couchbase//host) and connect to the cluster
	spec := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	cluster, err := gocb.Connect(spec)
	if err != nil {
		return err
	}
	// authenticate the user and connect to the specified bucket
	cluster.Authenticate(&gocb.PasswordAuthenticator{Username: username, Password: password})
	c.Bucket, err = cluster.OpenBucket(u.Path[1:], "")
	if err != nil {
		return err
	}
	return nil
}
