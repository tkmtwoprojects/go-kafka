//go:build integration
// +build integration

package connect

import (
	"fmt"
	"log"
	//"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
const someConnectorResponse = `{
           "name": "somename",
           "config": {
             "someparamone": "somevalone",
             "someparamtwo": "somevaltwo"
           },
           "tasks": [
             { "connector": "somename", "task": 1 },
             { "connector": "somename", "task": 2 }
           ]}`
*/

func setup() *Client {
	c, _ := NewClient(nil, "http://localhost:8083/")
	return c
}

//
// Create new datagen with suffixes in name and topic name
//
func NewDatagenConnector(suffix string) Connector {
	c := Connector{Name: fmt.Sprintf("DatagenUsers-%s", suffix), Config: map[string]interface{}{
		"connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
		"key.converter":   "org.apache.kafka.connect.storage.StringConverter",
		"kafka.topic":     fmt.Sprintf("datagen.users.%s", suffix),
		"max.interval":    "5000",
		"quickstart":      "users"}}
	return c
}

func TestItAllAlpha(t *testing.T) {
	client := setup()

	//
	//Get the cluster info, just make sure it's not empty
	//
	log.Print("Ensure we have a cluster")
	clusterInfo, err := client.Clusters.Get()
	assert.Nil(t, err)
	assert.NotNil(t, clusterInfo)
	assert.NotNil(t, clusterInfo.Version)
	assert.NotNil(t, clusterInfo.Commit)
	assert.NotNil(t, clusterInfo.KafkaClusterID)

	//
	//Tickle the connectors list
	//
	log.Print("Make sure we can get a list of connector names")
	connectorNames, err := client.Connectors.List()

	assert.Nil(t, err)
	assert.NotNil(t, connectorNames)

	//
	//Update connector Alpha
	//
	log.Print("Create a new DatagenConnector with the \"Alpha\" suffix")
	connAlphaReq := NewDatagenConnector("Alpha")

	connAlphaRes, err := client.Connectors.Update(connAlphaReq.Name, connAlphaReq.Config)
	assert.Nil(t, err)
	assert.NotNil(t, connAlphaRes)
	assert.NotNil(t, connAlphaRes.Name)
	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Name)
	assert.NotNil(t, connAlphaRes.Config)
	assert.Equal(t, 6, len(connAlphaRes.Config))

	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Config["name"])
	assert.Equal(t, "io.confluent.kafka.connect.datagen.DatagenConnector", connAlphaRes.Config["connector.class"])
	assert.Equal(t, "org.apache.kafka.connect.storage.StringConverter", connAlphaRes.Config["key.converter"])
	assert.Equal(t, "datagen.users.Alpha", connAlphaRes.Config["kafka.topic"])
	assert.Equal(t, "5000", connAlphaRes.Config["max.interval"])
	assert.Equal(t, "users", connAlphaRes.Config["quickstart"])

	log.Print("Change the max.interval to 5555")
	connAlphaReq.Config["max.interval"] = "5555"
	assert.Equal(t, "5555", connAlphaReq.Config["max.interval"])

	connAlphaRes, err = client.Connectors.Update(connAlphaReq.Name, connAlphaReq.Config)

	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Name)
	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Config["name"])
	assert.Equal(t, "io.confluent.kafka.connect.datagen.DatagenConnector", connAlphaRes.Config["connector.class"])
	assert.Equal(t, "org.apache.kafka.connect.storage.StringConverter", connAlphaRes.Config["key.converter"])
	assert.Equal(t, "datagen.users.Alpha", connAlphaRes.Config["kafka.topic"])
	assert.Equal(t, "5555", connAlphaRes.Config["max.interval"])
	assert.Equal(t, "users", connAlphaRes.Config["quickstart"])

	log.Print("Add a nonsense config parameter")
	connAlphaReq.Config["nosuchparam"] = "nosuchvalue"
	connAlphaRes, err = client.Connectors.Update(connAlphaReq.Name, connAlphaReq.Config)

	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Name)
	assert.Equal(t, "DatagenUsers-Alpha", connAlphaRes.Config["name"])
	assert.Equal(t, "io.confluent.kafka.connect.datagen.DatagenConnector", connAlphaRes.Config["connector.class"])
	assert.Equal(t, "org.apache.kafka.connect.storage.StringConverter", connAlphaRes.Config["key.converter"])
	assert.Equal(t, "datagen.users.Alpha", connAlphaRes.Config["kafka.topic"])
	assert.Equal(t, "5555", connAlphaRes.Config["max.interval"])
	assert.Equal(t, "users", connAlphaRes.Config["quickstart"])
	assert.Equal(t, "nosuchvalue", connAlphaRes.Config["nosuchparam"])

	log.Print("Delete DatagenUsers-Alpha since we're all done")
	err = client.Connectors.Delete(connAlphaReq.Name)

}
