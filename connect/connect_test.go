//go:build unit
// +build unit

package connect

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

const fullConnectorResponse = `{
           "name": "somename",
           "config": {
             "someparamone": "somevalone",
             "someparamtwo": "somevaltwo"
           },
           "tasks": [
             { "connector": "somename", "task": 1 },
             { "connector": "somename", "task": 2 }
           ]}`

func setup() *Client {
	c, _ := NewClient(nil, "http://localhost/")
	//c, _ := NewClientWithBasicAuth(nil, "http://localhost/", "someuname", "somepasswd")
	return c
}

func newResponder(statusCode int, contentBody string, contentType string) httpmock.Responder {
	resp := httpmock.NewStringResponse(statusCode, contentBody)
	resp.Header.Set("Content-Type", contentType)
	return httpmock.ResponderFromResponse(resp)
}

func TestClustersGet(t *testing.T) {

	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"GET",
		"http://localhost/",
		newResponder(200, `{"version":"v","commit":"c","kafka_cluster_id":"kci"}`, "application/json"),
	)

	sc, err := c.Clusters.Get()

	assert.Nil(t, err)
	assert.NotNil(t, sc)
	assert.Equal(t, "v", sc.Version)
	assert.Equal(t, "c", sc.Commit)
	assert.Equal(t, "kci", sc.KafkaClusterID)

}

func TestConnectorsList(t *testing.T) {

	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"GET",
		"http://localhost/connectors",
		newResponder(200, `["alpha", "bravo", "charlie"]`, "application/json"),
	)

	r, err := c.Connectors.List()

	assert.Nil(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, 3, len(r))
	assert.Equal(t, "alpha", r[0])
	assert.Equal(t, "bravo", r[1])
	assert.Equal(t, "charlie", r[2])
}

func TestConnectorsCreate(t *testing.T) {
	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("POST", "http://localhost/connectors", newResponder(200, fullConnectorResponse, "application/json"))

	//r, err := c.Connectors.Create(Connector{})
	r, err := c.Connectors.Create("somename", map[string]interface{}{"someparamone": "somevalone", "someparamtwo": "somevaltwo"})

	assert.Nil(t, err)
	assert.NotNil(t, r)

	assert.NotNil(t, r.Name)
	assert.Equal(t, "somename", r.Name)

	assert.NotNil(t, r.Config)
	assert.Nil(t, r.Config["foo"])

	assert.Equal(t, 2, len(r.Config))
	assert.Equal(t, "somevalone", r.Config["someparamone"])
	assert.Equal(t, "somevaltwo", r.Config["someparamtwo"])

	assert.NotNil(t, r.Tasks)
	assert.Equal(t, 2, len(r.Tasks))
	assert.Equal(t, 1, r.Tasks[0].Task)
	assert.Equal(t, 2, r.Tasks[1].Task)

}

func TestConnectorsUpdate(t *testing.T) {
	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("PUT", "http://localhost/connectors/somename/config", newResponder(200, fullConnectorResponse, "application/json"))

	//r, err := c.Connectors.Create(Connector{})
	r, err := c.Connectors.Update("somename", map[string]interface{}{"someparamone": "somevalone", "someparamtwo": "somevaltwo"})

	assert.Nil(t, err)
	assert.NotNil(t, r)

	assert.NotNil(t, r.Name)
	assert.Equal(t, "somename", r.Name)

	assert.NotNil(t, r.Config)
	assert.Nil(t, r.Config["foo"])

	assert.Equal(t, 2, len(r.Config))
	assert.Equal(t, "somevalone", r.Config["someparamone"])
	assert.Equal(t, "somevaltwo", r.Config["someparamtwo"])

	assert.NotNil(t, r.Tasks)
	assert.Equal(t, 2, len(r.Tasks))
	//assert.Equal(t, 1, r.Tasks[0].TaskID)
	//assert.Equal(t, 2, r.Tasks[1].TaskID)

}

func TestConnectorsGet(t *testing.T) {
	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost/connectors/somename", newResponder(200, fullConnectorResponse, "application/json"))

	//r, err := c.Connectors.Get(GetConnectorRequest{Name: "somename"})
	r, err := c.Connectors.Get("somename")

	assert.Nil(t, err)
	assert.NotNil(t, r)

	assert.NotNil(t, r.Name)
	assert.Equal(t, "somename", r.Name)

	assert.NotNil(t, r.Config)
	assert.Nil(t, r.Config["foo"])

	assert.Equal(t, 2, len(r.Config))
	assert.Equal(t, "somevalone", r.Config["someparamone"])
	assert.Equal(t, "somevaltwo", r.Config["someparamtwo"])

	assert.NotNil(t, r.Tasks)
	assert.Equal(t, 2, len(r.Tasks))
	assert.Equal(t, 1, r.Tasks[0].Task)
	assert.Equal(t, 2, r.Tasks[1].Task)

}

//
//
//
/*
func TestConnectorsGetConfig(t *testing.T) {
	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost/connectors/somename/config",
		newResponder(200,
			`{"someparamone": "somevalone", "someparamtwo": "somevaltwo"}`,
			"application/json"))

	r, err := c.Connectors.GetConfig(GetConnectorConfigRequest{Name: "somename"})

	assert.Nil(t, err)
	assert.NotNil(t, r)

	assert.Equal(t, 2, len(r.Config))
	assert.Equal(t, "somevalone", r.Config["someparamone"])
	assert.Equal(t, "somevaltwo", r.Config["someparamtwo"])

}
*/

//
//
//
/*
func TestConnectorsPutConfig(t *testing.T) {
	c := setup()

	httpmock.Reset()
	httpmock.ActivateNonDefault(c.restyClient.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("PUT", "http://localhost/connectors/somename/config",
		newResponder(200,
			`{"someparamone": "somevalone", "someparamtwo": "somevaltwo"}`,
			"application/json"))

	r, err := c.Connectors.PutConfig(PutConnectorConfigRequest{Name: "somename", Config: map[string]interface{}{"someparamone": "somevalone", "someparamtwo": "somevaltwo"}})

	assert.Nil(t, err)
	assert.NotNil(t, r)

	assert.Equal(t, 2, len(r.Config))
	assert.Equal(t, "somevalone", r.Config["someparamone"])
	assert.Equal(t, "somevaltwo", r.Config["someparamtwo"])


}
*/

/*
For later...

{
  "name": "DatagenSourceConnector_0",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSourceConnector_0",
    "kafka.api.key": "****",
    "kafka.api.secret": "****",
    "kafka.topic": "air.9403.datagen.users",
    "output.data.format": "JSON",
    "quickstart": "USERS",
    "max.interval": "73000",
    "tasks.max": "1"
  }
}


{
  "name": "DatagenSourceConnector_00",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSourceConnector_00",
    "kafka.topic": "topic00",
    "output.data.format": "JSON",
    "quickstart": "USERS",
    "max.interval": "1000",
    "tasks.max": "1"
  }
}

curl -X POST \
  http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{ "name": "datagen00",
    "config": {
      "connector.class":"io.confluent.kafka.connect.datagen.DatagenConnector",
      "topics":"topic00",
      "key.converter":"org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable":false,
      "value.converter":"org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable":false,
      "mongodb.connection.uri": "<PUT_YOUR_CONNECTION_STRING_HERE_AND_ADD_DATABASE_NAME>",
      "mongodb.collection": "blogpost"
    }
}
'





BEGIN THIS ONE WORKS for cp-all-in-one
curl -X POST \
  http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "DatagenUsers00",
        "config": {
           "name": "DatagenUsers00",
           "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
           "key.converter": "org.apache.kafka.connect.storage.StringConverter",
           "kafka.topic": "datagen.users00",
           "max.interval": "1000",
           "quickstart": "users"
        }
     }'
END THIS ONE WORKS



*/
