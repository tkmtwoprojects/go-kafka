package connect

//
// Implement KC as defined in
//   https://docs.confluent.io/platform/current/connect/references/restapi.html#get--connectors
//

import (
	"fmt"
)

type ConnectorsService service

//
//List all connectors as in
//  https://docs.confluent.io/platform/current/connect/references/restapi.html#get--connectors
//
func (s *ConnectorsService) List() ([]string, error) {
	var connectorNames []string

	resp, err :=
		s.client.restyClient.
			NewRequest().
			SetResult(&connectorNames).
			Get(s.client.BaseURL.String() + "connectors")

	if resp.StatusCode() >= 400 {
		return []string{}, fmt.Errorf("Get: %v", resp.String())
	}

	if err != nil {
		return []string{}, err
	}

	return connectorNames, err

}

//
// Task has both connector name and task id
//
type Task struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}

//
// A Connector struct with everything in it
//
type Connector struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config,omitempty"`
	Tasks  []Task                 `json:"tasks,omitempty"`
}

//
// Create a new connector as in
//   https://docs.confluent.io/platform/current/connect/references/restapi.html#post--connectors
//
func (s *ConnectorsService) Create(name string, config map[string]interface{}) (Connector, error) {
	requestConnector := Connector{Name: name, Config: config}
	result := Connector{}

	resp, err :=
		s.client.restyClient.
			NewRequest().
			SetBody(requestConnector).
			SetResult(&result).
			Post(s.client.BaseURL.String() + "connectors")

	if resp.StatusCode() >= 400 {
		return Connector{}, fmt.Errorf("Get: %v", resp.String())
	}

	if err != nil {
		return Connector{}, err
	}

	return result, err
}

//
// Update a connector as in
//   https://docs.confluent.io/platform/current/connect/references/restapi.html#put--connectors-(string-name)-config
//
func (s *ConnectorsService) Update(name string, config map[string]interface{}) (Connector, error) {

	result := Connector{}

	resp, err :=
		s.client.restyClient.
			NewRequest().
			SetBody(config).
			SetResult(&result).
			Put(s.client.BaseURL.String() + "connectors/" + name + "/config")

	if resp.StatusCode() >= 400 {
		return Connector{}, fmt.Errorf("Get: %v", resp.String())
	}

	if err != nil {
		return Connector{}, err
	}

	return result, err
}

//
// Delete a connector as in
//   https://docs.confluent.io/platform/current/connect/references/restapi.html#delete--connectors-(string-name)-
//
func (s *ConnectorsService) Delete(name string) error {

	resp, err :=
		s.client.restyClient.
			NewRequest().
			Delete(s.client.BaseURL.String() + "connectors/" + name + "/")

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("Get: %v", resp.String())
	}

	return err
}

//
// Get/Read a connector as in
//   https://docs.confluent.io/platform/current/connect/references/restapi.html#get--connectors-(string-name)
//
func (s *ConnectorsService) Get(name string) (Connector, error) {

	result := Connector{}

	resp, err :=
		s.client.restyClient.
			NewRequest().
			SetResult(&result).
			Get(s.client.BaseURL.String() + "connectors/" + name)

	if resp.StatusCode() >= 400 {
		return Connector{}, fmt.Errorf("Get: %v", resp.String())
	}

	if err != nil {
		return Connector{}, err
	}

	return result, err
}
