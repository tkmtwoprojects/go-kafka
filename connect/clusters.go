//
// Simple operation to get the details about a particular cluster.
// 
package connect

import (
	"fmt"
)

type ClustersService service

type Cluster struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterID string `json:"kafka_cluster_id"`
}

func (s *ClustersService) Get() (Cluster, error) {

	result := Cluster{}

	resp, err :=
		s.client.restyClient.
			NewRequest().
			SetResult(&result).
			Get(s.client.BaseURL.String())

	if resp.StatusCode() >= 400 {
		return Cluster{}, fmt.Errorf("Get: %v", resp.String())
	}

	if err != nil {
		return Cluster{}, err
	}

	return result, err
}
