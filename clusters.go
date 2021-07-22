package confluent

import (
	"encoding/json"
)

const (
	clusterUri = "/kafka/v3/clusters"
)

//Clusters active in Confluent system
// Support:
// - kafka cluster
// - Kafka connect cluster
// - KSql cluster
// - Schema Registry cluster
type Clusters struct {
	// Kafka cluster ID
	KafkaCluster string `json:"kafka-cluster,omitempty"`

	// Kafka Connect Cluster ID
	ConnectCluster string `json:"connect-cluster,omitempty"`

	// kSQL cluster ID
	KSqlCluster string `json:"ksql-cluster,omitempty"`

	// Schema Registry Cluster ID
	SchemaRegistryCluster string `json:"schema-registry-cluster,omitempty"`
}

type KafkaCluster struct {
	ClusterID              string  `json:"cluster_id"`
}

type Related struct {
	Related string `json:"related"`
}

func (c *Client) ListKafkaCluster() ([]KafkaCluster, error) {
	resp, err := c.DoRequest("GET", clusterUri, nil)
	if err != nil {
		return nil, err
	}

	body := struct{
		Data []KafkaCluster `json:"data"`
	}{}

	err = json.Unmarshal(resp, &body)
	if err != nil {
		return nil, err
	}

	return body.Data, nil
}

func (c *Client) GetKafkaCluster(clusterId string) (*KafkaCluster, error) {
	pathUri := clusterUri + "/" + clusterId
	resp, err := c.DoRequest("GET", pathUri, nil)
	if err != nil {
		return nil, err
	}

	var cluster *KafkaCluster

	err = json.Unmarshal(resp, &cluster)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}
