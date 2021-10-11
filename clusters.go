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
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#cluster
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

// Returns a list of known Kafka clusters. Currently both Kafka and Kafka REST Proxy are only aware
// of the Kafka cluster pointed at by the bootstrap.servers configuration.
// Therefore only one Kafka cluster will be returned in the response.
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#get--clusters
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

// Returns the Kafka cluster with the specified cluster_id.
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#get--clusters-cluster_id
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
