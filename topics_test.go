package confluent

import (
	"errors"
	"github.com/Shopify/sarama"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopics_CreateTopicsSuccess(t *testing.T) {
	mk := MockKafkaClient{}
	mock := MockHttpClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error) {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics", uri)
		return []byte(`{
				"kind": "KafkaTopic",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-X",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-X"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-X",
				"is_internal": false,
				"replication_factor": 3,
				"partitions": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-X/partitions"
				},
				"configs": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-X/configs"
				},
				"partition_reassignments": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-X/partitions/-/reassignments"
				}
			}
		`), 204, "204 Accepted", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	err := c.CreateTopic(clusterId, "topic-X", 3, 3, partitionConfig, nil)
	assert.NoError(t, err)
}

func TestTopics_CreateExistingTopic(t *testing.T) {
	mk := MockKafkaClient{}
	mock := MockHttpClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics", uri)
		return []byte(`
			{
				"error_code": 400,
				"message": "Topic 'topic-X' already exists"
			}
		`), 400, "400 Bad Request", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	err := c.CreateTopic(clusterId, "topic-X", 3, 3, partitionConfig, nil)
	assert.Equal(t, errors.New("error with status: 400 Bad Request Topic 'topic-X' already exists"), err)
}

func TestTopics_GetNonExistingTopic(t *testing.T) {
	mk := MockKafkaClient{}
	mock := MockHttpClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-X", uri)
		return []byte(`
			{
				"error_code": 404,
				"message": "This server does not host this topic-partition"
			}
		`), 404, "404 Not Found", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	newTopic, err := c.GetTopic(clusterId, "topic-X")
	assert.Equal(t, errors.New("error with status: 404 Not Found This server does not host this topic-partition"), err)
	assert.Nil(t, newTopic)
}


func TestTopics_GetAllTopics(t *testing.T) {
	mk := MockKafkaClient{}
	mock := MockHttpClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics", uri)
		return []byte(`
			{
				"kind": "KafkaTopicList",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics",
					"next": null
				},
				"data": [
					{
						"kind": "KafkaTopic",
						"metadata": {
							"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1",
							"resource_name": "crn:///kafka=cluster-1/topic=topic-1"
						},
						"cluster_id": "cluster-1",
						"topic_name": "topic-1",
						"is_internal": false,
						"replication_factor": 3,
						"partitions": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions"
						},
						"configs": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/configs"
						},
						"partition_reassignments": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/-/reassignments"
						}
					},
					{
						"kind": "KafkaTopic",
						"metadata": {
							"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-2",
							"resource_name": "crn:///kafka=cluster-1/topic=topic-2"
						},
						"cluster_id": "cluster-1",
						"topic_name": "topic-2",
						"is_internal": true,
						"replication_factor": 4,
						"partitions": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-2/partitions"
						},
						"configs": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-2/configs"
						},
						"partition_reassignments": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-2/partitions/-/reassignments"
						}
					},
					{
						"kind": "KafkaTopic",
						"metadata": {
							"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-3",
							"resource_name": "crn:///kafka=cluster-1/topic=topic-3"
						},
						"cluster_id": "cluster-1",
						"topic_name": "topic-3",
						"is_internal": false,
						"replication_factor": 5,
						"partitions": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-3/partitions"
						},
						"configs": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-3/configs"
						},
						"partition_reassignments": {
							"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-3/partitions/-/reassignments"
						}
					}
				]
			}
		`), 200, "OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	topics, err := c.ListTopics(clusterId)
	if assert.NoError(t, err) {
		assert.Equal(t, 3, len(topics))
		assert.Equal(t, "topic-2", topics[1].Name)
	}
}

func TestTopics_GetUpdatingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mk.TopicNameExpected = "my-topic"

	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)

	mockTopic := Topic{
		Name: "my-topic",
		Partitions: 3,
	}

	_, err := c.IsReplicationFactorUpdating(mockTopic.Name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTopics_IsPartitionRFChanging(t *testing.T) {
	var status sarama.PartitionReplicaReassignmentsStatus
	status.AddingReplicas = []int32{}
	status.RemovingReplicas = []int32{}
	assert.Equal(t, isPartitionRFChanging(&status), false)

	status.AddingReplicas = []int32{0, 2}

	assert.Equal(t, isPartitionRFChanging(&status), true)

	status.RemovingReplicas = []int32{0, 2}

	assert.Equal(t, isPartitionRFChanging(&status), true)
}

func TestTopics_BuildNewReplicas(t *testing.T) {
	allReplicas := []int32{1, 2, 3, 4}
	usedReplicas := []int32{5, 6, 7}
	deltaRF := int16(0)

	b,_ := buildNewReplicas(&allReplicas, &usedReplicas, deltaRF)
	assert.Equal(t, &usedReplicas, b)

	deltaRF = int16(-4)
	_, err := buildNewReplicas(&allReplicas, &usedReplicas, deltaRF)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("dropping too many replicas"), err)

	deltaRF = int16(-1)
	b, err = buildNewReplicas(&allReplicas, &usedReplicas, deltaRF)
	assert.Nil(t, err)
	assert.Equal(t, usedReplicas[:2], *b)

	deltaRF = int16(1)
	b, err = buildNewReplicas(&allReplicas, &usedReplicas, deltaRF)
	assert.Nil(t, err)
	assert.Equal(t, []int32{5, 6, 7, 2}, *b)

	deltaRF = int16(2)
	b, err = buildNewReplicas(&allReplicas, &usedReplicas, deltaRF)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("not enough brokers"), err)
}

