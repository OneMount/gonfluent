package confluent

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

func TestRBac_GetTopicPartitionsSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/partitions", uri)
		return []byte(`
		{
		"kind": "KafkaPartitionList",
		"metadata": {
			"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions",
			"next": null
		},
		"data": [
			{
				"kind": "KafkaPartition",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/1",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-1/partition=1"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-1",
				"partition_id": 1,
				"leader": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas/1"
				},
				"replicas": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/1/replicas"
				},
				"reassignment": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/1/reassignment"
				}
			},
			{
				"kind": "KafkaPartition",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/2",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-1/partition=2"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-1",
				"partition_id": 2,
				"leader": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas/2"
				},
				"replicas": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/2/replicas"
				},
				"reassignment": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/2/reassignment"
				}
			},
			{
				"kind": "KafkaPartition",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/3",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-1/partition=3"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-1",
				"partition_id": 3,
				"leader": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/3/replicas/3"
				},
				"replicas": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/3/replicas"
				},
				"reassignment": {
					"related": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/partitions/3/reassignment"
				}
			}
		]
	}
`), 200, "200 OK", nil
	}
	c := NewClient(&mock, &mk)
	partitions, err := c.GetTopicPartitions("cluster-1", "topic-1")
	if assert.Nil(t, err) {
		assert.Equal(t, 3, len(partitions))
		assert.Equal(t, "topic-1", partitions[0].TopicName)
		assert.Equal(t, 3, partitions[2].PartitionId)
	}
}

func TestRBac_GetTopicPartitionsFailWithNotExist(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/partitions", uri)
		return nil, 404, "404 Not Found", nil
	}
	c := NewClient(&mock, &mk)
	_, err := c.GetTopicPartitions("cluster-1", "topic-1")

	if assert.NotNil(t, err) {
		assert.Equal(t, errors.New("error with status: 404 Not Found"), err)
	}
}

func TestRBac_GetTopicPartitionsFailWithWrongResponse(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/partitions", uri)
		return []byte(`
		<
`), 200, "200 OK", nil
	}
	c := NewClient(&mock, &mk)
	_, err := c.GetTopicPartitions("cluster-1", "topic-1")
	assert.NotNil(t, err)
}
