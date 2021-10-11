package confluent

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusters_ListKafkaCluster(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters", uri)
		return []byte(`
{
			"kind": "KafkaClusterList",
			"metadata": {
				"self": "http://localhost:9391/v3/clusters",
				"next": null
			},
			"data": [
				{
					"kind": "KafkaCluster",
					"metadata": {
						"self": "http://localhost:9391/v3/clusters/cluster-1",
						"resource_name": "crn:///kafka=cluster-1"
					},
					"cluster_id": "cluster-1",
					"controller": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/brokers/1"
					},
					"acls": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/acls"
					},
					"brokers": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/brokers"
					},
					"broker_configs": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/broker-configs"
					},
					"consumer_groups": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/consumer-groups"
					},
					"topics": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/topics"
					},
					"partition_reassignments": {
						"related": "http://localhost:9391/v3/clusters/cluster-1/topics/-/partitions/-/reassignment"
					}
				}
			]
		}
`), 200, "200 OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	clusters, err := c.ListKafkaCluster()
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(clusters))
		assert.Equal(t, "cluster-1", clusters[0].ClusterID)
	}
}

func TestClusters_GetNonExistingKafkaCluster(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1", uri)
		return []byte(`
		{
			"error_code": 404,
			"message": "HTTP 404 Not Found"
		}
`), 404, "404 Not Found", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	cluster, err := c.GetKafkaCluster("cluster-1")
	assert.Equal(t, errors.New("error with status: 404 Not Found HTTP 404 Not Found"), err)
	assert.Nil(t, cluster)
}

func TestClusters_GetExistingKafkaCluster(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1", uri)
		return []byte(`
		{
			"kind": "KafkaCluster",
			"metadata": {
				"self": "http://localhost:9391/v3/clusters/cluster-1",
				"resource_name": "crn:///kafka=cluster-1"
			},
			"cluster_id": "cluster-1",
			"controller": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/brokers/1"
			},
			"acls": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/acls"
			},
			"brokers": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/brokers"
			},
			"broker_configs": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/broker-configs"
			},
			"consumer_groups": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/consumer-groups"
			},
			"topics": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/topics"
			},
			"partition_reassignments": {
				"related": "http://localhost:9391/v3/clusters/cluster-1/topics/-/partitions/-/reassignment"
			}
		}
`), 200, "200 OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	cluster, err := c.GetKafkaCluster("cluster-1")
	if assert.NoError(t, err) {
		assert.Equal(t, "cluster-1", cluster.ClusterID)
	}
}
