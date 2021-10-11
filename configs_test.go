package confluent

import (
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
)

func TestConfigs_GetTopicConfigs(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/configs", uri)
		return []byte(`
	{
		"kind": "KafkaTopicConfigList",
		"metadata": {
			"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/configs",
			"next": null
		},
		"data": [
			{
				"kind": "KafkaTopicConfig",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/configs/cleanup.policy",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-1/config=cleanup.policy"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-1",
				"name": "cleanup.policy",
				"value": "compact",
				"is_default": false,
				"is_read_only": false,
				"is_sensitive": false,
				"source": "DYNAMIC_TOPIC_CONFIG",
				"synonyms": [
					{
						"name": "cleanup.policy",
						"value": "compact",
						"source": "DYNAMIC_TOPIC_CONFIG"
					},
					{
						"name": "cleanup.policy",
						"value": "delete",
						"source": "DEFAULT_CONFIG"
					}
				]
			},
			{
				"kind": "KafkaTopicConfig",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/topics/topic-1/configs/compression.type",
					"resource_name": "crn:///kafka=cluster-1/topic=topic-1/config=compression.type"
				},
				"cluster_id": "cluster-1",
				"topic_name": "topic-1",
				"name": "compression.type",
				"value": "gzip",
				"is_default": false,
				"is_read_only": false,
				"is_sensitive": false,
				"source": "DYNAMIC_TOPIC_CONFIG",
				"synonyms": [
					{
						"name": "compression.type",
						"value": "gzip",
						"source": "DYNAMIC_TOPIC_CONFIG"
					},
					{
						"name": "compression.type",
						"value": "producer",
						"source": "DEFAULT_CONFIG"
					}
				]
			}
		]
	}
`), 200, "200 OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	configs, err := c.GetTopicConfigs("cluster-1", "topic-1")
	if assert.NoError(t, err) {
		assert.Equal(t, 2, len(configs))
		assert.Equal(t, "cluster-1", configs[0].ClusterId)
	}
}

func TestConfigs_GetTopicConfigsFailWithResourceNotFound(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/configs", uri)
		return nil, 404, "404 Not Found", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	_, err := c.GetTopicConfigs("cluster-1", "topic-1")
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "404 Not Found")
	}
}

func TestConfigs_GetTopicConfigsFailWithWrongData(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/configs", uri)
		return []byte("<"), 200, "200 OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	_, err := c.GetTopicConfigs("cluster-1", "topic-1")
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "invalid character '<'")
	}
}

func TestConfigs_UpdateTopicConfigs(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/configs:alter", uri)
		return nil, 204, "204 Accepted", nil
	}

	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	mockSynonyms := []TopicConfig{
		{
			Name: "cleanup.policy",
			Value: "DELETE",
		},
		{
			Name: "compression.type",
			Value: "gzip",
		},
	}

	err := c.UpdateTopicConfigs("cluster-1", "topic-1", mockSynonyms)
	assert.Nil(t, err)
}

func TestConfigs_UpdateTopicConfigsFailWithTopicNotFound(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/kafka/v3/clusters/cluster-1/topics/topic-1/configs:alter", uri)
		return nil, 404, "404 Not Found", nil
	}

	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	mockSynonyms := []TopicConfig{
		{
			Name: "cleanup.policy",
			Value: "DELETE",
		},
		{
			Name: "compression.type",
			Value: "gzip",
		},
	}

	err := c.UpdateTopicConfigs("cluster-1", "topic-1", mockSynonyms)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "404 Not Found")
	}
}