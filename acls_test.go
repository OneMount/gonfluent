package confluent

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcls_ListAclsSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/clusters/cluster-1/acls", uri)
		return []byte(`
		{
		"kind": "KafkaAclList",
		"metadata": {
			"self": "http://localhost:9391/v3/clusters/cluster-1/acls?principal=alice"
		},
		"data": [
			{
				"kind": "KafkaAcl",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/acls?resource_type=TOPIC&resource_name=topic-&pattern_type=PREFIXED&principal=alice&host=*&operation=ALL&permission=ALLOW"
				},
				"cluster_id": "cluster-1",
				"resource_type": "TOPIC",
				"resource_name": "topic-",
				"pattern_type": "PREFIXED",
				"principal": "alice",
				"host": "*",
				"operation": "ALL",
				"permission": "ALLOW"
			},
			{
				"kind": "KafkaAcl",
				"metadata": {
					"self": "http://localhost:9391/v3/clusters/cluster-1/acls?resource_type=CLUSTER&resource_name=cluster-1&pattern_type=LITERAL&principal=bob&host=*&operation=DESCRIBE&permission=DENY"
				},
				"cluster_id": "cluster-1",
				"resource_type": "CLUSTER",
				"resource_name": "cluster-2",
				"pattern_type": "LITERAL",
				"principal": "alice",
				"host": "*",
				"operation": "DESCRIBE",
				"permission": "DENY"
			}
		]
	}
`), 200, "200 OK", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	acls, err := c.ListAcls("cluster-1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(acls))
	assert.Equal(t, "alice", acls[0].Principal)
}

func TestAcls_CreateAclsSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/clusters/cluster-1/acls", uri)
		return []byte(``), 201, "201", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	aclConfig := Acl{}
	err := c.CreateAcl("cluster-1", &aclConfig)
	assert.NoError(t, err)
}

func TestAcls_DeleteAclSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodDelete, method, "Expected method 'Delete', got %s", method)
		assert.Equal(t, "/clusters/cluster-1/acls", uri)
		return []byte(`
		{
			"data": [
				{
					"kind": "KafkaAcl",
					"metadata": {
						"self": "http://localhost:9391/v3/clusters/cluster-1/acls?resource_type=TOPIC&resource_name=topic-&pattern_type=PREFIXED&principal=alice&host=*&operation=ALL&permission=ALLOW"
					},
					"cluster_id": "cluster-1",
					"resource_type": "TOPIC",
					"resource_name": "topic-",
					"pattern_type": "PREFIXED",
					"principal": "alice",
					"host": "*",
					"operation": "ALL",
					"permission": "ALLOW"
				},
				{
					"kind": "KafkaAcl",
					"metadata": {
						"self": "http://localhost:9391/v3/clusters/cluster-1/acls?resource_type=CLUSTER&resource_name=cluster-1&pattern_type=LITERAL&principal=bob&host=*&operation=DESCRIBE&permission=DENY"
					},
					"cluster_id": "cluster-1",
					"resource_type": "CLUSTER",
					"resource_name": "cluster-2",
					"pattern_type": "LITERAL",
					"principal": "alice",
					"host": "*",
					"operation": "DESCRIBE",
					"permission": "DENY"
				}
			]
		}
`), 200, "200", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	err := c.DeleteAcl("cluster-1", "")
	assert.NoError(t, err)
}
