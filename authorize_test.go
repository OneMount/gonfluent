package confluent

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testPrincipals = []UserPrincipalAction{
		{
			Scope: Scope{
				Clusters: AuthorClusters{
					KafkaCluster: clusterId,
				},
			},
			ResourceName: "Testing-Principal",
			ResourceType: "Cluster",
			Operation:    "ClusterAdmin",
		},
	}
)

func TestAuthorize_CreatePrincipalSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPut, method, "Expected method 'PUT', got %s", method)
		assert.Equal(t, "/security/1.0/authorize", uri)
		return []byte(`
		  [
			"ALLOWED",
			"DENIED"
		  ]
`), 200, "200", nil
	}
	c := NewClient(&mock, &mk)
	newPrincipal, err := c.CreatePrincipal("User:testing", testPrincipals)
	assert.NoError(t, err)
	assert.Equal(t, "Testing-Principal", newPrincipal.Actions[0].ResourceName)
}

func TestAuthorize_CreatePrincipalFail(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPut, method, "Expected method 'PUT', got %s", method)
		assert.Equal(t, "/security/1.0/authorize", uri)
		return []byte(`
		  {
			"status_code": 400,
			"error_code": 0,
			"type": "INVALID REQUEST DATA",
			"message": "Bad request",
			"errors": [
			  {
				"error_type": "string",
				"message": "INVALID REQUEST DATA"
			  }
			]
		  }
`), 400, "400 Bad Request", nil
	}
	c := NewClient(&mock, &mk)
	newPrincipal, err := c.CreatePrincipal("User:testing", testPrincipals)
	assert.NotNil(t, err)
	assert.Nil(t, newPrincipal)
	assert.Equal(t, errors.New("error with status: 400 Bad Request INVALID REQUEST DATA"), err)
}
