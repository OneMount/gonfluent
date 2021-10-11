package confluent

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogin_Success(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/security/1.0/authenticate", uri)
		return []byte(`
		{
			"auth_token": "abcdefghizk",
			"auth_type": "Basic Auth",
			"expires_in": 3600
		}
`), 204, "204", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	token, err := c.Login()
	assert.Nil(t, err)
	assert.Equal(t, "abcdefghizk", token)
}

func TestLogin_Fail(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/security/1.0/authenticate", uri)
		return nil, 403, "403", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	_, err := c.Login()
	assert.NotNil(t, err)
}

func TestLogin_FailWithWrongResponse(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodGet, method, "Expected method 'GET', got %s", method)
		assert.Equal(t, "/security/1.0/authenticate", uri)
		return []byte(`
		<
`), 200, "200", nil
	}
	clusterAdmin, _ := mk.NewSaramaClusterAdmin()
	c := NewClient(&mock, &mk, clusterAdmin)
	_, err := c.Login()
	assert.NotNil(t, err)
}
