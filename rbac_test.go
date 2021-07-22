package confluent

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	cDetails = ClusterDetails{
		Clusters: Clusters{
			KafkaCluster: clusterId,
		},
	}
	principal = "User:confluent-test"
	roleName  = "Operator"
	uRoleBinding = RoleBinding{
		Scope: cDetails,
		ResourcePatterns: []ResourcePattern{
			{
				ResourceType: "TOPIC",
				Name:         "Test-binding",
				PatternType:  "PREFIXED",
			},
		},
	}
)

func TestRBac_BindPrincipalToRoleSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator", uri)
		return []byte(``), 204, "204", nil
	}
	c := NewClient(&mock, &mk)
	err := c.BindPrincipalToRole("User:confluent-test", "Operator", cDetails)
	assert.Nil(t, err)
}

func TestRBac_BindPrincipalToRoleFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator", uri)
		return []byte(`
		{
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role Operator",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role Operator"
				}
			]
		}
		`), 422, "422 Unprocessable Entity", nil
	}
	c := NewClient(&mock, &mk)
	err := c.BindPrincipalToRole("User:confluent-test", "Operator", cDetails)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 422 Unprocessable Entity Cannot find role Operator"), err)
}

func TestRBac_DeleteRoleBindingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodDelete, method, "Expected method 'DELETE', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator", uri)
		return []byte(``), 204, "204", nil
	}
	c := NewClient(&mock, &mk)
	err := c.DeleteRoleBinding(principal, roleName, cDetails)
	assert.Nil(t, err)
}

func TestRBac_DeleteRoleBindingFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodDelete, method, "Expected method 'DELETE', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator", uri)
		return []byte(`
		{
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role Operator",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role Operator"
				}
			]
		}
`), 422, "422 Unprocessable Entity", nil
	}
	c := NewClient(&mock, &mk)
	err := c.DeleteRoleBinding("User:confluent-test", "Operator", ClusterDetails{
		Clusters: Clusters{
			KafkaCluster: "cluster-1",
		},
	})
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 422 Unprocessable Entity Cannot find role Operator"), err)
}

func TestRBac_LookupRoleBindingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/resources", uri)
		return []byte(`
		[
			{
				"resourceType": "Topic",
				"name": "clicksTopic1",
				"patternType": "LITERAL"
			},
			{
				"resourceType": "Topic",
				"name": "orders-2019",
				"patternType": "PREFIXED"
			}
		]
`), 200, "200 OK", nil
	}
	c := NewClient(&mock, &mk)
	roleBinding, err := c.LookupRoleBinding(principal, roleName, cDetails)
	if assert.Nil(t, err) {
		assert.Equal(t, 2, len(roleBinding))
		assert.Equal(t, "clicksTopic1", roleBinding[0].Name)
	}
}

func TestRBac_LookupRoleBindingFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/resources", uri)
		return []byte(`
		{
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role `+roleName+`",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role `+roleName+`"
				}
			]
		}
`), 400, "404 Not Found", nil
	}
	c := NewClient(&mock, &mk)
	roleBinding, err := c.LookupRoleBinding(principal, roleName, cDetails)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 404 Not Found Cannot find role "+roleName), err)
	assert.Nil(t, roleBinding)
}

func TestRBac_IncreaseRoleBindingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(``), 200, "200 OK", nil
	}
	c := NewClient(&mock, &mk)
	err := c.IncreaseRoleBinding(principal, roleName, uRoleBinding)
	assert.Nil(t, err)
}

func TestRBac_IncreaseRoleBindingFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPost, method, "Expected method 'POST', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(`
		{
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role `+roleName+`",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role `+roleName+`"
				}
			]
		}
`), 404, "404 Not Found", nil
	}
	c := NewClient(&mock, &mk)
	err := c.IncreaseRoleBinding(principal, roleName, uRoleBinding)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 404 Not Found Cannot find role "+roleName), err)
}

func TestRBac_DecreaseRoleBindingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodDelete, method, "Expected method 'DELETE', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(``), 204, "204", nil
	}
	c := NewClient(&mock, &mk)
	err := c.DecreaseRoleBinding(principal, roleName, uRoleBinding)
	assert.Nil(t, err)
}

func TestRBac_DecreaseRoleBindingFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodDelete, method, "Expected method 'DELETE', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(`
		{	
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role `+roleName+`",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role `+roleName+`"
				}
			]
		}
`), 404, "404 Not Found", nil
	}
	c := NewClient(&mock, &mk)
	err := c.DecreaseRoleBinding(principal, roleName, uRoleBinding)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 404 Not Found Cannot find role "+roleName), err)
}

func TestRBac_OverwriteRoleBindingSuccess(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPut, method, "Expected method 'PUT', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(`
		{	
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role `+roleName+`",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role `+roleName+`"
				}
			]
		}
`), 204, "", nil
	}
	c := NewClient(&mock, &mk)
	err := c.OverwriteRoleBinding(principal, roleName, uRoleBinding)
	assert.Nil(t, err)
}

func TestRBac_OverwriteRoleBindingFailWithNonExistedRole(t *testing.T) {
	mock := MockHttpClient{}
	mk := MockKafkaClient{}
	mock.DoRequestFn = func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)  {
		assert.Equal(t, http.MethodPut, method, "Expected method 'PUT', got %s", method)
		assert.Equal(t, "/security/1.0/principals/User:confluent-test/roles/Operator/bindings", uri)
		return []byte(`
		{
			"status_code": 422,
			"error_code": 4221,
			"type": "NonExisted",
			"message": "Cannot find role Operator",
			"errors": [
				{
					"error_type": "string",
					"message": "Cannot find role Operator"
				}
			]
		}
`), 404, "404 Not Found", nil
	}
	c := NewClient(&mock, &mk)
	err := c.OverwriteRoleBinding(principal, roleName, uRoleBinding)
	assert.NotNil(t, err)
	assert.Equal(t, errors.New("error with status: 404 Not Found Cannot find role "+roleName), err)
}
