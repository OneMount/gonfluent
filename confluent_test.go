package confluent

import (
	"io"
)

var (
	partitionConfig = []TopicConfig{
		{
			Name:  "compression.type",
			Value: "gzip",
		},
		{
			Name:  "cleanup.policy",
			Value: "compact",
		},
	}
	clusterId = "cluster-1"
)

type MockHttpClient struct {
	DoRequestFn func(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error)
}
func (mock *MockHttpClient) DoRequest(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error) {
	return mock.DoRequestFn(method, uri, reqBody)
}

