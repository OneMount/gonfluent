package confluent

import (
	"crypto/tls"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net/http"
)

type HttpClient interface {
	DoRequest(method string, uri string, reqBody io.Reader)  (responseBody []byte, statusCode int, status string, err error)
}
type DefaultHttpClient struct {
	// BaseURL : https://localhost:8090
	// API endpoint of Confluent platform
	BaseUrl string

	// Define the user-agent would be sent to confluent api
	// Default: confluent-client-go-sdk
	Username string

	Password  string
	Token     string
	UserAgent string
}
func NewDefaultHttpClient(baseUrl string, username string, password string) *DefaultHttpClient {
	return &DefaultHttpClient{
		BaseUrl: baseUrl,
		Username: username,
		Password: password,
		UserAgent: userAgent,
	}
}
func (c *DefaultHttpClient) DoRequest(method string, uri string, reqBody io.Reader) (responseBody []byte, statusCode int, status string, err error) {
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	req, err := http.NewRequest(method, c.BaseUrl+uri, reqBody)
	if err != nil {
		return nil, 0, "", err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	} else {
		auth := c.Username + ":" + c.Password
		token := base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Authorization", "Basic " + token)
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, respErr := client.Do(req)

	if respErr != nil {
		return nil, 0, "", respErr
	}

	respBody, bodyErr := ioutil.ReadAll(res.Body)
	return respBody, res.StatusCode, res.Status, bodyErr
}
