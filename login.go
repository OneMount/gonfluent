package confluent

import (
	"encoding/json"
)

type Authenticate struct {
	AuthToken string `json:"auth_token"`
	TokenType string `json:"token_type"`
	ExpiresIn int    `json:"expires_in"`
}

type AuthenticateError struct {
	StatusCode int    `json:"status_code"`
	ErrrorCode int    `json:"errror_code"`
	Type       string `json:"type"`
	Message    string `json:"message"`
	Errors     []struct {
		ErrorType string `json:"error_type"`
		Message   string `json:"message"`
	} `json:"errors"`
}

func (c *Client) Login() (string, error) {
	u := "/security/1.0/authenticate"
	authenReq, err := c.DoRequest("GET", u, nil)
	if err != nil {
		return "", err
	}
	var authenticate *Authenticate
	err = json.Unmarshal(authenReq, &authenticate)
	if err != nil {
		return "", err
	}
	return authenticate.AuthToken, nil
}
