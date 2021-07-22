package confluent

import (
	"bytes"
	"encoding/json"
)

const (
	authorPath = "/security/1.0/authorize"
)

type AuthorClusters struct {
	KafkaCluster string `json:"kafka-cluster"`
}

type Scope struct {
	Clusters AuthorClusters `json:"clusters"`
}

type UserPrincipalAction struct {
	Scope        Scope  `json:"scope"`
	ResourceName string `json:"resourceName"`
	ResourceType string `json:"resourceType"`
	Operation    string `json:"operation"`
}

type UserPrincipal struct {
	// UserPrincipal example: User:<Username>
	UserPrincipal string `json:"userPrincipal"`

	// Actions allow or deny for this principal
	Actions []UserPrincipalAction `json:"actions"`
}

func (c *Client) CreatePrincipal(userPrincipal string, principals []UserPrincipalAction) (*UserPrincipal, error) {
	u := authorPath

	principal := &UserPrincipal{
		UserPrincipal: userPrincipal,
		Actions:       principals,
	}

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(principal)

	_, err := c.DoRequest("PUT", u, payloadBuf)
	if err != nil {
		return nil, err
	}
	return principal, nil
}
