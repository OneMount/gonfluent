package confluent

import (
	"bytes"
	"encoding/json"
)

const (
	aclsPath = "acls"
)

type Acl struct {
	ClusterId    string `json:"cluster_id,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`
	PatternType  string `json:"pattern_type,omitempty"`
	Principal    string `json:"principal,omitempty"`
	Host         string `json:"host,omitempty"`
	Operation    string `json:"operation,omitempty"`
	Permission   string `json:"permission,omitempty"`
}

// Returns a list of ACLs that match the search criteria.
// Parameters:
//    cluster_id (string) – The Kafka cluster ID.
// Query Parameters:
//    resource_type (string) – The ACL resource type.
//    resource_name (string) – The ACL resource name.
//    pattern_type (string) – The ACL pattern type.
//    principal (string) – The ACL principal.
//    host (string) – The ACL host.
//    operation (string) – The ACL operation.
//    permission (string) – The ACL permission.
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#get--clusters-cluster_id-acls
func (c *Client) ListAcls(clusterId string) ([]Acl, error) {
	u := "/clusters/" + clusterId + "/" + aclsPath
	r, err := c.DoRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	body := struct{
		Data []Acl `json:"data"`
	}{}

	err = json.Unmarshal(r, &body)
	if err != nil {
		return nil, err
	}

	return body.Data, nil
}

// Creates an ACL.
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#post--clusters-cluster_id-acls
func (c *Client) CreateAcl(clusterId string, aclConfig *Acl) error {
	u := "/clusters/" + clusterId + "/" + aclsPath

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(aclConfig)
	_, err := c.DoRequest("POST", u, payloadBuf)
	if err != nil {
		return err
	}

	return nil
}

// Deletes the list of ACLs that matches the search criteria.
// Parameters:
//    cluster_id (string) – The Kafka cluster ID.
// Query Parameters:
//    resource_type (string) – The ACL resource type.
//    resource_name (string) – The ACL resource name.
//    pattern_type (string) – The ACL pattern type.
//    principal (string) – The ACL principal.
//    host (string) – The ACL host.
//    operation (string) – The ACL operation.
//    permission (string) – The ACL permission.
// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#delete--clusters-cluster_id-acls
func (c *Client) DeleteAcl(clusterId, resourceName string) error {
	u := "/clusters/" + clusterId + "/" + aclsPath

	aclDetele := Acl{
		ResourceName: resourceName,
	}

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(aclDetele)
	_, err := c.DoRequest("DELETE", u, payloadBuf)
	if err != nil {
		return err
	}

	return nil
}
