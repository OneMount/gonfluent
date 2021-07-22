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
