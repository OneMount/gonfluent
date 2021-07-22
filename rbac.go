package confluent

import (
	"bytes"
	"encoding/json"
)

const (
	principalPath = "/security/1.0/principals/"
)

type ClusterDetails struct {
	ClusterName string             `json:"clusterName,omitempty"`
	Clusters    Clusters `json:"clusters"`
}

type ResourcePattern struct {
	ResourceType string `json:"resourceType,omitempty"`
	Name         string `json:"name,omitempty"`
	PatternType  string `json:"patternType,omitempty"`
}

type RoleBinding struct {
	Scope            ClusterDetails `json:"scope"`
	ResourcePatterns []ResourcePattern `json:"resourcePatterns"`
}

// BindPrincipalToRole will bind the principal to a cluster-scoped role for a specific cluster or in a given scope
func (c *Client) BindPrincipalToRole(principal, roleName string, cDetails ClusterDetails) error {
	u := principalPath + principal + "/roles/" + roleName

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(cDetails)

	_, err := c.DoRequest("POST", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}

// DeleteRoleBinding remove the role (cluster or resource scoped) from the principal at the give scope/cluster
func (c *Client) DeleteRoleBinding(principal, roleName string, cDetails ClusterDetails) error {
	u := principalPath + principal + "/roles/" + roleName

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(cDetails)

	_, err := c.DoRequest("DELETE", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}

// LookupRoleBinding will lookup the role-bindings for the principal at the given scope/cluster using the given role
func (c *Client) LookupRoleBinding(principal, roleName string, cDetails ClusterDetails) ([]ResourcePattern, error) {
	u := principalPath + principal + "/roles/" + roleName + "/resources"

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(cDetails)

	r, err := c.DoRequest("POST", u, payloadBuf)
	if err != nil {
		return nil, err
	}

	var patterns []ResourcePattern

	err = json.Unmarshal(r, &patterns)
	if err != nil {
		return nil, err
	}
	return patterns, nil
}

// IncreaseRoleBinding : incrementally grant the resources to the principal at the given scope/cluster using the given role
func (c *Client) IncreaseRoleBinding(principal, roleName string, uRoleBinding RoleBinding) error {
	u := principalPath + principal + "/roles/" + roleName + "/bindings"

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(uRoleBinding)

	_, err := c.DoRequest("POST", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}

// DecreaseRoleBinding : Incrementally remove the resources from the principal at the given scope/cluster using the given role
func (c *Client) DecreaseRoleBinding(principal, roleName string, uRoleBinding RoleBinding) error {
	u := principalPath + principal + "/roles/" + roleName + "/bindings"

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(uRoleBinding)

	_, err := c.DoRequest("DELETE", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}

// OverwriteRoleBinding will overwrite existing resource grants
func (c *Client) OverwriteRoleBinding(principal, roleName string, uRoleBinding RoleBinding) error {
	u := principalPath + principal + "/roles/" + roleName + "/bindings"

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(uRoleBinding)

	_, err := c.DoRequest("PUT", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}
