package confluent

import (
	"bytes"
	"encoding/json"
)

type TopicConfig struct {
	ClusterId   string     `json:"cluster_id,omitempty"`
	TopicName   string     `json:"topic_name,omitempty"`
	Name        string     `json:"name"`
	Value       string     `json:"value"`
	IsDefault   bool       `json:"is_default,omitempty"`
	IsReadOnly  bool       `json:"is_read_only,omitempty"`
	IsSensitive bool       `json:"is_sensitive,omitempty"`
	Source      string     `json:"source,omitempty"`
	Synonyms    []Synonyms `json:"synonyms,omitempty"`
}

type Synonyms struct {
	Name      string `json:"name"`
	Value     string `json:"value,omitempty"`
	Source    string `json:"source,omitempty"`
	Operation string `json:"operation,omitempty"`
}

// @ref https://docs.confluent.io/platform/current/kafka-rest/api.html#get--clusters-cluster_id-topics-topic_name-configs
// Return the list of configs that belong to the specified topic.
func (c *Client) GetTopicConfigs(clusterId string, topicName string) ([]TopicConfig, error) {
	u := "/kafka/v3/clusters/" + clusterId + "/topics/" + topicName + "/configs"

	r, err := c.DoRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	res  := struct{
		Data []TopicConfig `json:"data"`
	}{}
	err = json.Unmarshal(r, &res)
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

// @ref Return the list of configs that belong to the specified topic.
// Updates or deletes a set of topic configs.
func (c *Client) UpdateTopicConfigs(clusterId string, topicName string, data []TopicConfig) error {
	u := "/kafka/v3/clusters/" + clusterId + "/topics/" + topicName + "/configs:alter"

	reqBody := struct{
		Data []TopicConfig `json:"data"`
	}{}
	reqBody.Data = data

	payloadBuf := new(bytes.Buffer)
	json.NewEncoder(payloadBuf).Encode(reqBody)

	_, err := c.DoRequest("POST", u, payloadBuf)
	if err != nil {
		return err
	}
	return nil
}

