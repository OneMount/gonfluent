package confluent

import "encoding/json"

type Partition struct {
	ClusterID   string `json:"cluster_id"`
	TopicName   string `json:"topic_name"`
	PartitionId int    `json:"partition_id"`
}

func (c *Client) GetTopicPartitions(clusterId, topicName string) ([]Partition, error) {
	u := "/kafka/v3/clusters/"+clusterId+"/topics/"+topicName+"/partitions"
	r, err := c.DoRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	body := struct{
		Data []Partition `json:"data"`
	}{}

	err = json.Unmarshal(r, &body)
	if err != nil {
		return nil, err
	}
	return body.Data, nil
}
