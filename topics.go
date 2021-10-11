package confluent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"math/rand"
	"time"
)

const (
	topicPath = "topics"
)

type relatedTopic struct {
	ClusterID              string  `json:"cluster_id,omitempty"`
	TopicName              string  `json:"topic_name,omitempty"`
	IsInternal             bool    `json:"is_internal,omitempty"`
	ReplicationFactor      int16   `json:"replication_factor,omitempty"`
	Partitions             Related `json:"partitions,omitempty"`
	Configs                Related `json:"configs,omitempty"`
	PartitionReassignments Related `json:"partition_reassignments,omitempty"`
}

type ReplicasAssignment struct {
	ParitionId int   `json:"partition_id,omitempty"`
	BrokerIds  []int `json:"broker_ids,omitempty"`
}

type Topic struct {
	ClusterID           string               `json:"cluster_id,omitempty"`
	IsInternal          bool                 `json:"is_internal,omitempty"`
	Name                string               `json:"topic_name"`
	Partitions          int32                `json:"partitions_count,omitempty"`
	ReplicationFactor   int16                `json:"replication_factor,omitempty"`
	Config              []TopicConfig        `json:"configs,omitempty"`
	ReplicasAssignments []ReplicasAssignment `json:"replicas_assignments,omitempty"`
	PartitionsDetails   []Partition          `json:"partitions_details,omitempty"`
}

func (c *Client) DoRequest(method string, uri string, reqBody io.Reader) ([]byte, error) {
	respBody, statusCode, status, err := c.httpClient.DoRequest(method, uri, reqBody)
	if err != nil {
		return respBody, err
	}
	if statusCode > 204 {
		var errorBody *ErrorResponse
		err = json.Unmarshal(respBody, &errorBody)
		if err != nil {
			return nil, errors.New("error with status: " + status)
		}

		if errorBody.Errors != nil {
			return nil, errors.New("error with status: " + status + " " + errorBody.Errors[0].Message)
		}
		return nil, errors.New("error with status: " + status + " " + errorBody.Message)
	}
	return respBody, nil
}

func (c *Client) ListTopics(clusterId string) ([]Topic, error) {
	uri := "/kafka/v3/clusters/" + clusterId + "/" + topicPath
	r, err := c.DoRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	body := struct {
		Data []relatedTopic `json:"data"`
	}{}

	err = json.Unmarshal(r, &body)
	if err != nil {
		return nil, err
	}
	var topics []Topic
	for _, v := range body.Data {
		topics = append(topics, Topic{
			ClusterID:         v.ClusterID,
			IsInternal:        v.IsInternal,
			Name:              v.TopicName,
			ReplicationFactor: v.ReplicationFactor,
		})
	}

	return topics, nil
}

func (c *Client) GetTopic(clusterId, topicName string) (*Topic, error) {
	uri := "/kafka/v3/clusters/" + clusterId + "/" + topicPath + "/" + topicName
	r, err := c.DoRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	body := relatedTopic{}

	err = json.Unmarshal(r, &body)
	if err != nil {
		return nil, err
	}
	topic := Topic{
		ClusterID:         body.ClusterID,
		Name:              body.TopicName,
		IsInternal:        body.IsInternal,
		ReplicationFactor: body.ReplicationFactor,
	}

	p, err := c.GetTopicPartitions(clusterId, topicName)
	if err != nil {
		return nil, err
	}
	topic.Partitions = int32(len(p))
	topic.PartitionsDetails = p

	config, err := c.GetTopicConfigs(clusterId, topicName)
	if err != nil {
		return nil, err
	}
	topic.Config = config

	return &topic, nil
}

func (c *Client) CreateTopic(clusterId, topicName string, partitionsCount, replicationFactor int, configs []TopicConfig, replicasAssignments []ReplicasAssignment) error {
	uri := "/kafka/v3/clusters/" + clusterId + "/" + topicPath
	topicConfig := &Topic{
		Name:                topicName,
		Partitions:          int32(partitionsCount),
		ReplicationFactor:   int16(replicationFactor),
		Config:              configs,
		ReplicasAssignments: replicasAssignments,
	}
	payloadBuf := new(bytes.Buffer)
	err := json.NewEncoder(payloadBuf).Encode(topicConfig)
	if err != nil {
		return err
	}

	_, err = c.DoRequest("POST", uri, payloadBuf)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) DeleteTopic(clusterId, topicName string) error {
	uri := "/kafka/v3/clusters/" + clusterId + "/" + topicPath + "/" + topicName
	_, err := c.DoRequest("DELETE", uri, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) UpdatePartitions(t Topic) error {
	broker, err := c.saramaClient.Controller()
	if err != nil {
		return err
	}

	timeout := time.Duration(c.saramaClient.Config().Admin.Timeout) * time.Second
	tp := map[string]*sarama.TopicPartition{
		t.Name: &sarama.TopicPartition{
			Count: int32(t.Partitions),
		},
	}

	req := &sarama.CreatePartitionsRequest{
		TopicPartitions: tp,
		Timeout:         timeout,
		ValidateOnly:    false,
	}
	res, err := broker.CreatePartitions(req)
	if err == nil {
		for _, e := range res.TopicPartitionErrors {
			if e.Err != sarama.ErrNoError {
				return e.Err
			}
		}
	}

	return err
}

func (c *Client) UpdateReplicationsFactor(t Topic) error {
	if err := c.saramaClient.RefreshMetadata(); err != nil {
		return err
	}
	assignment, err := c.buildAssignment(t)
	if err != nil {
		return err
	}
	return c.saramaClusterAdmin.AlterPartitionReassignments(t.Name, *assignment)
}

func (c *Client) IsReplicationFactorUpdating(topic string) (bool, error) {
	if err := c.saramaClient.RefreshMetadata(); err != nil {
		return false, err
	}

	partitions, err := c.saramaClient.Partitions(topic)
	if err != nil {
		return false, err
	}

	statusMap, err := c.saramaClusterAdmin.ListPartitionReassignments(topic, partitions)
	if err != nil {
		return false, err
	}

	for _, status := range statusMap[topic] {
		if isPartitionRFChanging(status) {
			return true, nil
		}
	}

	return false, nil
}

func (c *Client) allReplicas() *[]int32 {
	brokers := c.saramaClient.Brokers()
	replicas := make([]int32, 0, len(brokers))

	for _, b := range brokers {
		id := c.saramaClient.ID(b)
		fmt.Println(id)
		if id != -1 {
			replicas = append(replicas, id)
		}
	}

	return &replicas
}

func (c *Client) buildAssignment(t Topic) (*[][]int32, error) {
	partitions, err := c.saramaClient.Partitions(t.Name)
	if err != nil {
		return nil, err
	}

	allReplicas := c.allReplicas()
	newRF := t.ReplicationFactor
	rand.Seed(time.Now().UnixNano())

	assignment := make([][]int32, len(partitions))
	for _, p := range partitions {
		oldReplicas, err := c.saramaClient.Replicas(t.Name, p)
		if err != nil {
			return &assignment, err
		}

		oldRF := int16(len(oldReplicas))
		deltaRF := int16(newRF) - oldRF
		newReplicas, err := buildNewReplicas(allReplicas, &oldReplicas, deltaRF)
		if err != nil {
			return &assignment, err
		}

		assignment[p] = *newReplicas
	}

	return &assignment, nil
}

func isPartitionRFChanging(status *sarama.PartitionReplicaReassignmentsStatus) bool {
	return len(status.AddingReplicas) != 0 || len(status.RemovingReplicas) != 0
}

func buildNewReplicas(allReplicas *[]int32, usedReplicas *[]int32, deltaRF int16) (*[]int32, error) {
	usedCount := int16(len(*usedReplicas))

	if deltaRF == 0 {
		return usedReplicas, nil
	} else if deltaRF < 0 {
		end := usedCount + deltaRF
		if end < 1 {
			return nil, errors.New("dropping too many replicas")
		}

		head := (*usedReplicas)[:end]
		return &head, nil
	} else {
		extraCount := int16(len(*allReplicas)) - usedCount
		if extraCount < deltaRF {
			return nil, errors.New("not enough brokers")
		}

		unusedReplicas := *findUnusedReplicas(allReplicas, usedReplicas, extraCount)
		newReplicas := *usedReplicas
		for i := int16(0); i < deltaRF; i++ {
			j := rand.Intn(len(unusedReplicas))
			newReplicas = append(newReplicas, unusedReplicas[j])
			unusedReplicas[j] = unusedReplicas[len(unusedReplicas)-1]
			unusedReplicas = unusedReplicas[:len(unusedReplicas)-1]
		}

		return &newReplicas, nil
	}
}

func findUnusedReplicas(allReplicas *[]int32, usedReplicas *[]int32, extraCount int16) *[]int32 {
	usedMap := make(map[int32]bool, len(*usedReplicas))
	for _, r := range *usedReplicas {
		usedMap[r] = true
	}

	unusedReplicas := make([]int32, 0, extraCount)
	for _, r := range *allReplicas {
		_, exists := usedMap[r]
		if !exists {
			unusedReplicas = append(unusedReplicas, r)
		}
	}

	return &unusedReplicas
}
