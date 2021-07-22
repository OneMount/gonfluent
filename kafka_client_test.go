package confluent

import (
	"github.com/Shopify/sarama"
	"testing"
)

var t *testing.T

type MockKafkaAdmin struct{
	TopicNameExpected string
	PartitionExpected int32
	AssignmentExpected [][]int32
}

func (mca *MockKafkaAdmin) ListPartitionReassignments(topic string, partitions []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error) {
	if topic != mca.TopicNameExpected {
		t.Fatal("not expected: ", sarama.ErrInvalidTopic)
	}
	return nil, nil
}

func (mca *MockKafkaAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return nil
}

type MockKafkaClient struct {
	MockBrokers *sarama.MockBroker
	MockVersion sarama.KafkaVersion
	client sarama.Client
	TopicNameExpected string
	PartitionExpected int32
	AssignmentExpected [][]int32
}

func (mk *MockKafkaClient) InitKafkaClient() sarama.Client {
	seedBroker := mk.MockBrokers
	client, err := sarama.NewClient([]string{seedBroker.Addr()}, mk.newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (mk *MockKafkaClient) newTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = mk.MockVersion
	return config
}

func (mk *MockKafkaClient) Client() sarama.Client {
	return mk.client
}

func (mk *MockKafkaClient) Replicas(topic string, partitionId int32) ([]int32, error) {
	if partitionId == 1 {
		return []int32{1}, nil
	}
	if partitionId == 2 {
		return []int32{2}, nil
	}
	return nil, nil
}

func (mk *MockKafkaClient) NewClusterAdminFromClient() (*KafkaAdmin, error) {
	return &KafkaAdmin{
		Fn: &MockKafkaAdmin{
			TopicNameExpected: mk.TopicNameExpected,
			PartitionExpected: mk.PartitionExpected,
			AssignmentExpected: mk.AssignmentExpected,
		},
	}, nil
}

func (mk *MockKafkaClient) Controller() (*sarama.Broker, error) {
	c := mk.InitKafkaClient()
	return c.Controller()
}

func (mk *MockKafkaClient) Config() *sarama.Config {
	c := mk.InitKafkaClient()
	return c.Config()
}

func (mk *MockKafkaClient) RefreshMetadata() error {
	return nil
}

func (mk *MockKafkaClient) Partitions(topic string) ([]int32, error) {
	return []int32{1,2}, nil
}

func (mk *MockKafkaClient) Brokers() []*sarama.Broker {
	return nil
}

func (mk *MockKafkaClient) ID(broker *sarama.Broker) int32 {
	return 3
}
