package confluent

import (
	"errors"
	"github.com/Shopify/sarama"
)

type KafkaClusterAdminFn interface {
	ListPartitionReassignments(topic string, partitions []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error)
	AlterPartitionReassignments(topic string, assignment [][]int32) error
}

type KafkaAdmin struct {
	Fn KafkaClusterAdminFn
}

type KClient interface {
	Controller() (*sarama.Broker, error)
	Config() *sarama.Config
	RefreshMetadata() error
	Partitions(topic string) ([]int32, error)
	Brokers() []*sarama.Broker
	Replicas(topic string, partitionId int32) ([]int32, error)
	Client() sarama.Client
	NewClusterAdminFromClient() (*KafkaAdmin, error)
	ID(broker *sarama.Broker) int32
}

type ClusterAdmin struct {
	adminClient         sarama.ClusterAdmin
}

func (ca *ClusterAdmin) ListPartitionReassignments(topic string, partitions []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error) {
	return ca.adminClient.ListPartitionReassignments(topic, partitions)
}

func (ca *ClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return ca.adminClient.AlterPartitionReassignments(topic, assignment)
}

func (k *SaramaClient) Client() sarama.Client {
	return k.client
}

func (k *SaramaClient) Replicas(topic string, partitionId int32) ([]int32, error) {
	return k.client.Replicas(topic, partitionId)
}

func (k *SaramaClient) Controller() (*sarama.Broker, error) {
	return k.client.Controller()
}

func (k *SaramaClient) Config() *sarama.Config {
	return k.client.Config()
}

func (k *SaramaClient) RefreshMetadata() error {
	return k.client.RefreshMetadata()
}

func (k *SaramaClient) Partitions(topic string) ([]int32, error) {
	return k.client.Partitions(topic)
}

func (k *SaramaClient) Brokers() []*sarama.Broker {
	return k.client.Brokers()
}

func (k *SaramaClient) ID(broker *sarama.Broker) int32 {
	return broker.ID()
}

func (k *SaramaClient) NewClusterAdminFromClient() (*KafkaAdmin, error) {
	a, err := sarama.NewClusterAdminFromClient(k.client)
	if err != nil {
		return nil, err
	}
	admin := ClusterAdmin{
		adminClient: a,
	}
	return &KafkaAdmin{
		Fn: &admin,
	}, nil
}

// Init kafka client point to sarama client
func NewSaramaClient(config *Config) (*SaramaClient, error) {
	if config == nil {
		return nil, errors.New("Cannot create client without kafka config")
	}

	if config.BootstrapServers == nil {
		return nil, errors.New("No bootstrap_servers provided")
	}

	kc, err := config.newKafkaConfig()
	if err != nil {
		return nil, err
	}

	bootstrapServers := *(config.BootstrapServers)
	c, err := sarama.NewClient(bootstrapServers, kc)
	if err != nil {
		return nil, err
	}

	kafkaClient := &SaramaClient{
		client:      c,
		config:      config,
		kafkaConfig: kc,
	}

	err = kafkaClient.populateAPIVersions()
	if err != nil {
		return kafkaClient, err
	}

	err = kafkaClient.extractTopics()

	return kafkaClient, err
}
