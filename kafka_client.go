package confluent

import (
	"errors"
	"github.com/Shopify/sarama"
)

type SaramaClusterAdmin interface {
	ListPartitionReassignments(topic string, partitions []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error)
	AlterPartitionReassignments(topic string, assignment [][]int32) error
}

type SaramaClient interface {
	Controller() (*sarama.Broker, error)
	Config() *sarama.Config
	RefreshMetadata() error
	Partitions(topic string) ([]int32, error)
	Brokers() []*sarama.Broker
	Replicas(topic string, partitionId int32) ([]int32, error)
	ID(broker *sarama.Broker) int32
}

type DefaultSaramaClusterAdmin struct {
	adminClient         sarama.ClusterAdmin
}

func (ca *DefaultSaramaClusterAdmin) ListPartitionReassignments(topic string, partitions []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error) {
	return ca.adminClient.ListPartitionReassignments(topic, partitions)
}

func (ca *DefaultSaramaClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return ca.adminClient.AlterPartitionReassignments(topic, assignment)
}

func (k *DefaultSaramaClient) Replicas(topic string, partitionId int32) ([]int32, error) {
	return k.client.Replicas(topic, partitionId)
}

func (k *DefaultSaramaClient) Controller() (*sarama.Broker, error) {
	return k.client.Controller()
}

func (k *DefaultSaramaClient) Config() *sarama.Config {
	return k.client.Config()
}

func (k *DefaultSaramaClient) RefreshMetadata() error {
	return k.client.RefreshMetadata()
}

func (k *DefaultSaramaClient) Partitions(topic string) ([]int32, error) {
	return k.client.Partitions(topic)
}

func (k *DefaultSaramaClient) Brokers() []*sarama.Broker {
	return k.client.Brokers()
}

func (k *DefaultSaramaClient) ID(broker *sarama.Broker) int32 {
	return broker.ID()
}

func NewDefaultSaramaClusterAdmin(saramaClient sarama.Client) (SaramaClusterAdmin, error) {
	a, err := sarama.NewClusterAdminFromClient(saramaClient)
	if err != nil {
		return nil, err
	}
	admin := DefaultSaramaClusterAdmin{
		adminClient: a,
	}
	return &admin, nil
}

// Init kafka client point to sarama client
func NewDefaultSaramaClient(config *Config) (*DefaultSaramaClient, sarama.Client, error) {
	if config == nil {
		return nil, nil, errors.New("Cannot create client without kafka config")
	}

	if config.BootstrapServers == nil {
		return nil, nil, errors.New("No bootstrap_servers provided")
	}

	kc, err := config.newKafkaConfig()
	if err != nil {
		return nil, nil, err
	}

	bootstrapServers := *(config.BootstrapServers)
	c, err := sarama.NewClient(bootstrapServers, kc)
	if err != nil {
		return nil, nil, err
	}

	kafkaClient := &DefaultSaramaClient{
		client:      c,
		config:      config,
		kafkaConfig: kc,
	}

	err = kafkaClient.populateAPIVersions()
	if err != nil {
		return kafkaClient, c, err
	}

	err = kafkaClient.extractTopics()

	return kafkaClient, c, err
}
