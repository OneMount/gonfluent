package confluent

import (
	"errors"
	"github.com/Shopify/sarama"
	"log"
)

type Config struct {
	BootstrapServers        *[]string
	Timeout                 int
	CACert                  string
	ClientCert              string
	ClientCertKey           string
	ClientCertKeyPassphrase string
	TLSEnabled              bool
	SkipTLSVerify           bool
	SASLUsername            string
	SASLPassword            string
	SASLMechanism           string
}

type void struct{}

var member void

type SaramaClient struct {
	client        sarama.Client
	kafkaConfig   *sarama.Config
	config        *Config
	supportedAPIs map[int]int
	topics        map[string]void
}

func (k *SaramaClient) extractTopics() error {
	topics, err := k.client.Topics()
	if err != nil {
		return err
	}
	k.topics = make(map[string]void)
	for _, t := range topics {
		k.topics[t] = member
	}
	return nil
}

func (k *SaramaClient) populateAPIVersions() error {
	ch := make(chan []*sarama.ApiVersionsResponseBlock)
	errCh := make(chan error)

	brokers := k.client.Brokers()
	kafkaConfig := k.kafkaConfig
	for _, broker := range brokers {
		go apiVersionsFromBroker(broker, kafkaConfig, ch, errCh)
	}

	clusterApiVersions := make(map[int][2]int) // valid api version intervals across all brokers
	errs := make([]error, 0)
	for i := 0; i < len(brokers); i++ {
		select {
		case brokerApiVersions := <-ch:
			updateClusterApiVersions(&clusterApiVersions, brokerApiVersions)
		case err := <-errCh:
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errors.New(sarama.MultiError{Errors: &errs}.PrettyError())
	}

	k.supportedAPIs = make(map[int]int, len(clusterApiVersions))
	for apiKey, versionMinMax := range clusterApiVersions {
		versionMin := versionMinMax[0]
		versionMax := versionMinMax[1]

		if versionMax >= versionMin {
			k.supportedAPIs[apiKey] = versionMax
		}
	}

	return nil
}

func updateClusterApiVersions(clusterApiVersions *map[int][2]int, brokerApiVersions []*sarama.ApiVersionsResponseBlock) {
	cluster := *clusterApiVersions

	for _, apiBlock := range brokerApiVersions {
		apiKey := int(apiBlock.ApiKey)
		brokerMin := int(apiBlock.MinVersion)
		brokerMax := int(apiBlock.MaxVersion)

		clusterMinMax, exists := cluster[apiKey]
		if !exists {
			cluster[apiKey] = [2]int{brokerMin, brokerMax}
		} else {
			// shrink the cluster interval according to
			// the broker interval

			clusterMin := clusterMinMax[0]
			clusterMax := clusterMinMax[1]

			if brokerMin > clusterMin {
				clusterMinMax[0] = brokerMin
			}

			if brokerMax < clusterMax {
				clusterMinMax[1] = brokerMax
			}

			cluster[apiKey] = clusterMinMax
		}
	}
}

func apiVersionsFromBroker(broker *sarama.Broker, config *sarama.Config, ch chan<- []*sarama.ApiVersionsResponseBlock, errCh chan<- error) {
	resp, err := rawApiVersionsRequest(broker, config)

	if err != nil {
		errCh <- err
	} else if resp.Err != sarama.ErrNoError {
		errCh <- errors.New(resp.Err.Error())
	} else {
		ch <- resp.ApiVersions
	}
}

func rawApiVersionsRequest(broker *sarama.Broker, config *sarama.Config) (*sarama.ApiVersionsResponse, error) {
	if err := broker.Open(config); err != nil && err != sarama.ErrAlreadyConnected {
		return nil, err
	}

	defer func() {
		if err := broker.Close(); err != nil && err != sarama.ErrNotConnected {
			log.Fatal(err)
		}
	}()

	return broker.ApiVersions(&sarama.ApiVersionsRequest{})
}
