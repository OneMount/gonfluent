package confluent

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func (co *Config) saslEnabled() bool {
	return co.SASLUsername != "" || co.SASLPassword != ""
}

func (co *Config) newKafkaConfig() (*sarama.Config, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_4_0_0
	kafkaConfig.ClientID = "confluent-go-client"
	kafkaConfig.Admin.Timeout = time.Duration(co.Timeout) * time.Second
	kafkaConfig.Metadata.Full = true // the default, but just being clear

	if co.saslEnabled() {
		switch co.SASLMechanism {
		case "scram-sha512":
		case "scram-sha256":
		case "plain":
		default:
			return nil, errors.New("[ERROR] Invalid sasl mechanism \"%s\": can only be \"scram-sha256\", \"scram-sha512\" or \"plain\"" + co.SASLMechanism)
		}
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Password = co.SASLPassword
		kafkaConfig.Net.SASL.User = co.SASLUsername
		kafkaConfig.Net.SASL.Handshake = true
	} else {
		log.Printf("[WARN] SASL disabled username: '%s', password '%s'", co.SASLUsername, "****")
	}

	if co.TLSEnabled {
		tlsConfig, err := newTLSConfig(
			co.ClientCert,
			co.ClientCertKey,
			co.CACert,
			co.ClientCertKeyPassphrase,
		)

		if err != nil {
			return kafkaConfig, err
		}
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = tlsConfig
		kafkaConfig.Net.TLS.Config.InsecureSkipVerify = co.SkipTLSVerify
	}

	return kafkaConfig, nil
}

//func NewTLSConfig(clientCert, clientKey, caCert, clientKeyPassphrase string) (*tls.Config, error) {
//	return newTLSConfig(clientCert, clientKey, caCert, clientKeyPassphrase)
//}

func parsePemOrLoadFromFile(input string) (*pem.Block, []byte, error) {
	// attempt to parse
	var inputBytes = []byte(input)
	inputBlock, _ := pem.Decode(inputBytes)

	if inputBlock == nil {
		//attempt to load from file
		log.Printf("[INFO] Attempting to load from file '%s'", input)
		var err error
		inputBytes, err = ioutil.ReadFile(input)
		if err != nil {
			return nil, nil, err
		}
		inputBlock, _ = pem.Decode(inputBytes)
		if inputBlock == nil {
			return nil, nil, fmt.Errorf("[ERROR] Error unable to decode pem")
		}
	}
	return inputBlock, inputBytes, nil
}

func newTLSConfig(clientCert, clientKey, caCert, clientKeyPassphrase string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	if clientCert != "" && clientKey != "" {
		keyBlock, keyBytes, err := parsePemOrLoadFromFile(clientKey)
		if err != nil {
			return &tlsConfig, err
		}

		keyBytes = pem.EncodeToMemory(&pem.Block{
			Type:  keyBlock.Type,
			Bytes: keyBytes,
		})

		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return &tlsConfig, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if caCert == "" {
		return &tlsConfig, nil
	}

	caCertPool, _ := x509.SystemCertPool()
	if caCertPool == nil {
		caCertPool = x509.NewCertPool()
	}

	_, caBytes, err := parsePemOrLoadFromFile(caCert)
	if err != nil {
		return &tlsConfig, err
	}
	ok := caCertPool.AppendCertsFromPEM(caBytes)
	if !ok {
		return &tlsConfig, errors.New("couldn't add the caPem")
	}

	tlsConfig.RootCAs = caCertPool
	return &tlsConfig, nil
}

func (co *Config) copyWithMaskedSensitiveValues() Config {
	c := Config{
		co.BootstrapServers,
		co.Timeout,
		co.CACert,
		co.ClientCert,
		"*****",
		"*****",
		co.TLSEnabled,
		co.SkipTLSVerify,
		co.SASLUsername,
		"*****",
		co.SASLMechanism,
	}
	return c
}
