package confluent

const (
	clientVersion = "0.1"
	userAgent     = "confluent-client-go-sdk-" + clientVersion
)

/*
Client will provide all Confluent clients
*/
type Client struct {
	//httpClient provide http client to connect to confluent rest API
	httpClient   HttpClient

	//saramaClient provider Kafka client to connect to Kafka brokers
	// The reason why we need sarama client working along with http client is : Confluent provide all Kafka Feature without Kafka admin features
	// So that we need to init new Kafka Client to use some functions of Kafka:
	// - Update replications factors.
	// - Update partitions
	saramaClient SaramaClient

	//saramaClient provider Kafka Admin client to connect to Kafka brokers, init and release from Sarama client.
	saramaClusterAdmin SaramaClusterAdmin
}

type ErrorResponse struct {
	StatusCode int    `json:"status_code,omitempty"`
	ErrorCode  int    `json:"error_code"`
	Type       string `json:"type,omitempty"`
	Message    string `json:"message"`
	Errors     []struct {
		ErrorType string `json:"error_type,omitempty"`
		Message   string `json:"message,omitempty"`
	} `json:"errors,omitempty"`
}

type Metadata struct {
	Self         string `json:"self"`
	ResourceName string `json:"resource_name,omitempty"`
	Next         string `json:"next,omitempty"`
}

func NewClient(httpClient HttpClient, saramaClient SaramaClient, saramaClusterAdmin SaramaClusterAdmin) *Client {
	return &Client{
		httpClient: httpClient,
		saramaClient: saramaClient,
		saramaClusterAdmin: saramaClusterAdmin,
	}
}
