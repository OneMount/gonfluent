package confluent

const (
	clientVersion = "0.1"
	userAgent     = "confluent-client-go-sdk-" + clientVersion
)

type Client struct {
	httpClient   HttpClient
	saramaClient KClient
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

func NewClient(httpClient HttpClient, kClient KClient) *Client {
	return &Client{httpClient: httpClient, saramaClient: kClient}
}
