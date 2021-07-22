package main

import (
	"fmt"
	"os"

	confluent "github.com/wayarmy/gonfluent"
)

const (
	ClientVersion = "0.1"
	UserAgent     = "confluent-client-go-sdk-" + ClientVersion
)

func main() {
	baseUrl := "https://localhost:8090"

	username := os.Getenv("CONFLUENT_USER")
	password := os.Getenv("CONFLUENT_PASSWORD")

	// Initialize the client
	httpClient := confluent.NewDefaultHttpClient(baseUrl, username, password)
	httpClient.UserAgent = UserAgent
	bootstrapServer := []string{
		"localhost:9093",
	}
	kConfig := &confluent.Config{
		BootstrapServers: &bootstrapServer,
		CACert:           "certs/ca.pem",
		ClientCert:       "certs/cert.pem",
		ClientCertKey:    "certs/key.pem",
		SkipTLSVerify:    true,
		SASLMechanism:    "plain",
		TLSEnabled:       true,
		Timeout:          120,
	}
	kClient, err := confluent.NewSaramaClient(kConfig)
	if err != nil {
		panic(err)
	}

	client := confluent.NewClient(httpClient, kClient)
	bearerToken, err := client.Login()
	if err != nil {
		panic(err)
	}
	httpClient.Token = bearerToken

	// Get the list of clusters in Confluent platform
	listCluster, err := client.ListKafkaCluster()
	if err != nil {
		panic(err)
	}

	fmt.Println(listCluster)

	// Get the cluster information
	clusterId := listCluster[0].ClusterID
	cluster, err := client.GetKafkaCluster(clusterId)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v", cluster)
	fmt.Println(cluster)
	//
	//topicConfig, err := client.GetTopicConfigs(clusterId, "example_topic_name")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("%#v", topicConfig)

	//topic, err := client.GetTopic(clusterId, "test7-terraform-confluent-provider")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(topic.Partitions)

	////Create a new topic
	//partitionConfig := []confluent.TopicConfig{
	//	{
	//		Name:  "compression.type",
	//		Value: "gzip",
	//	},
	//	{
	//		Name:  "cleanup.policy",
	//		Value: "compact",
	//	},
	//	//{
	//	//	Name:  "retention.ms",
	//	//	Value: "20000",
	//	//},
	//}
	//err = client.CreateTopic(clusterId, "test_topic_name", 3, 3, partitionConfig, nil)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("Topic Created!")

	d := []confluent.TopicConfig{
		{
			Name:  "compression.type",
			Value: "gzip",
		},
		{
			Name:  "cleanup.policy",
			Value: "compact",
		},
		{
			Name:  "retention.ms",
			Value: "300000",
		},
	}

	err = client.UpdateTopicConfigs(clusterId, "test_topic_name", d)
	if err != nil {
		panic(err)
	}
	fmt.Println("Topic Updated!")
	//

	// // Create Principal but don't know use case for it
	// testPrincipals := []confluent.UserPrincipalAction{
	// 	{
	// 		Scope: confluent.Scope{
	// 			Clusters: confluent.AuthorClusters{
	// 				KafkaCluster: clusterId,
	// 			},
	// 		},
	// 		ResourceName: "Testing-Principal",
	// 		ResourceType: "Cluster",
	// 		Operation:    "Read",
	// 	},
	// }

	// newPrincipal, err := client.CreatePrincipal("User:system-platform", testPrincipals)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("%s", newPrincipal)

	//

	// Get the topics information
	//getTopic, err := client.GetTopic(clusterId, "test2_topic_name")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("%v", getTopic)

	//Delete the existing topic
	//deleteTopic := client.DeleteTopic(clusterId, "test_topic_name")
	//if deleteTopic != nil {
	//	panic(deleteTopic)
	//}
	//fmt.Println("Topic deleted")

	//// Add role assignment
	//c := confluent.ClusterDetails{}
	//c.Clusters.KafkaCluster = clusterId
	//err = client.BindPrincipalToRole("User:manh.do", "Operator", c)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("Role Binded!")

	// // Add role assignment for principal to topic
	// r := confluent.UpdateRoleBinding{
	// 	Scope: c,
	// }
	// r.ResourcePatterns = []confluent.RoleBindings{
	// 	{
	// 		ResourceType: "Topic",
	// 		Name:         "system-platform",
	// 		PatternType:  "PREFIXED",
	// 	},
	// }
	// principalCN := "User:CN=common-name.example.com"
	// err = client.IncreaseRoleBinding(principalCN, "ResourceOwner", r)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Role Binded!")
	//t := confluent.Topic{
	//	Name: "test_topic_name",
	//	Partitions: 10,
	//	ReplicationFactor: 4,
	//}
	//err = client.UpdatePartitions(t)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("Update partition successfully")
	//err = client.UpdateReplicationsFactor(t)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("Update RF successfully")

}
