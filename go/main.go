package main

import "C"

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"math"

	yaml "gopkg.in/yaml.v2"

	"github.com/BurntSushi/toml"
	"github.com/zeebe-io/zbc-go/zbc"
	"github.com/zeebe-io/zbc-go/zbc/sbe"
)

const (
	version              = "0.1.0-alpha1"
	defaultConfiguration = "/etc/zeebe/config.toml"
)

var (
	errResourceNotFound = errors.New("Resource at the given path not found")
)

func isFatal(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

type contact struct {
	Address string `toml:"address"`
	Port    string `toml:"port"`
}

func (c *contact) String() string {
	return fmt.Sprintf("%s:%s", c.Address, c.Port)
}

type config struct {
	Version string  `toml:"version"`
	Broker  contact `toml:"broker"`
}

func (cf *config) String() string {
	return fmt.Sprintf("version: %s\tBroker: %s", cf.Version, cf.Broker.String())

}


func sendTask(client *zbc.Client, topic string, m *zbc.Task) (*zbc.Message, error) {
	commandRequest := zbc.NewTaskMessage(&sbe.ExecuteCommandRequest{
		PartitionId: 0,
		Position:    0,
		Key:         0,
		TopicName:   []uint8("default-topic"),
		Command:     []uint8{},
	}, m)

	return sendRequest(client, commandRequest)
}

func sendWorkflowInstance(client *zbc.Client, topic string, m *zbc.WorkflowInstance) (*zbc.Message, error) {
	cmdReq := &sbe.ExecuteCommandRequest{
		PartitionId: 0,
		Position:    0,
		Key:         math.MaxUint64,
		TopicName:   []uint8(topic),
		Command:     []uint8{},
	}
	cmdReq.Key = cmdReq.KeyNullValue()
	commandRequest := zbc.NewWorkflowMessage(cmdReq, m)

	return sendRequest(client, commandRequest)
}

func sendDeployment(client *zbc.Client, topic string, m *zbc.Deployment) (*zbc.Message, error) {
	commandRequest := zbc.NewDeploymentMessage(&sbe.ExecuteCommandRequest{
		PartitionId: 0,
		Position:    0,
		Key:         0,
		TopicName:   []uint8(topic),
		Command:     []uint8{},
	}, m)

	return sendRequest(client, commandRequest)
}

func sendRequest(client *zbc.Client, commandRequest *zbc.Message) (*zbc.Message, error) {
	response, err := client.Responder(commandRequest)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return response, nil
}

func openSubscription(client *zbc.Client, topic string, pid int32, lo string, tt string) {
	taskSub := &zbc.TaskSubscription{
		TopicName:     topic,
		PartitionID:   pid,
		Credits:       32,
		LockDuration:  300000,
		LockOwner:     lo,
		SubscriberKey: 0,
		TaskType:      tt,
	}
	subscriptionCh, err := client.TaskConsumer(taskSub)
	isFatal(err)

	log.Println("Waiting for events ....")
	for {
		message := <-subscriptionCh
		fmt.Printf("%#v\n", *message.Data)
		//
	}
}

func loadCommandYaml(path string, command interface{}) error {
	yamlFile, _ := loadFile(path)

	err := yaml.Unmarshal(yamlFile, command)
	if err != nil {
		return err
	}
	return nil
}

func loadFile(path string) ([]byte, error) {
	log.Printf("Loading resource at %s\n", path)
	if len(path) == 0 {
		return nil, errResourceNotFound
	}
	filename, _ := filepath.Abs(path)
	return ioutil.ReadFile(filename)

}

func loadConfig(path string, c *config) {
	if _, err := toml.DecodeFile(path, c); err != nil {
		log.Printf("Reading configuration failed. Expecting to found configuration file at %s\n", path)
		log.Printf("HINT: Configuration file is not in place. Try setting configuration path with:")
		log.Fatalln(" zbctl --config <path to config.toml>")
	}
}

// Let's export some stuff

//export subscribe
func subscribe(broker string, topic string, partitionId int, lockOwner string, taskType string) {
	client, err := zbc.NewClient(broker)
	isFatal(err)
	log.Println("Connected to Zeebe.")
	openSubscription(client, topic,
		int32(partitionId),
		lockOwner,
		taskType)
	//return nil
}

//export deployWorkflow
func deployWorkflow(filePath string, broker string, topic string) {
	content, err := loadFile(filePath)
	isFatal(err)

	var deployment = zbc.Deployment{
		State:   "CREATE_DEPLOYMENT",
		BpmnXml: content,
	}

	client, err := zbc.NewClient(broker)
	isFatal(err)
	log.Println("Connected to Zeebe.")

	response, err := sendDeployment(client, topic, &deployment)
	isFatal(err)

	if response.Data != nil {
		if state, ok := (*response.Data)["state"]; ok {
			log.Println(state)
		}
	} else {
		log.Println("err: received nil response")
	}
}

//export startWorkFlowInstance
func startWorkFlowInstance(command string, broker string, topic string) {
	var workflowInstance zbc.WorkflowInstance
	err := loadCommandYaml(command, &workflowInstance)
	isFatal(err)
	log.Println(broker);
	client, err := zbc.NewClient(broker)
	isFatal(err)
	log.Println("Connected to Zeebe.")

	response, err := sendWorkflowInstance(client, topic, &workflowInstance)
	isFatal(err)

	log.Println("Success. Received response:")
	log.Println(*response)
}

func main() {
	
}
