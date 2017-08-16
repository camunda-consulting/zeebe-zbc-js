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
	"github.com/urfave/cli"
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
	var conf config

	app := cli.NewApp()
	app.Usage = "Zeebe control client application"
	app.Version = version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config, cfg",
			Value:  defaultConfiguration,
			Usage:  "Location of the configuration file.",
			EnvVar: "ZBC_CONFIG",
		},
	}
	app.Before = cli.BeforeFunc(func(c *cli.Context) error {
		loadConfig(c.String("config"), &conf)
		log.Println(conf.String())
		return nil
	})

	app.Authors = []cli.Author{
		{Name: "Daniel Meyer", Email: ""},
		{Name: "Sebastian Menski", Email: ""},
		{Name: "Philipp Ossler", Email: ""},
		{Name: "Sam", Email: "samuel.picek@camunda.com"},
	}
	app.Commands = []cli.Command{
		{
			Name:    "create-task",
			Aliases: []string{"t"},
			Usage:   "create a new task using the given YAML file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
			},
			Action: func(c *cli.Context) error {
				var task zbc.Task
				err := loadCommandYaml(c.Args().First(), &task)
				isFatal(err)

				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")

				response, err := sendTask(client, c.String("topic"), &task)
				isFatal(err)

				log.Println("Success. Received response:")
				log.Println(*response.Data)
				return nil
			},
		},
		{
			Name:    "create-workflow-instance",
			Aliases: []string{"wf"},
			Usage:   "create a new workflow instance using the given YAML file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
			},
			Action: func(c *cli.Context) error {
				var workflowInstance zbc.WorkflowInstance
				err := loadCommandYaml(c.Args().First(), &workflowInstance)
				isFatal(err)

				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")

				response, err := sendWorkflowInstance(client, c.String("topic"), &workflowInstance)
				isFatal(err)

				log.Println("Success. Received response:")
				log.Println(*response.Data)
				return nil
			},
		},
		{
			Name:    "deploy",
			Aliases: []string{"d"},
			Usage:   "deploy a workflow",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
			},
			Action: func(c *cli.Context) error {
				content, err := loadFile(c.Args().First())
				isFatal(err)

				var deployment = zbc.Deployment{
					State:   "CREATE_DEPLOYMENT",
					BpmnXml: content,
				}

				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")

				response, err := sendDeployment(client, c.String("topic"), &deployment)
				isFatal(err)

				if response.Data != nil {
					if state, ok := (*response.Data)["state"]; ok {
						log.Println(state)
					}
				} else {
					log.Println("err: received nil response")
				}
				return nil
			},
		},
		{
			Name:    "open",
			Aliases: []string{"n"},
			Usage:   "open a subscription",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "topic, t",
					Value:  "default-topic",
					Usage:  "Executing command request on specific topic.",
					EnvVar: "ZB_TOPIC_NAME",
				},
				cli.Int64Flag{
					Name:   "partition-id, p",
					Value:  0,
					Usage:  "Specify partition on which we are opening subscription.",
					EnvVar: "ZB_PARTITION_ID",
				},
				cli.StringFlag{
					Name:   "lock-owner, l",
					Value:  "zbc",
					Usage:  "Specify lock owner.",
					EnvVar: "ZB_LOCK_OWNER",
				},
				cli.StringFlag{
					Name:   "task-type, tt",
					Value:  "foo",
					Usage:  "Specify task type.",
					EnvVar: "ZB_TASK_TYPE",
				},
			},
			Action: func(c *cli.Context) error {
				client, err := zbc.NewClient(conf.Broker.String())
				isFatal(err)
				log.Println("Connected to Zeebe.")
				openSubscription(client, c.String("topic"),
					int32(c.Int64("partition-id")),
					c.String("lock-owner"),
					c.String("task-type"))
				return nil
			},
		},
	}
	app.Run(os.Args)
}
