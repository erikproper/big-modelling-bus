/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Generic
 * Component: Layer 1 - Events Connector
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.11.2025
 *
 */

package connect

import (
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

type (
	tModellingBusEventsConnector struct {
		user,
		port,
		broker,
		prefix,
		agentID,
		password,
		environmentID string

		loadDelay int

		connectionBeingOpenened bool

		currentMessages,
		openingMessages map[string][]byte

		client mqtt.Client

		reporter *generics.TReporter
	}
)

func (e *tModellingBusEventsConnector) mqttEnvironmentTopicRoot() string {
	return e.prefix + "/" + generics.ModellingBusVersion + "/" + e.environmentID
}

func (e *tModellingBusEventsConnector) mqttEnvironmentTopicListFor(environmentID string) string {
	return e.prefix + "/" + generics.ModellingBusVersion + "/" + environmentID + "/#"
}

func (e *tModellingBusEventsConnector) mqttAgentTopicRootFor(environmentID, agentID string) string {
	return e.prefix + "/" + generics.ModellingBusVersion + "/" + environmentID + "/" + agentID
}

func (e *tModellingBusEventsConnector) mqttAgentTopicPath(agentID, topicPath string) string {
	return e.prefix + "/" + generics.ModellingBusVersion + "/" + e.environmentID + "/" + agentID + "/" + topicPath
}

func (e *tModellingBusEventsConnector) connectionLostHandler(c mqtt.Client, err error) {
	e.reporter.Panic("MQTT connection lost. %s", err)
}

func (e *tModellingBusEventsConnector) waitForMQTT() {
	e.reporter.Progress(generics.ProgressLevelDetailed, "Sleeping for %d miliseconds to collect information from the MQTT bus.", e.loadDelay)
	time.Sleep(time.Duration(e.loadDelay) * time.Second / 1000)
}

func (e *tModellingBusEventsConnector) collectTopicsForModellingEnvironment(environmentID string) {
	token := e.client.Subscribe(e.mqttEnvironmentTopicListFor(environmentID), 0, func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := msg.Payload()
		if len(payload) == 0 {
			delete(e.openingMessages, topic)
			delete(e.currentMessages, topic)
		} else {
			if e.connectionBeingOpenened {
				e.openingMessages[topic] = payload
				e.currentMessages[topic] = payload
			} else {
				if _, defined := e.openingMessages[topic]; !defined {
					e.openingMessages[topic] = []byte{}
				}
				e.currentMessages[topic] = payload
			}
		}
	})
	token.Wait()

	e.waitForMQTT()

	if len(e.openingMessages) == 0 {
		e.reporter.Progress(generics.ProgressLevelDetailed, "No topics found.")
	} else {
		e.reporter.Progress(generics.ProgressLevelDetailed, "Found topic(s):")
		for topic := range e.openingMessages {
			if strings.HasPrefix(topic, e.mqttEnvironmentTopicRoot()) {
				e.reporter.Progress(generics.ProgressLevelDetailed, "- %s", topic)
			}
		}
	}
}

func (e *tModellingBusEventsConnector) connectToMQTT(postingOnly bool) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + e.broker + ":" + e.port)
	opts.SetUsername(e.user)
	opts.SetPassword(e.password)
	opts.SetConnectionLostHandler(e.connectionLostHandler)

	connected := false
	for !connected {
		e.reporter.Progress(generics.ProgressLevelBasic, "Trying to connect to the MQTT broker.")

		e.client = mqtt.NewClient(opts)
		token := e.client.Connect()
		token.Wait()

		err := token.Error()
		if err != nil {
			e.reporter.Error("Error connecting to the MQTT broker. %s", err)

			time.Sleep(5 * time.Second)
		} else {
			connected = true
		}
	}

	e.openingMessages = map[string][]byte{}
	if connected {
		e.reporter.Progress(generics.ProgressLevelBasic, "Connected to the MQTT broker.")

		// Continuously connect all used topics underneath the topic root, and their messages
		// We need this to enable deletion of topics, as well as to be able to pro-actively
		// pull information from the modelling bus
		if !postingOnly {
			e.collectTopicsForModellingEnvironment(e.environmentID)
		}

		e.connectionBeingOpenened = false
	}
}

// / Document the QoS choices.
func (e *tModellingBusEventsConnector) listenForEvents(agentID, topicPath string, eventHandler func([]byte)) {
	mqttTopicPath := e.mqttAgentTopicPath(agentID, topicPath)

	token := e.client.Subscribe(mqttTopicPath, 0, func(client mqtt.Client, msg mqtt.Message) {
		payload := msg.Payload()

		if len(payload) > 0 && string(e.openingMessages[mqttTopicPath]) != string(payload) {
			eventHandler(payload)
		}
	})
	token.Wait()
}

// Pro-actively get the (latest) message from the bus.
func (e *tModellingBusEventsConnector) messageFromEvent(agentID, topicPath string) []byte {
	mqttTopicPath := e.mqttAgentTopicPath(agentID, topicPath)

	message := e.openingMessages[mqttTopicPath]
	// When messageFromEvent is called too soon after opening the connection to the MQTT broker,
	// we may not have received a message yet. So, we need to be patient. Once.
	if len(message) == 0 {
		e.waitForMQTT()
		message = e.openingMessages[mqttTopicPath]
	}

	return message
}

func (e *tModellingBusEventsConnector) postMessage(topicPath string, message []byte) {
	token := e.client.Publish(topicPath, 0, true, string(message))
	token.Wait()
}

func (e *tModellingBusEventsConnector) postEvent(topicPath string, message []byte) {
	e.postMessage(e.mqttAgentTopicPath(e.agentID, topicPath), message)
}

func (e *tModellingBusEventsConnector) deletePath(topicPath string) {
	e.postMessage(topicPath, []byte{})
}

func (e *tModellingBusEventsConnector) deletePostingPath(topicPath string) {
	e.postEvent(topicPath, []byte{})
}

func (e *tModellingBusEventsConnector) deleteEnvironment(environmentID string) {
	e.collectTopicsForModellingEnvironment(environmentID)

	for topic := range e.openingMessages {
		if strings.HasPrefix(topic, e.mqttAgentTopicRootFor(environmentID, e.agentID)) {
			e.deletePath(topic)
		}
	}
}

func createModellingBusEventsConnector(environmentID, agentID string, configData *generics.TConfigData, reporter *generics.TReporter, postingOnly bool) *tModellingBusEventsConnector {
	e := tModellingBusEventsConnector{}

	// Get data from the config file
	e.port = configData.GetValue("mqtt", "port").String()
	e.user = configData.GetValue("mqtt", "user").String()
	e.broker = configData.GetValue("mqtt", "broker").String()
	e.password = configData.GetValue("mqtt", "password").String()
	e.prefix = configData.GetValue("mqtt", "prefix").String()
	e.loadDelay = configData.GetValue("mqtt", "load_delay").IntWithDefault(1)
	e.connectionBeingOpenened = true
	e.currentMessages = map[string][]byte{}
	e.openingMessages = map[string][]byte{}

	e.agentID = agentID
	e.environmentID = environmentID
	e.reporter = reporter

	e.connectToMQTT(postingOnly)

	return &e
}
