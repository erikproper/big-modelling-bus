/*
 *
 * Package: mbconnect
 * Layer:   1
 * Module:  events_connector
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.11.2025
 *
 */

package mbconnect

import (
	"github.com/eclipse/paho.mqtt.golang"
	"strings"
	"time"
)

type (
	tModellingBusEventsConnector struct {
		agentID,
		user,
		port,
		topicRoot,
		broker,
		password string

		loadDelay int

		messages map[string][]byte

		client mqtt.Client

		reporter *TReporter
	}
)

func (e *tModellingBusEventsConnector) connectionLostHandler(c mqtt.Client, err error) {
	e.reporter.Panic("MQTT connection lost. %s", err)
}

func (e *tModellingBusEventsConnector) WaitForMQTT() {
	e.reporter.Progress("Sleeping for %d miliseconds to collect information from the MQTT bus.", e.loadDelay)
	time.Sleep(time.Duration(e.loadDelay) * time.Second / 1000)
}

func (e *tModellingBusEventsConnector) connectToMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + e.broker + ":" + e.port)
	opts.SetUsername(e.user)
	opts.SetPassword(e.password)
	opts.SetConnectionLostHandler(e.connectionLostHandler)

	connected := false
	for !connected {
		e.reporter.Progress("Trying to connect to the MQTT broker.")

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

	e.messages = map[string][]byte{}
	if connected {
		e.reporter.Progress("Connected to the MQTT broker.")

		// Continuously connect all used topics underneath the topic root, and their messages
		// We need this to enable deletion of topics, as well as to be able to pro-actively
		// pull information from the modelling bus
		mqttTopicPath := e.topicRoot + "/#"
		token := e.client.Subscribe(mqttTopicPath, 0, func(client mqtt.Client, msg mqtt.Message) {
			topic := msg.Topic()
			payload := msg.Payload()
			if len(payload) == 0 {
				delete(e.messages, topic)
			} else {
				e.messages[topic] = payload
			}
		})
		token.Wait()

		e.WaitForMQTT()

		if len(e.messages) == 0 {
			e.reporter.Progress("No topics found.")
		} else {
			e.reporter.Progress("Found topic(s):")
			for topic := range e.messages {
				if strings.HasPrefix(topic, e.topicRoot) {
					e.reporter.Progress("- %s", topic)
				}
			}
		}
	}
}

func (e *tModellingBusEventsConnector) mqttTopicPath(agentID, topicPath string) string {
	return e.topicRoot + "/" + agentID + "/" + topicPath
}

// / Document the QoS choices.
func (e *tModellingBusEventsConnector) listenForEvents(agentID, topicPath string, eventHandler func([]byte)) {
	mqttTopicPath := e.mqttTopicPath(agentID, topicPath)

	token := e.client.Subscribe(mqttTopicPath, 0, func(client mqtt.Client, msg mqtt.Message) {
		if len(msg.Payload()) > 0 {
			eventHandler(msg.Payload())
		}
	})
	token.Wait()
}

// Pro-actively get the (latest) message from the bus.
func (e *tModellingBusEventsConnector) messageFromEvent(agentID, topicPath string) []byte {
	mqttTopicPath := e.mqttTopicPath(agentID, topicPath)

	message := e.messages[mqttTopicPath]
	// When messageFromEvent is called too soon after opening the connection to the MQTT broker,
	// we may not have received a message yet. So, we need to be patient. Once.
	if len(message) == 0 {
		e.WaitForMQTT()
		message = e.messages[mqttTopicPath]
	}

	return e.messages[mqttTopicPath]
}

func (e *tModellingBusEventsConnector) postMessage(topicPath string, message []byte) {
	token := e.client.Publish(topicPath, 0, true, string(message))
	token.Wait()
}

func (e *tModellingBusEventsConnector) postEvent(topicPath string, message []byte) {
	mqttTopicPath := e.mqttTopicPath(e.agentID, topicPath)
	e.postMessage(mqttTopicPath, message)
}

func (e *tModellingBusEventsConnector) deletePath(topicPath string) {
	e.postMessage(topicPath, []byte{})
}

func (e *tModellingBusEventsConnector) deletePostingPath(topicPath string) {
	e.postEvent(topicPath, []byte{})
}

func (e *tModellingBusEventsConnector) deleteExperiment() {
	for topic := range e.messages {
		if strings.HasPrefix(topic, e.topicRoot) {
			e.deletePath(topic)
		}
	}
}

func createModellingBusEventsConnector(topicBase, agentID string, configData *TConfigData, reporter *TReporter) *tModellingBusEventsConnector {
	e := tModellingBusEventsConnector{}

	e.reporter = reporter

	// Get data from the config file
	e.agentID = agentID
	e.port = configData.GetValue("mqtt", "port").String()
	e.user = configData.GetValue("mqtt", "user").String()
	e.broker = configData.GetValue("mqtt", "broker").String()
	e.password = configData.GetValue("mqtt", "password").String()
	e.topicRoot = configData.GetValue("mqtt", "prefix").String() + "/" + topicBase
	e.loadDelay = configData.GetValue("mqtt", "load_delay").IntWithDefault(1)

	e.connectToMQTT()

	return &e
}
