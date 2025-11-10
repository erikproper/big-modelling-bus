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
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"time"
)

const (
	mqttMaxMessageSizeDefault = 10240
)

type (
	tModellingBusEventsConnector struct {
		agentID,
		mqttUser,
		mqttPort,
		mqttRoot,
		mqttBroker,
		mqttPassword string
		mqttMaxMessageSize int

		mqttClient mqtt.Client

		errorReporter TErrorReporter
	}
)

func (e *tModellingBusEventsConnector) connectionLostHandler(c mqtt.Client, err error) {
	panic(fmt.Sprintf("PANIC; MQTT connection lost, reason: %v\n", err))
}

func (e *tModellingBusEventsConnector) connectToMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + e.mqttBroker + ":" + e.mqttPort)
	opts.SetClientID("mqtt-client-" + e.agentID)
	opts.SetUsername(e.mqttUser)
	opts.SetPassword(e.mqttPassword)
	opts.SetConnectionLostHandler(e.connectionLostHandler)

	for connected := false; !connected; {
		// Two log channels needed. One for errors, and one for normal progress.
		fmt.Println("Trying to connect to the MQTT broker")

		e.mqttClient = mqtt.NewClient(opts)
		token := e.mqttClient.Connect()
		token.Wait()

		err := token.Error()
		if err != nil {
			e.errorReporter("Error connecting to the MQTT broker:", err)

			time.Sleep(5)
		} else {
			connected = true
		}
	}

	fmt.Println("Connected to the MQTT broker")
}

func (e *tModellingBusEventsConnector) listenForEvents(AgentID, topicPath string, eventHandler func([]byte)) {
	mqttTopicPath := e.mqttRoot + "/" + AgentID + "/" + topicPath
	token := e.mqttClient.Subscribe(mqttTopicPath, 1, func(client mqtt.Client, msg mqtt.Message) {
		eventHandler(msg.Payload())
	})
	token.Wait()
}

func (e *tModellingBusEventsConnector) postEvent(topicPath string, message []byte) {
	mqttTopicPath := e.mqttRoot + "/" + e.agentID + "/" + topicPath
	token := e.mqttClient.Publish(mqttTopicPath, 0, true, string(message))
	token.Wait()
}

func (e *tModellingBusEventsConnector) eventPayloadAllowed(payload []byte) bool {
	return len(payload) <= e.mqttMaxMessageSize
}

func createModellingBusEventsConnector(topicBase, agentID string, configData *TConfigData, errorReporter TErrorReporter) *tModellingBusEventsConnector {
	e := tModellingBusEventsConnector{}

	e.errorReporter = errorReporter

	// Get data from the config file
	e.agentID = agentID
	e.mqttPort = configData.GetValue("mqtt", "port").String()
	e.mqttUser = configData.GetValue("mqtt", "user").String()
	e.mqttBroker = configData.GetValue("mqtt", "broker").String()
	e.mqttPassword = configData.GetValue("mqtt", "password").String()
	e.mqttRoot = configData.GetValue("mqtt", "prefix").String() + "/" + topicBase
	e.mqttMaxMessageSize = configData.GetValue("mqtt", "max_message_size").IntWithDefault(mqttMaxMessageSizeDefault)

	e.connectToMQTT()

	return &e
}
