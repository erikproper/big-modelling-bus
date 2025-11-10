/*
 *
 * Package: mbconnect
 * Layer:   2
 * Module:  basic_modelling_bus
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
	"encoding/json"
	"fmt"
	"os"
)

type (
	TModellingBusConnector struct {
		configData *TConfigData

		modellingBusRepositoryConnector *tModellingBusRepositoryConnector
		modellingBusEventsConnector     *tModellingBusEventsConnector

		agentID,
		lastTimeTimestamp string

		timestampCounter int

		errorReporter TErrorReporter // used "up" here?
	}
)

type tEvent struct {
	tRepositoryEvent

	Timestamp   string          `json:"timestamp"`
	JSONMessage json.RawMessage `json:"message,omitempty"`
}

func (b *TModellingBusConnector) postFile(topicPath, fileName, fileExtension, localFilePath, timestamp string) {
	event := tEvent{}
	event.Timestamp = timestamp

	event.tRepositoryEvent = b.modellingBusRepositoryConnector.pushFileToRepository(topicPath, fileName, fileExtension, localFilePath)

	message, err := json.Marshal(event)
	if err != nil {
		b.errorReporter("Something went wrong JSONing the link data", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

func (b *TModellingBusConnector) listenForFilePostings(agentID, topicPath string, postingHandler func(string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		event := tEvent{}

		// Use a generic error checker for Unmarshal. Should return a bool
		err := json.Unmarshal(message, &event)
		if err == nil {
			tempFilePath := b.modellingBusRepositoryConnector.getFileFromRepository(event.tRepositoryEvent, GetTimestamp())

			postingHandler(tempFilePath)
		}

	})
}

func (b *TModellingBusConnector) postJSON(topicPath, jsonVersion string, jsonMessage []byte, timestamp string) {
	if b.modellingBusEventsConnector.eventPayloadAllowed(jsonMessage) {
		event := tEvent{}
		event.Timestamp = timestamp
		event.JSONMessage = jsonMessage

		message, err := json.Marshal(event)
		if err != nil {
			b.errorReporter("Something went wrong JSONing the link data", err)
			return
		}

		b.modellingBusEventsConnector.postEvent(topicPath, message)

		// CLEAN any old ones on the ftp server!!
	} else {
		event := tEvent{}
		event.Timestamp = timestamp

		event.tRepositoryEvent = b.modellingBusRepositoryConnector.pushJSONAsFileToRepository(topicPath, jsonMessage)

		message, err := json.Marshal(event)
		if err != nil {
			b.errorReporter("Something went wrong JSONing the link data", err)
			return
		}

		b.modellingBusEventsConnector.postEvent(topicPath, message)
	}
}

func (b *TModellingBusConnector) listenForJSONPostings(agentID, topicPath string, postingHandler func([]byte, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		event := tEvent{}

		err := json.Unmarshal(message, &event)
		if err == nil {
			if len(event.JSONMessage) > 0 {
				postingHandler(event.JSONMessage, event.Timestamp)
			} else {
				tempFilePath := b.modellingBusRepositoryConnector.getFileFromRepository(event.tRepositoryEvent, GetTimestamp())

				jsonPayload, err := os.ReadFile(tempFilePath)
				if err == nil {
					postingHandler(jsonPayload, event.Timestamp)
				} else {
					b.errorReporter("Something went wrong while retrieving file", err)
				}

				os.Remove(tempFilePath)
			}
		}
	})
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Unique IDs
 */

func (b *TModellingBusConnector) GetNewID() string {
	return fmt.Sprintf("%s-%s", b.agentID, GetTimestamp())
}

/*
 * Creation
 */

func CreateModellingBusConnector(configPath string, errorReporter TErrorReporter) TModellingBusConnector {
	var ok bool

	modellingBusConnector := TModellingBusConnector{}
	// This needs to be done on the top level ...
	modellingBusConnector.errorReporter = errorReporter
	modellingBusConnector.configData, ok = LoadConfig(configPath, modellingBusConnector.errorReporter)
	if !ok {
		fmt.Println("Config file not found ... need to fix this")
	}

	modellingBusConnector.agentID = modellingBusConnector.configData.GetValue("", "agent").String()

	topicBase := modellingBusVersion + "/" + modellingBusConnector.configData.GetValue("", "experiment").String()

	modellingBusConnector.modellingBusRepositoryConnector =
		createModellingBusRepositoryConnector(
			topicBase,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.errorReporter)

	modellingBusConnector.modellingBusEventsConnector =
		createModellingBusEventsConnector(
			topicBase,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.errorReporter)

	modellingBusConnector.lastTimeTimestamp = ""
	modellingBusConnector.timestampCounter = 0

	return modellingBusConnector
}
