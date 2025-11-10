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
 * Version of: XX.10.2025
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

		// Use a generic error checker for Unmarshal. Should return a bool
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
 * Initialisation & creation
 */

func (b *TModellingBusConnector) Initialise(configPath string, errorReporter TErrorReporter) {
	var ok bool

	// This needs to be done on the top level ...
	b.errorReporter = errorReporter
	b.configData, ok = LoadConfig(configPath, b.errorReporter)
	if !ok {
		fmt.Println("Config file not found ... need to fix this")
	}

	b.agentID = b.configData.GetValue("", "agent").String()

	topicBase := modellingBusVersion + "/" + b.configData.GetValue("", "experiment").String()
	b.modellingBusRepositoryConnector = createModellingBusRepositoryConnector(topicBase, b.agentID, b.configData, b.errorReporter)
	b.modellingBusEventsConnector = createModellingBusEventsConnector(topicBase, b.agentID, b.configData, b.errorReporter)

	b.lastTimeTimestamp = ""
	b.timestampCounter = 0

}

func CreateModellingBusConnector(config string, errorReporter TErrorReporter) TModellingBusConnector {
	modellingBusConnector := TModellingBusConnector{}
	modellingBusConnector.Initialise(config, errorReporter)

	return modellingBusConnector
}
