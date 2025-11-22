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
	"os"
)

type (
	TModellingBusConnector struct {
		modellingBusRepositoryConnector *tModellingBusRepositoryConnector
		modellingBusEventsConnector     *tModellingBusEventsConnector

		agentID string

		reporter *TReporter
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

	event.tRepositoryEvent = b.modellingBusRepositoryConnector.addFile(topicPath, fileName, fileExtension, localFilePath)

	message, err := json.Marshal(event)
	if err != nil {
		b.reporter.Error("Something went wrong JSONing the link data. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

func (b *TModellingBusConnector) deleteFile(topicPath, fileName, fileExtension string) {
	b.modellingBusEventsConnector.deleteEvent(topicPath)
	b.modellingBusRepositoryConnector.deleteFile(topicPath, fileName, fileExtension)
}

func (b *TModellingBusConnector) deleteExperiment() {
//HERE
//	b.modellingBusEventsConnector.deleteEvent(topicPath)
//	b.modellingBusRepositoryConnector.deleteFile(topicPath, fileName, fileExtension)
}

func (b *TModellingBusConnector) listenForFilePostings(agentID, topicPath string, postingHandler func(string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		event := tEvent{}

		// Use a generic error checker for Unmarshal. Should return a bool
		err := json.Unmarshal(message, &event)
		if err == nil {
			tempFilePath := b.modellingBusRepositoryConnector.getFile(event.tRepositoryEvent, GetTimestamp())

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
			b.reporter.Error("Something went wrong JSONing the link data. %s", err)
			return
		}

		b.modellingBusEventsConnector.postEvent(topicPath, message)

		// CLEAN any old ones on the ftp server!!
	} else {
		event := tEvent{}
		event.Timestamp = timestamp

		event.tRepositoryEvent = b.modellingBusRepositoryConnector.addJSONAsFile(topicPath, jsonMessage)

		message, err := json.Marshal(event)
		if err != nil {
			b.reporter.Error("Something went wrong JSONing the link data. %s", err)
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
				tempFilePath := b.modellingBusRepositoryConnector.getFile(event.tRepositoryEvent, GetTimestamp())

				jsonPayload, err := os.ReadFile(tempFilePath)
				if err == nil {
					postingHandler(jsonPayload, event.Timestamp)
				} else {
					b.reporter.Error("Something went wrong while retrieving file. %s", err)
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
 * Creation
 */

func CreateModellingBusConnector(configData *TConfigData, reporter *TReporter) TModellingBusConnector {
	agentID := configData.GetValue("", "agent").String()
	experimentID := configData.GetValue("", "experiment").String()
	topicBase := modellingBusVersion + "/" + experimentID

	modellingBusConnector := TModellingBusConnector{}
	modellingBusConnector.reporter = reporter
	modellingBusConnector.agentID = agentID

	modellingBusConnector.modellingBusRepositoryConnector =
		createModellingBusRepositoryConnector(
			topicBase,
			agentID,
			configData,
			reporter)

	modellingBusConnector.modellingBusEventsConnector =
		createModellingBusEventsConnector(
			topicBase,
			agentID,
			configData,
			reporter)

	return modellingBusConnector
}
