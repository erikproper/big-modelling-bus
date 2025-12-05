/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Generic
 * Component: Layer 2 - Basic Modelling Bus
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
	"encoding/json"
	"os"

	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

type (
	TModellingBusConnector struct {
		modellingBusRepositoryConnector *tModellingBusRepositoryConnector
		modellingBusEventsConnector     *tModellingBusEventsConnector

		agentID,
		environmentID string

		Reporter   *generics.TReporter
		configData *generics.TConfigData
	}

	tStreamedEvent struct {
		Timestamp string          `json:"timestamp"`
		Payload   json.RawMessage `json:"payload"`
	}
)

/*
 * Posting things
 */

func (b *TModellingBusConnector) postFile(topicPath, localFilePath, timestamp string) {
	event := b.modellingBusRepositoryConnector.addFile(topicPath, localFilePath, timestamp)

	message, err := json.Marshal(event)
	if err != nil {
		b.Reporter.Error("Something went wrong JSONing the link data. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

func (b *TModellingBusConnector) postJSONAsFile(topicPath string, jsonMessage []byte, timestamp string) {
	event := b.modellingBusRepositoryConnector.addJSONAsFile(topicPath, jsonMessage, timestamp)

	message, err := json.Marshal(event)
	if err != nil {
		b.Reporter.Error("Something went wrong JSONing the link data. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

func (b *TModellingBusConnector) postJSONAsStreamed(topicPath string, jsonMessage []byte, timestamp string) {
	event := tStreamedEvent{}
	event.Timestamp = timestamp
	event.Payload = jsonMessage

	message, err := json.Marshal(event)
	if err != nil {
		b.Reporter.Error("Something went wrong JSONing the event. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

/*
 * Retrieving things
 */

func (b *TModellingBusConnector) getLinkedFileFromRepository(message []byte, localFileName string) (string, string) {
	event := tRepositoryEvent{}

	// WORK: Use a generic error checker for Unmarshal. Should return a bool
	err := json.Unmarshal(message, &event)
	if err == nil {
		return b.modellingBusRepositoryConnector.getFile(event, localFileName), event.Timestamp
	} else {
		return "", ""
	}
}

func (b *TModellingBusConnector) getFileFromPosting(agentID, topicPath, localFileName string) (string, string) {
	return b.getLinkedFileFromRepository(b.modellingBusEventsConnector.messageFromEvent(agentID, topicPath), localFileName)
}

func (b *TModellingBusConnector) getJSONFromTemporaryFile(tempFilePath, timestamp string) ([]byte, string) {
	jsonPayload, err := os.ReadFile(tempFilePath)
	os.Remove(tempFilePath)

	if err != nil {
		b.Reporter.Error("Something went wrong while retrieving file. %s", err)
		b.Reporter.Error("Temporary file to be opened: %s", tempFilePath)
		return []byte{}, ""
	}

	return jsonPayload, timestamp
}

func (b *TModellingBusConnector) getJSON(agentID, topicPath string) ([]byte, string) {
	tempFilePath, timestamp := b.getLinkedFileFromRepository(b.modellingBusEventsConnector.messageFromEvent(agentID, topicPath), generics.JSONFileName)

	jsonPayload, err := os.ReadFile(tempFilePath)
	os.Remove(tempFilePath)

	if err != nil {
		return []byte{}, ""
	}

	return jsonPayload, timestamp
}

func (b *TModellingBusConnector) getStreamed(agentID, topicPath string) ([]byte, string) {
	event := tStreamedEvent{}

	message := b.modellingBusEventsConnector.messageFromEvent(agentID, topicPath)

	err := json.Unmarshal(message, &event)
	if err == nil {
		return event.Payload, event.Timestamp
	} else {
		return []byte{}, ""
	}
}

/*
 * Listening for postings
 */

func (b *TModellingBusConnector) listenForFilePostings(agentID, topicPath, localFileName string, postingHandler func(string, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		postingHandler(b.getLinkedFileFromRepository(message, localFileName))
	})
}

func (b *TModellingBusConnector) listenForJSONFilePostings(agentID, topicPath string, postingHandler func([]byte, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		postingHandler(b.getJSONFromTemporaryFile(b.getLinkedFileFromRepository(message, generics.JSONFileName)))
	})
}


func (b *TModellingBusConnector) listenForStreamedPostings(agentID, topicPath string, postingHandler func([]byte, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		event := tStreamedEvent{}

		err := json.Unmarshal(message, &event)
		if err == nil {
			postingHandler(event.Payload, event.Timestamp
		}
	})
}

/*
 * Deleting postings
 */

func (b *TModellingBusConnector) deletePosting(topicPath string) {
	b.modellingBusEventsConnector.deletePostingPath(topicPath)
	b.modellingBusRepositoryConnector.deletePostingPath(topicPath)
}

/*
 *
 * Externally visible functionality
 *
 */

func (b *TModellingBusConnector) DeleteEnvironment(environment ...string) {
	environmentToDelete := b.environmentID
	if len(environment) > 0 {
		environmentToDelete = environment[0]
	}

	b.Reporter.Progress(1, "Deleting environment: %s", environmentToDelete)

	b.modellingBusEventsConnector.deleteEnvironment(environmentToDelete)
	b.modellingBusRepositoryConnector.deleteEnvironment(environmentToDelete)
}

func CreateModellingBusConnector(configData *generics.TConfigData, reporter *generics.TReporter, postingOnly bool) TModellingBusConnector {
	modellingBusConnector := TModellingBusConnector{}
	modellingBusConnector.environmentID = configData.GetValue("", "environment").String()
	modellingBusConnector.agentID = configData.GetValue("", "agent").String()
	modellingBusConnector.configData = configData
	modellingBusConnector.Reporter = reporter

	modellingBusConnector.modellingBusRepositoryConnector =
		createModellingBusRepositoryConnector(
			modellingBusConnector.environmentID,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.Reporter)

	modellingBusConnector.modellingBusEventsConnector =
		createModellingBusEventsConnector(
			modellingBusConnector.environmentID,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.Reporter,
			postingOnly)

	return modellingBusConnector
}
