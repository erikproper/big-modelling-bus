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

		agentID,
		environmentID string

		reporter   *TReporter
		configData *TConfigData
	}
)

func (b *TModellingBusConnector) postFile(topicPath, localFilePath string) {
	event := b.modellingBusRepositoryConnector.addFile(topicPath, localFilePath, GetTimestamp())

	message, err := json.Marshal(event)
	if err != nil {
		b.reporter.Error("Something went wrong JSONing the link data. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

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

func (b *TModellingBusConnector) listenForFilePostings(agentID, topicPath, localFileName string, postingHandler func(string, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		postingHandler(b.getLinkedFileFromRepository(message, localFileName))
	})
}

func (b *TModellingBusConnector) getFileFromPosting(agentID, topicPath, localFileName string) (string, string) {
	return b.getLinkedFileFromRepository(b.modellingBusEventsConnector.messageFromEvent(agentID, topicPath), localFileName)
}

func (b *TModellingBusConnector) postJSON(topicPath, jsonVersion string, jsonMessage []byte, timestamp string) {
	event := b.modellingBusRepositoryConnector.addJSONAsFile(topicPath, jsonMessage, timestamp)

	message, err := json.Marshal(event)
	if err != nil {
		b.reporter.Error("Something went wrong JSONing the link data. %s", err)
		return
	}

	b.modellingBusEventsConnector.postEvent(topicPath, message)
}

func (b *TModellingBusConnector) getJSONFromTemporaryFile(tempFilePath, timestamp string) ([]byte, string) {
	jsonPayload, err := os.ReadFile(tempFilePath)
	os.Remove(tempFilePath)

	if err != nil {
		b.reporter.Error("Something went wrong while retrieving file. %s", err)
		b.reporter.Error("Temporary file to be opened: %s", tempFilePath)
		return []byte{}, ""
	}

	return jsonPayload, timestamp
}

func (b *TModellingBusConnector) listenForJSONPostings(agentID, topicPath string, postingHandler func([]byte, string)) {
	b.modellingBusEventsConnector.listenForEvents(agentID, topicPath, func(message []byte) {
		postingHandler(b.getJSONFromTemporaryFile(b.getLinkedFileFromRepository(message, jsonFileName)))
	})
}

func (b *TModellingBusConnector) getJSON(agentID, topicPath string) ([]byte, string) {
	tempFilePath, timestamp := b.getLinkedFileFromRepository(b.modellingBusEventsConnector.messageFromEvent(agentID, topicPath), jsonFileName)

	jsonPayload, err := os.ReadFile(tempFilePath)
	os.Remove(tempFilePath)

	if err != nil {
		return []byte{}, ""
	}

	return jsonPayload, timestamp
}

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

	b.reporter.Progress(1, "Deleting environment: %s", environmentToDelete)

	b.modellingBusEventsConnector.deleteEnvironment(environmentToDelete)
	b.modellingBusRepositoryConnector.deleteEnvironment(environmentToDelete)
}

func CreateModellingBusConnector(configData *TConfigData, reporter *TReporter) TModellingBusConnector {
	modellingBusConnector := TModellingBusConnector{}
	modellingBusConnector.environmentID = configData.GetValue("", "environment").String()
	modellingBusConnector.agentID = configData.GetValue("", "agent").String()
	modellingBusConnector.configData = configData
	modellingBusConnector.reporter = reporter

	modellingBusConnector.modellingBusRepositoryConnector =
		createModellingBusRepositoryConnector(
			modellingBusConnector.environmentID,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.reporter)

	modellingBusConnector.modellingBusEventsConnector =
		createModellingBusEventsConnector(
			modellingBusConnector.environmentID,
			modellingBusConnector.agentID,
			modellingBusConnector.configData,
			modellingBusConnector.reporter)

	return modellingBusConnector
}
