/*
 *
 * Package: mbconnect
 * Module:  modelling_bus_models_connector
 *
 * Defines ... TModellingBusModelConnector
 * func (b *TModellingBusModelConnector) PostConsidering(consideringdStateJSON []byte, err error) {
 * func (b *TModellingBusModelConnector) PostState(stateJSON []byte, err error) {
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
)

const (
	modelsPathElement           = "models"
	modelStatePathElement       = "state"
	modelConsideringPathElement = "considering"
	modelUpdatePathElement      = "update"
)

type (
	TModellingBusModelConnector struct {
		ModellingBusConnector TModellingBusConnector
		timestamp             string `json:"timestamp"`
		modelJSONVersion      string `json:"json version"`
		ModelID               string `json:"model id"`

		// Externally visible
		ModelCurrentContent    json.RawMessage `json:"content"`
		ModelUpdatedContent    json.RawMessage `json:"-"`
		ModelConsideredContent json.RawMessage `json:"-"`

		// Before we can communicate updates or considering postings, we must have
		// communicated the state of the model first
		stateCommunicated bool `json:"-"`
	}
)

/*
 *
 * Internal functionality
 *
 */

func (b *TModellingBusModelConnector) modelsTopicPath(ModelID string) string {
	return modelsPathElement +
		"/" + ModelID +
		"/" + b.modelJSONVersion
}

func (b *TModellingBusModelConnector) modelsStateTopicPath(ModelID string) string {
	return b.modelsTopicPath(ModelID) +
		"/" + modelStatePathElement
}

func (b *TModellingBusModelConnector) modelsUpdateTopicPath(ModelID string) string {
	return b.modelsTopicPath(ModelID) +
		"/" + modelUpdatePathElement
}

func (b *TModellingBusModelConnector) modelsConsideringTopicPath(ModelID string) string {
	return b.modelsTopicPath(ModelID) +
		"/" + modelConsideringPathElement
}

type TMQTTDelta struct {
	Operations     json.RawMessage `json:"operations"`
	Timestamp      string          `json:"timestamp"`
	StateTimestamp string          `json:"state timestamp"`
}

func (b *TModellingBusModelConnector) postModelsJSONDelta(modelsDeltaTopicPath string, oldStateJSON, newStateJSON []byte, err error) {
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong when converting to JSON", err)
		return
	}

	deltaOperationsJSON, err := JSONDiff(oldStateJSON, newStateJSON)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong running the JSON diff", err)
		return
	}

	delta := TMQTTDelta{}
	delta.Timestamp = b.ModellingBusConnector.GetTimestamp()
	delta.StateTimestamp = b.timestamp
	delta.Operations = deltaOperationsJSON

	deltaJSON, err := json.Marshal(delta)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong JSONing the diff patch", err)
		return
	}

	b.ModellingBusConnector.postJSONFile(modelsDeltaTopicPath, b.modelJSONVersion, delta.Timestamp, deltaJSON)
}

func (b *TModellingBusModelConnector) processModelsJSONDeltaPosting(currentJSONState json.RawMessage, deltaJSON []byte) (json.RawMessage, bool) {
	delta := TMQTTDelta{}
	err := json.Unmarshal(deltaJSON, &delta)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong unJSONing the received diff patch", err)
		return currentJSONState, false
	}

	if delta.StateTimestamp != b.timestamp {
		b.ModellingBusConnector.errorReporter("Received update out of order", nil)
		return currentJSONState, false
	}

	newJSONState, err := JSONApplyPatch(currentJSONState, delta.Operations)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Applying patch didn't work'", err)
		return currentJSONState, false
	}

	return newJSONState, true
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Initialisation and creation
 */

func (b *TModellingBusModelConnector) Initialise(ModellingBusConnector TModellingBusConnector, modelJSONVersion string) {
	b.ModellingBusConnector = ModellingBusConnector
	b.modelJSONVersion = modelJSONVersion
	b.ModelCurrentContent = []byte{}
	b.ModelUpdatedContent = []byte{}
	b.ModelConsideredContent = []byte{}
	b.timestamp = b.ModellingBusConnector.GetTimestamp()
	b.stateCommunicated = false
}

func CreateModellingBusModelConnector(ModellingBusConnector TModellingBusConnector, modelJSONVersion string) TModellingBusModelConnector {
	ModellingBusModelConnector := TModellingBusModelConnector{}
	ModellingBusModelConnector.Initialise(ModellingBusConnector, modelJSONVersion)

	return ModellingBusModelConnector
}

/*
 * Posting
 */

func (b *TModellingBusModelConnector) PrepareForPosting(ModelID string) {
	b.ModelID = ModelID

	b.ModellingBusConnector.mkFilePath(b.modelsStateTopicPath(b.ModelID))
	b.ModellingBusConnector.mkEventPath(b.modelsStateTopicPath(b.ModelID))

	b.ModellingBusConnector.mkFilePath(b.modelsConsideringTopicPath(b.ModelID))
	b.ModellingBusConnector.mkEventPath(b.modelsConsideringTopicPath(b.ModelID))

	b.ModellingBusConnector.mkFilePath(b.modelsUpdateTopicPath(b.ModelID))
	b.ModellingBusConnector.mkEventPath(b.modelsUpdateTopicPath(b.ModelID))
}

func (b *TModellingBusModelConnector) PostConsidering(consideringStateJSON []byte, err error) {
	if b.stateCommunicated {
		b.ModelConsideredContent = consideringStateJSON

		b.postModelsJSONDelta(b.modelsUpdateTopicPath(b.ModelID), b.ModelCurrentContent, b.ModelUpdatedContent, err)
		b.postModelsJSONDelta(b.modelsConsideringTopicPath(b.ModelID), b.ModelUpdatedContent, b.ModelConsideredContent, err)
	} else {
		b.ModellingBusConnector.errorReporter("We must always see a state posting, before a considering posting!", nil)
	}
}

func (b *TModellingBusModelConnector) PostUpdate(updatedStateJSON []byte, err error) {
	if b.stateCommunicated {
		b.ModelUpdatedContent = updatedStateJSON

		b.postModelsJSONDelta(b.modelsUpdateTopicPath(b.ModelID), b.ModelCurrentContent, b.ModelUpdatedContent, err)
	} else {
		b.PostState(updatedStateJSON, err)
	}
}

func (b *TModellingBusModelConnector) PostState(stateJSON []byte, err error) {
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong when converting to JSON", err)
		return
	}

	b.timestamp = b.ModellingBusConnector.GetTimestamp()
	b.ModelCurrentContent = stateJSON

	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong JSONing the model data", err)
		return
	}

	b.ModellingBusConnector.postJSONFile(b.modelsStateTopicPath(b.ModelID), b.modelJSONVersion, b.timestamp, stateJSON)
	b.stateCommunicated = true
}

/*
 * Listening
 */

func (b *TModellingBusModelConnector) ListenToStatePostings(agentID, ModelID string, handler func()) {
	b.ModellingBusConnector.listenForJSONFilePostings(agentID, b.modelsStateTopicPath(ModelID), func(timestamp string, json []byte) {
		b.ModelCurrentContent = json
		b.ModelUpdatedContent = json
		b.ModelConsideredContent = json
		b.timestamp = timestamp

		handler()
	})
}

func (b *TModellingBusModelConnector) ListenToUpdatePostings(agentID, ModelID string, handler func()) {
	b.ModellingBusConnector.listenForJSONFilePostings(agentID, b.modelsUpdateTopicPath(ModelID), func(timestamp string, json []byte) {
		ok := false
		b.ModelUpdatedContent, ok = b.processModelsJSONDeltaPosting(b.ModelCurrentContent, json)
		if ok {
			b.ModelConsideredContent = b.ModelUpdatedContent

			handler()
		} else {
			fmt.Println("Something went wrong ... yeah .. fix this message")
		}
	})
}

func (b *TModellingBusModelConnector) ListenToConsideringPostings(agentID, ModelID string, handler func()) {
	b.ModellingBusConnector.listenForJSONFilePostings(agentID, b.modelsConsideringTopicPath(ModelID), func(timestamp string, json []byte) {
		ok := false
		b.ModelConsideredContent, ok = b.processModelsJSONDeltaPosting(b.ModelUpdatedContent, json)
		if ok {
			handler()
		} else {
			fmt.Println("Something went wrong ... yeah .. fix this message")
		}
	})
}
