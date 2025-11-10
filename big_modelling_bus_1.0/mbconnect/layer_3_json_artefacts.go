/*
 *
 * Package: mbconnect
 * Layer:   3
 * Module:  json_artefacts
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
)

const (
	artefactsPathElement           = "artefacts/file"
	artefactStatePathElement       = "state"
	artefactConsideringPathElement = "considering"
	artefactUpdatePathElement      = "update"
)

type (
	TModellingBusArtefactConnector struct {
		ModellingBusConnector TModellingBusConnector
		Timestamp             string `json:"timestamp"`
		JSONVersion           string `json:"json version"`
		ArtefactID            string `json:"artefact id"`

		// Externally visible
		ArtefactCurrentContent    json.RawMessage `json:"content"`
		ArtefactUpdatedContent    json.RawMessage `json:"-"`
		ArtefactConsideredContent json.RawMessage `json:"-"`

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

func (b *TModellingBusArtefactConnector) artefactsTopicPath(artefactID string) string {
	return artefactsPathElement +
		"/" + artefactID +
		"/" + b.JSONVersion
}

func (b *TModellingBusArtefactConnector) artefactsStateTopicPath(artefactID string) string {
	return b.artefactsTopicPath(artefactID) +
		"/" + artefactStatePathElement
}

func (b *TModellingBusArtefactConnector) artefactsUpdateTopicPath(artefactID string) string {
	return b.artefactsTopicPath(artefactID) +
		"/" + artefactUpdatePathElement
}

func (b *TModellingBusArtefactConnector) artefactsConsideringTopicPath(artefactID string) string {
	return b.artefactsTopicPath(artefactID) +
		"/" + artefactConsideringPathElement
}

type TMQTTDelta struct {
	Operations     json.RawMessage `json:"operations"`
	Timestamp      string          `json:"timestamp"`
	StateTimestamp string          `json:"state timestamp"`
}

func (b *TModellingBusArtefactConnector) postartefactsJSONDelta(artefactsDeltaTopicPath string, oldStateJSON, newStateJSON []byte, err error) {
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong when converting to JSON", err)
		return
	}

	deltaOperationsJSON, err := jsonDiff(oldStateJSON, newStateJSON)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong running the JSON diff", err)
		return
	}

	delta := TMQTTDelta{}
	delta.Timestamp = GetTimestamp()
	delta.StateTimestamp = b.Timestamp
	delta.Operations = deltaOperationsJSON

	deltaJSON, err := json.Marshal(delta)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong JSONing the diff patch", err)
		return
	}

	b.ModellingBusConnector.postJSON(artefactsDeltaTopicPath, b.JSONVersion, deltaJSON, delta.Timestamp)
}

func (b *TModellingBusArtefactConnector) processartefactsJSONDeltaPosting(currentJSONState json.RawMessage, deltaJSON []byte) (json.RawMessage, bool) {
	delta := TMQTTDelta{}
	err := json.Unmarshal(deltaJSON, &delta)
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong unJSONing the received diff patch", err)
		return currentJSONState, false
	}

	if delta.StateTimestamp != b.Timestamp {
		b.ModellingBusConnector.errorReporter("Received update out of order", nil)
		return currentJSONState, false
	}

	newJSONState, err := jsonApplyPatch(currentJSONState, delta.Operations)
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

func (b *TModellingBusArtefactConnector) Initialise(ModellingBusConnector TModellingBusConnector, JSONVersion string) {
	b.ModellingBusConnector = ModellingBusConnector
	b.JSONVersion = JSONVersion
	b.ArtefactCurrentContent = []byte{}
	b.ArtefactUpdatedContent = []byte{}
	b.ArtefactConsideredContent = []byte{}
	b.Timestamp = GetTimestamp()
	b.stateCommunicated = false
}

func CreateModellingBusModelConnector(ModellingBusConnector TModellingBusConnector, modelJSONVersion string) TModellingBusArtefactConnector {
	ModellingBusModelConnector := TModellingBusArtefactConnector{}
	ModellingBusModelConnector.Initialise(ModellingBusConnector, modelJSONVersion)

	return ModellingBusModelConnector
}

/*
 * Posting
 */

func (b *TModellingBusArtefactConnector) PrepareForPosting(ArtefactID string) {
	b.ArtefactID = ArtefactID
}

func (b *TModellingBusArtefactConnector) PostConsidering(consideringStateJSON []byte, err error) {
	if b.stateCommunicated {
		b.ArtefactConsideredContent = consideringStateJSON

		b.postartefactsJSONDelta(b.artefactsUpdateTopicPath(b.ArtefactID), b.ArtefactCurrentContent, b.ArtefactUpdatedContent, err)
		b.postartefactsJSONDelta(b.artefactsConsideringTopicPath(b.ArtefactID), b.ArtefactUpdatedContent, b.ArtefactConsideredContent, err)
	} else {
		b.ModellingBusConnector.errorReporter("We must always see a state posting, before a considering posting!", nil)
	}
}

func (b *TModellingBusArtefactConnector) PostUpdate(updatedStateJSON []byte, err error) {
	if b.stateCommunicated {
		b.ArtefactUpdatedContent = updatedStateJSON

		b.postartefactsJSONDelta(b.artefactsUpdateTopicPath(b.ArtefactID), b.ArtefactCurrentContent, b.ArtefactUpdatedContent, err)
	} else {
		b.PostState(updatedStateJSON, err)
	}
}

func (b *TModellingBusArtefactConnector) PostState(stateJSON []byte, err error) {
	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong when converting to JSON", err)
		return
	}

	b.Timestamp = GetTimestamp()
	b.ArtefactCurrentContent = stateJSON

	if err != nil {
		b.ModellingBusConnector.errorReporter("Something went wrong JSONing the model data", err)
		return
	}

	b.ModellingBusConnector.postJSON(b.artefactsStateTopicPath(b.ArtefactID), b.JSONVersion, stateJSON, b.Timestamp)
	b.stateCommunicated = true
}

/*
 * Listening
 */

//listenForJSONPostings(agentID, topicPath string, postingHandler func([]byte))

func (b *TModellingBusArtefactConnector) ListenToStatePostings(agentID, ArtefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsStateTopicPath(ArtefactID), func(json []byte, timestamp string) {
		b.ArtefactCurrentContent = json
		b.ArtefactUpdatedContent = json
		b.ArtefactConsideredContent = json
		b.Timestamp = timestamp

		handler()
	})
}

func (b *TModellingBusArtefactConnector) ListenToUpdatePostings(agentID, ArtefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsUpdateTopicPath(ArtefactID), func(json []byte, timestamp string) {
		ok := false
		b.ArtefactUpdatedContent, ok = b.processartefactsJSONDeltaPosting(b.ArtefactCurrentContent, json)
		if ok {
			b.ArtefactConsideredContent = b.ArtefactUpdatedContent

			handler()
		} else {
			fmt.Println("Something went wrong ... yeah .. fix this message")
		}
	})
}

func (b *TModellingBusArtefactConnector) ListenToConsideringPostings(agentID, ArtefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsConsideringTopicPath(ArtefactID), func(json []byte, timestamp string) {
		ok := false
		b.ArtefactConsideredContent, ok = b.processartefactsJSONDeltaPosting(b.ArtefactUpdatedContent, json)
		if ok {
			handler()
		} else {
			fmt.Println("Something went wrong ... yeah .. fix this message")
		}
	})
}
