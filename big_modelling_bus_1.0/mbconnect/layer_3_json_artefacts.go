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
		JSONVersion           string `json:"json version"`
		ArtefactID            string `json:"artefact id"`
		CurrentTimestamp      string `json:"current timestamp"`

		CurrentContent    json.RawMessage `json:"content"`
		UpdatedContent    json.RawMessage `json:"-"`
		ConsideredContent json.RawMessage `json:"-"`

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

type TJSONDelta struct {
	Operations       json.RawMessage `json:"operations"`
	Timestamp        string          `json:"timestamp"`
	CurrentTimestamp string          `json:"current timestamp"`
}

func (b *TModellingBusArtefactConnector) postDelta(deltaTopicPath string, oldStateJSON, newStateJSON []byte, err error) {
	// Can we avoid dragging the err in here??
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Something went wrong when converting to JSON. %s", err)
		return
	}

	deltaOperationsJSON, err := jsonDiff(oldStateJSON, newStateJSON)
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Something went wrong running the JSON diff. %s", err)
		return
	}

	delta := TJSONDelta{}
	delta.Timestamp = GetTimestamp()
	delta.CurrentTimestamp = b.CurrentTimestamp
	delta.Operations = deltaOperationsJSON

	deltaJSON, err := json.Marshal(delta)
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Something went wrong JSONing the diff patch. %s", err)
		return
	}

	b.ModellingBusConnector.postJSON(deltaTopicPath, b.JSONVersion, deltaJSON, delta.Timestamp)
}

func (b *TModellingBusArtefactConnector) applyDelta(currentJSONState json.RawMessage, deltaJSON []byte) (json.RawMessage, bool) {
	delta := TJSONDelta{}
	err := json.Unmarshal(deltaJSON, &delta)
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Something went wrong unJSONing the received diff patch. %s", err)
		return currentJSONState, false
	}

	if delta.CurrentTimestamp != b.CurrentTimestamp {
		return currentJSONState, false
	}

	newJSONState, err := jsonApplyPatch(currentJSONState, delta.Operations)
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Applying patch didn't work. %s", err)
		return currentJSONState, false
	}

	return newJSONState, true
}

func (b *TModellingBusArtefactConnector) updateCurrent(json []byte, currentTimestamp string) {
	b.CurrentContent = json
	b.UpdatedContent = json
	b.ConsideredContent = json
	b.CurrentTimestamp = currentTimestamp
}

func (b *TModellingBusArtefactConnector) updateUpdated(json []byte, _ ...string) bool {
	ok := false
	b.UpdatedContent, ok = b.applyDelta(b.CurrentContent, json)
	if ok {
		b.ConsideredContent = b.UpdatedContent
	}

	return ok
}

func (b *TModellingBusArtefactConnector) updateConsidering(json []byte, _ ...string) bool {
	ok := false
	b.ConsideredContent, ok = b.applyDelta(b.UpdatedContent, json)

	return ok
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Posting
 */

func (b *TModellingBusArtefactConnector) PrepareForPosting(ArtefactID string) {
	b.ArtefactID = ArtefactID
}

func (b *TModellingBusArtefactConnector) PostConsidering(consideringStateJSON []byte, err error) {
	if !b.stateCommunicated {
		b.PostState(b.CurrentContent, err)
	}

	b.ConsideredContent = consideringStateJSON

	b.postDelta(b.artefactsConsideringTopicPath(b.ArtefactID), b.UpdatedContent, b.ConsideredContent, err)
}

func (b *TModellingBusArtefactConnector) PostUpdate(updatedStateJSON []byte, err error) {
	if !b.stateCommunicated {
		b.PostState(updatedStateJSON, err)
	}

	b.UpdatedContent = updatedStateJSON
	b.ConsideredContent = updatedStateJSON

	b.postDelta(b.artefactsUpdateTopicPath(b.ArtefactID), b.CurrentContent, b.UpdatedContent, err)
}

func (b *TModellingBusArtefactConnector) PostState(stateJSON []byte, err error) {
	if err != nil {
		b.ModellingBusConnector.reporter.Error("Something went wrong when converting to JSON. %s", err)
		return
	}

	b.CurrentTimestamp = GetTimestamp()
	b.CurrentContent = stateJSON
	b.UpdatedContent = stateJSON
	b.ConsideredContent = stateJSON

	b.ModellingBusConnector.postJSON(b.artefactsStateTopicPath(b.ArtefactID), b.JSONVersion, b.CurrentContent, b.CurrentTimestamp)

	b.stateCommunicated = true
}

/*
 * Current state listening & getting
 */

func (b *TModellingBusArtefactConnector) ListenForStatePostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsStateTopicPath(artefactID), func(json []byte, currentTimestamp string) {
		b.updateCurrent(json, currentTimestamp)
		handler()
	})
}

func (b *TModellingBusArtefactConnector) GetState(agentID, artefactID string) {
	b.updateCurrent(b.ModellingBusConnector.getJSON(agentID, b.artefactsStateTopicPath(artefactID)))
}

/*
 * Updated state listening & getting
 */

func (b *TModellingBusArtefactConnector) ListenForUpdatePostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsUpdateTopicPath(artefactID), func(json []byte, _ string) {
		if b.updateUpdated(json) {
			handler()
		}
	})
}

func (b *TModellingBusArtefactConnector) GetUpdate(agentID, artefactID string) {
	b.GetState(agentID, artefactID)

	b.updateUpdated(b.ModellingBusConnector.getJSON(agentID, b.artefactsUpdateTopicPath(artefactID)))
}

/*
 * Considered state listening & getting
 */

func (b *TModellingBusArtefactConnector) ListenForConsideringPostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.artefactsConsideringTopicPath(artefactID), func(json []byte, _ string) {
		if b.updateConsidering(json) {
			handler()
		}
	})
}

func (b *TModellingBusArtefactConnector) GetConsidering(agentID, artefactID string) {
	b.GetUpdate(agentID, artefactID)

	b.updateConsidering(b.ModellingBusConnector.getJSON(agentID, b.artefactsConsideringTopicPath(artefactID)))
}

/*
 * Creation
 */

func CreateModellingBusArtefactConnector(ModellingBusConnector TModellingBusConnector, JSONVersion string) TModellingBusArtefactConnector {
	ModellingBusArtefactConnector := TModellingBusArtefactConnector{}
	ModellingBusArtefactConnector.ModellingBusConnector = ModellingBusConnector
	ModellingBusArtefactConnector.JSONVersion = JSONVersion
	ModellingBusArtefactConnector.CurrentContent = []byte{}
	ModellingBusArtefactConnector.UpdatedContent = []byte{}
	ModellingBusArtefactConnector.ConsideredContent = []byte{}
	ModellingBusArtefactConnector.CurrentTimestamp = GetTimestamp()
	ModellingBusArtefactConnector.stateCommunicated = false

	return ModellingBusArtefactConnector
}
