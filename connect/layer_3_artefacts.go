/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Generic
 * Component: Layer 3 - Artefacts
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

	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

const (
	jsonArtefactsPathElement = "artefacts/json"
	rawArtefactsPathElement  = "artefacts/raw"

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

func (b *TModellingBusArtefactConnector) rawArtefactsTopicPath(artefactID string) string {
	return rawArtefactsPathElement +
		"/" + artefactID
}

func (b *TModellingBusArtefactConnector) jsonArtefactsTopicPath(artefactID string) string {
	return jsonArtefactsPathElement +
		"/" + artefactID +
		"/" + b.JSONVersion
}

func (b *TModellingBusArtefactConnector) jsonArtefactsStateTopicPath(artefactID string) string {
	return b.jsonArtefactsTopicPath(artefactID) +
		"/" + artefactStatePathElement
}

func (b *TModellingBusArtefactConnector) jsonArtefactsUpdateTopicPath(artefactID string) string {
	return b.jsonArtefactsTopicPath(artefactID) +
		"/" + artefactUpdatePathElement
}

func (b *TModellingBusArtefactConnector) jsonArtefactsConsideringTopicPath(artefactID string) string {
	return b.jsonArtefactsTopicPath(artefactID) +
		"/" + artefactConsideringPathElement
}

/*
 *
 * Internal functionality
 *
 */

type TJSONDelta struct {
	Operations       json.RawMessage `json:"operations"`
	Timestamp        string          `json:"timestamp"`
	CurrentTimestamp string          `json:"current timestamp"`
}

func (b *TModellingBusArtefactConnector) postJSONDelta(deltaTopicPath string, oldStateJSON, newStateJSON []byte, err error) {
	// Can we avoid dragging the err in here??
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Something went wrong when converting to JSON. %s", err)
		return
	}

	deltaOperationsJSON, err := generics.JSONDiff(oldStateJSON, newStateJSON)
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Something went wrong running the JSON diff. %s", err)
		return
	}

	delta := TJSONDelta{}
	delta.Timestamp = generics.GetTimestamp()
	delta.CurrentTimestamp = b.CurrentTimestamp
	delta.Operations = deltaOperationsJSON

	deltaJSON, err := json.Marshal(delta)
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Something went wrong JSONing the diff patch. %s", err)
		return
	}

	b.ModellingBusConnector.postJSON(deltaTopicPath, deltaJSON, delta.Timestamp)
}

func (b *TModellingBusArtefactConnector) applyJSONDelta(currentJSONState json.RawMessage, deltaJSON []byte) (json.RawMessage, bool) {
	delta := TJSONDelta{}
	err := json.Unmarshal(deltaJSON, &delta)
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Something went wrong unJSONing the received diff patch. %s", err)
		return currentJSONState, false
	}

	if delta.CurrentTimestamp != b.CurrentTimestamp {
		return currentJSONState, false
	}

	newJSONState, err := generics.JSONApplyPatch(currentJSONState, delta.Operations)
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Applying patch didn't work. %s", err)
		return currentJSONState, false
	}

	return newJSONState, true
}

func (b *TModellingBusArtefactConnector) updateCurrentJSON(json []byte, currentTimestamp string) {
	b.CurrentContent = json
	b.UpdatedContent = json
	b.ConsideredContent = json
	b.CurrentTimestamp = currentTimestamp
}

func (b *TModellingBusArtefactConnector) updateUpdatedJSON(json []byte, _ ...string) bool {
	ok := false
	b.UpdatedContent, ok = b.applyJSONDelta(b.CurrentContent, json)
	if ok {
		b.ConsideredContent = b.UpdatedContent
	}

	return ok
}

func (b *TModellingBusArtefactConnector) updateConsideringJSON(json []byte, _ ...string) bool {
	ok := false
	b.ConsideredContent, ok = b.applyJSONDelta(b.UpdatedContent, json)

	return ok
}

func (b *TModellingBusArtefactConnector) foundJSONIssue(err error) bool {
	if err != nil {
		b.ModellingBusConnector.Reporter.Error("Something went wrong when converting to JSON. %s", err)
		return true
	}

	return false
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

func (b *TModellingBusArtefactConnector) PostRawArtefactState(topicPath, localFilePath string) {
	b.ModellingBusConnector.postFile(b.rawArtefactsTopicPath(b.ArtefactID), localFilePath)
}

func (b *TModellingBusArtefactConnector) PostJSONArtefactState(stateJSON []byte, err error) {
	if b.foundJSONIssue(err) {
		return
	}

	b.CurrentTimestamp = generics.GetTimestamp()
	b.CurrentContent = stateJSON
	b.UpdatedContent = stateJSON
	b.ConsideredContent = stateJSON

	b.ModellingBusConnector.postJSON(b.jsonArtefactsStateTopicPath(b.ArtefactID), b.CurrentContent, b.CurrentTimestamp)

	b.stateCommunicated = true
}

func (b *TModellingBusArtefactConnector) PostUpdateJSONArtefact(updatedStateJSON []byte, err error) {
	if b.foundJSONIssue(err) {
		return
	}

	if !b.stateCommunicated {
		b.PostJSONArtefactState(updatedStateJSON, err)
	}

	b.UpdatedContent = updatedStateJSON
	b.ConsideredContent = updatedStateJSON

	b.postJSONDelta(b.jsonArtefactsUpdateTopicPath(b.ArtefactID), b.CurrentContent, b.UpdatedContent, err)
}

func (b *TModellingBusArtefactConnector) PostConsideringJSONArtefact(consideringStateJSON []byte, err error) {
	if b.foundJSONIssue(err) {
		return
	}
	if !b.stateCommunicated {
		b.PostJSONArtefactState(b.CurrentContent, err)
	}

	b.ConsideredContent = consideringStateJSON

	b.postJSONDelta(b.jsonArtefactsConsideringTopicPath(b.ArtefactID), b.UpdatedContent, b.ConsideredContent, err)
}

/*
 * Current state listening
 */

// b.rawArtefactsTopicPath(b.ArtefactID)
func (b *TModellingBusArtefactConnector) ListenForRawArtefactPostings(agentID, artefactID string, postingHandler func(string)) {
	b.ModellingBusConnector.listenForFilePostings(agentID, b.rawArtefactsTopicPath(artefactID), generics.JSONFileName, func(localFilePath, _ string) {
		postingHandler(localFilePath)
	})
}

func (b *TModellingBusArtefactConnector) ListenForJSONStatePostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.jsonArtefactsStateTopicPath(artefactID), func(json []byte, currentTimestamp string) {
		b.updateCurrentJSON(json, currentTimestamp)
		handler()
	})
}

func (b *TModellingBusArtefactConnector) ListenForJSONUpdatePostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.jsonArtefactsUpdateTopicPath(artefactID), func(json []byte, _ string) {
		if b.updateUpdatedJSON(json) {
			handler()
		}
	})
}

func (b *TModellingBusArtefactConnector) ListenForJSONConsideringPostings(agentID, artefactID string, handler func()) {
	b.ModellingBusConnector.listenForJSONPostings(agentID, b.jsonArtefactsConsideringTopicPath(artefactID), func(json []byte, _ string) {
		if b.updateConsideringJSON(json) {
			handler()
		}
	})
}

/*
 * Current state getting
 */

func (b *TModellingBusArtefactConnector) GetRawArtefact(agentID, topicPath, localFileName string) string {
	localFilePath, _ := b.ModellingBusConnector.getFileFromPosting(agentID, topicPath, localFileName)
	return localFilePath
}

func (b *TModellingBusArtefactConnector) GetJSONState(agentID, artefactID string) {
	b.updateCurrentJSON(b.ModellingBusConnector.getJSON(agentID, b.jsonArtefactsStateTopicPath(artefactID)))
}

func (b *TModellingBusArtefactConnector) GetJSONUpdate(agentID, artefactID string) {
	b.GetJSONState(agentID, artefactID)

	b.updateUpdatedJSON(b.ModellingBusConnector.getJSON(agentID, b.jsonArtefactsUpdateTopicPath(artefactID)))
}

func (b *TModellingBusArtefactConnector) GetJSONConsidering(agentID, artefactID string) {
	b.GetJSONUpdate(agentID, artefactID)

	b.updateConsideringJSON(b.ModellingBusConnector.getJSON(agentID, b.jsonArtefactsConsideringTopicPath(artefactID)))
}

/*
 * Deleting
 */

func (b *TModellingBusArtefactConnector) DeleteRawArtefact(artefactID string) {
	b.ModellingBusConnector.deletePosting(b.rawArtefactsTopicPath(artefactID))
}

func (b *TModellingBusArtefactConnector) DeleteJSONArtefact(artefactID string) {
	b.ModellingBusConnector.deletePosting(b.jsonArtefactsTopicPath(artefactID))
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
	ModellingBusArtefactConnector.CurrentTimestamp = generics.GetTimestamp()
	ModellingBusArtefactConnector.stateCommunicated = false

	return ModellingBusArtefactConnector
}
