/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Generic
 * Component: Layer 3 - Observation
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 05.12.2025
 *
 */

package connect

import (
	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

const (
	rawObservationsPathElement      = "observations/raw"
	jsonObservationsPathElement     = "observations/json"
	streamedObservationsPathElement = "observations/streamed"
)

/*
 * Defining topic paths
 */

func (b *TModellingBusConnector) rawObservationsTopicPath(observationID string) string {
	return rawObservationsPathElement +
		"/" + observationID
}

func (b *TModellingBusConnector) jsonObservationsTopicPath(observationID string) string {
	return jsonObservationsPathElement +
		"/" + observationID
}

func (b *TModellingBusConnector) streamedObservationsTopicPath(observationID string) string {
	return streamedObservationsPathElement +
		"/" + observationID
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Posting observations
 */

func (b *TModellingBusConnector) PostRawObservation(observationID, localFilePath string) {
	b.postFile(b.rawObservationsTopicPath(observationID), localFilePath, generics.GetTimestamp())
}

func (b *TModellingBusConnector) PostJSONObservation(observationID string, json []byte) {
	b.postJSONAsFile(b.jsonObservationsTopicPath(observationID), json, generics.GetTimestamp())
}

func (b *TModellingBusConnector) PostStreamedObservation(observationID string, json []byte) {
	b.postJSONAsStreamed(b.streamedObservationsTopicPath(observationID), json, generics.GetTimestamp())
}

/*
 * Listening to observations related postings
 */

func (b *TModellingBusConnector) ListenForRawObservationPostings(agentID, observationID string, postingHandler func(string)) {
	b.listenForFilePostings(agentID, b.rawObservationsTopicPath(observationID), generics.JSONFileName, func(localFilePath, _ string) {
		postingHandler(localFilePath)
	})
}

func (b *TModellingBusConnector) ListenForJSONObservationPostings(agentID, observationID string, postingHandler func([]byte, string)) {
	b.listenForFilePostings(agentID, b.jsonObservationsTopicPath(observationID), generics.JSONFileName, func(localFilePath, timestamp string) {
		postingHandler(b.getJSONFromTemporaryFile(localFilePath, timestamp))
	})
}

//func (b *TModellingBusConnector) PostStreamedObservation(observationID string, json []byte) {
//	b.postJSONAsStreamed(b.streamedObservationsTopicPath(observationID), json, generics.GetTimestamp())
//}

/*
 * Retrieving observations
 */

func (b *TModellingBusConnector) GetRawObservation(agentID, observationID, localFileName string) (string, string) {
	return b.getFileFromPosting(agentID, b.rawObservationsTopicPath(observationID), localFileName)
}

func (b *TModellingBusConnector) GetJSONObservation(agentID, observationID string) ([]byte, string) {
	return b.getJSON(agentID, b.jsonObservationsTopicPath(observationID))
}

func (b *TModellingBusConnector) GetStreamedObservation(agentID, observationID string) ([]byte, string) {
	return b.getStreamed(agentID, b.streamedObservationsTopicPath(observationID))
}

/*
 * Deleting observations
 */

func (b *TModellingBusConnector) DeleteRawObservation(observationID string) {
	b.deletePosting(b.rawObservationsTopicPath(observationID))
}

func (b *TModellingBusConnector) DeleteJSONObservation(observationID string) {
	b.deletePosting(b.jsonObservationsTopicPath(observationID))
}
