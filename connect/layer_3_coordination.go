/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Connect
 * Component: Layer 3 - Coordination
 *
 * This module implements the coordination related functionality of the modelling bus.
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 05.12.2025
 *
 */

package connect

import "github.com/erikproper/big-modelling-bus.go.v1/generics"

/*
 * Defining constants
 */

const (
	coordinationPathElement = "coordination"
)

/*
 * Defining topic paths
 */

// coordinationTopicPath defines the topic path for coordination messages.
func (b *TModellingBusConnector) coordinationTopicPath(coordinationID string) string {
	return coordinationPathElement + "/" + coordinationID
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Posting coordination messages
 */

// Post a coordination message to the modelling bus
func (b *TModellingBusConnector) PostCoordination(coordinationID string, json []byte) {
	b.postJSONAsStreamed(b.coordinationTopicPath(coordinationID), json, generics.GetTimestamp())
}

/*
 * Listening to coordination related postings
 */

// Listen for coordination postings on the modelling bus
func (b *TModellingBusConnector) ListenForCoordinationPostings(agentID, coordinationID string, postingHandler func([]byte, string)) {
	b.listenForStreamedPostings(agentID, b.coordinationTopicPath(coordinationID), postingHandler)
}

/*
 * Retrieving coordination messages
 */

// Retrieve coordination messages from the modelling bus
func (b *TModellingBusConnector) GetCoordination(agentID, coordinationID string) ([]byte, string) {
	return b.getStreamedEvent(agentID, b.coordinationTopicPath(coordinationID))
}

/*
 * Deleting coordination messages
 */

// Delete coordination messages from the modelling bus
func (b *TModellingBusConnector) DeleteCoordination(coordinationID string) {
	b.deletePosting(b.coordinationTopicPath(coordinationID))
}
