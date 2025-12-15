package cdm_v1_0_v1_0

import (
	"encoding/json"

	"github.com/erikproper/big-modelling-bus.go.v1/connect"
	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

type (
	TCDMModelListener struct {
		ModelListener connect.TModellingBusArtefactConnector

		CurrentModel    TCDMModel
		UpdatedModel    TCDMModel
		ConsideredModel TCDMModel
	}
)

func (l *TCDMModelListener) UpdateModelsFromBus() {
	l.CurrentModel.GetStateFromBus(l.ModelListener)
	l.UpdatedModel.GetUpdatedFromBus(l.ModelListener)
	l.ConsideredModel.GetConsideredFromBus(l.ModelListener)
}

func (l *TCDMModelListener) UniteIDSets(mp func(TCDMModel) map[string]bool) map[string]bool {
	result := map[string]bool{}

	for e, c := range mp(l.CurrentModel) {
		if c {
			result[e] = true
		}
	}

	for e, c := range mp(l.UpdatedModel) {
		if c {
			result[e] = true
		}
	}

	for e, c := range mp(l.ConsideredModel) {
		if c {
			result[e] = true
		}
	}

	return result
}

/*
 *  Aggregate data across the model versions
 */

func (l *TCDMModelListener) QualityTypes() map[string]bool {
	return l.UniteIDSets(func(m TCDMModel) map[string]bool {
		return m.QualityTypes
	})
}

func (l *TCDMModelListener) ConcreteIndividualTypes() map[string]bool {
	return l.UniteIDSets(func(m TCDMModel) map[string]bool {
		return m.ConcreteIndividualTypes
	})
}

func (l *TCDMModelListener) RelationTypes() map[string]bool {
	return l.UniteIDSets(func(m TCDMModel) map[string]bool {
		return m.RelationTypes
	})
}

func (l *TCDMModelListener) InvolvementTypesOfRelationType(relationType string) map[string]bool {
	return l.UniteIDSets(func(m TCDMModel) map[string]bool {
		return m.InvolvementTypesOfRelationType[relationType]
	})
}

func (l *TCDMModelListener) AlternativeReadingsOfRelationType(relationType string) map[string]bool {
	return l.UniteIDSets(func(m TCDMModel) map[string]bool {
		return m.AlternativeReadingsOfRelationType[relationType]
	})
}

/*
 *  Creating and updating the model listener
 */

// Creating a CDM model listener, which uses a given ModellingBusConnector to listen for models and their updates
func CreateCDMListener(ModellingBusConnector connect.TModellingBusConnector, reporter *generics.TReporter) TCDMModelListener {
	// Setting up a new CDM model listener
	cdmModelListener := TCDMModelListener{}
	cdmModelListener.ModelListener = connect.CreateModellingBusArtefactConnector(ModellingBusConnector, ModelJSONVersion, "")
	cdmModelListener.CurrentModel = CreateCDMModel(reporter)
	cdmModelListener.UpdatedModel = CreateCDMModel(reporter)
	cdmModelListener.ConsideredModel = CreateCDMModel(reporter)

	// Return the created CDM model listener
	return cdmModelListener
}

// Updating the model's state from given JSON
func (m *TCDMModel) UpdateModelFromJSON(modelJSON json.RawMessage) bool {
	m.Clean()

	return m.reporter.MaybeReportError("Unmarshalling state content failed.", json.Unmarshal(modelJSON, m))
}

// Listening for model state postings on the modelling bus
func (m *TCDMModel) ListenForModelStatePostings(agentId, modelID string, handler func()) {
	m.ModelListener.ListenForJSONArtefactStatePostings(agentId, modelID, handler)
}

// Listening for model update postings on the modelling bus
func (m *TCDMModel) ListenForModelUpdatePostings(agentId, modelID string, handler func()) {
	m.ModelListener.ListenForJSONArtefactUpdatePostings(agentId, modelID, handler)
}

// Listening for model update postings on the modelling bus
func (m *TCDMModel) ListenForModelConsideringPostings(agentId, modelID string, handler func()) {
	m.ModelListener.ListenForJSONArtefactConsideringPostings(agentId, modelID, handler)
}

// Retrieving the model's state from the modelling bus
func (m *TCDMModel) GetStateFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	return m.UpdateModelFromJSON(artefactBus.CurrentContent)
}

// Retrieving the model's updated state from the modelling bus
func (m *TCDMModel) GetUpdatedFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	return m.UpdateModelFromJSON(artefactBus.UpdatedContent)
}

// Retrieving the model's considered state from the modelling bus
func (m *TCDMModel) GetConsideredFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	return m.UpdateModelFromJSON(artefactBus.ConsideredContent)
}
