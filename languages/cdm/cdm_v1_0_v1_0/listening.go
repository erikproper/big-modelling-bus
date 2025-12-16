/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Languages/Conceptual Domain Modelling, Version 1
 * Component: Definition
 *
 * This component provides the functionality fto listen for updates of
 * models expressed in the
 *    Conceptual Domain Modelling language, Version 1,
 * on the BIG Modelling Bus.
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 15.12.2025
 *
 */

package cdm_v1_0_v1_0

import (
	"github.com/erikproper/big-modelling-bus.go.v1/connect"
	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

/*
 * Definition of the CDM model listener
 */

type (
	TCDMModelListener struct {
		ModelListener connect.TModellingBusArtefactConnector

		CurrentModel    TCDMModel
		UpdatedModel    TCDMModel
		ConsideredModel TCDMModel
	}
)

/*
 * Getting model versions from the modelling bus
 */

// Updating all models from the modelling bus
func (l *TCDMModelListener) UpdateModelsFromBus() {
	l.CurrentModel.SetModelFromJSON(l.ModelListener.CurrentContent)
	l.UpdatedModel.SetModelFromJSON(l.ModelListener.UpdatedContent)
	l.ConsideredModel.SetModelFromJSON(l.ModelListener.ConsideredContent)
}

// // HERE

// Listening for model state postings on the modelling bus
func (l *TCDMModelListener) ListenForModelStatePostings(agentID, modelID string, handler func()) {
	l.Reporter.Progress(generics.ProgressLevelBasic, "Listening for model state postings for model %s from agent %s", modelID, agentID)
	l.ModelListener.ListenForJSONArtefactStatePostings(agentID, modelID, func() {
		l.UpdateModelsFromBus()
		handler()
	})
}

// Listening for model update postings on the modelling bus
func (l *TCDMModelListener) ListenForModelUpdatePostings(agentID, modelID string, handler func()) {
	l.ModelListener.ListenForJSONArtefactUpdatePostings(agentID, modelID, func() {
		l.UpdateModelsFromBus()
		handler()
	})
}

// Listening for model considering postings on the modelling bus
func (l *TCDMModelListener) ListenForModelConsideringPostings(agentID, modelID string, handler func()) {
	l.ModelListener.ListenForJSONArtefactConsideringPostings(agentID, modelID, func() {
		l.UpdateModelsFromBus()
		handler()
	})
}

/*
 *  Aggregate data across the model versions
 */

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
