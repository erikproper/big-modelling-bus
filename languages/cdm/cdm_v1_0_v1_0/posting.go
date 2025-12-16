/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Languages/Conceptual Domain Modelling, Version 1
 * Component: Definition
 *
 * This component provides the functionality for models expressed in the
 *    Conceptual Domain Modelling language, Version 1,
 * to be posted on he BIG Modelling Bus.
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 15.12.2025
 *
 */

package cdm_v1_0_v1_0

import (
	"github.com/erikproper/big-modelling-bus.go.v1/connect"
)

// Posting functionalities for the CDM model
type (
	TCDMModelPoster struct {
		modelPoster connect.TModellingBusArtefactConnector // ???
	}
)

// Posting the model's state
func (p *TCDMModelPoster) PostState(m TCDMModel) {
	p.modelPoster.ModellingBusConnector.Reporter.Progress(1, "XXXX Posting model state for model %s as %s", m.ModelName, m.GetModelAsJSON())
	p.modelPoster.PostJSONArtefactState(m.GetModelAsJSON())
}

// Posting the model's update
func (p *TCDMModelPoster) PostUpdate(m TCDMModel) {
	p.modelPoster.PostJSONArtefactUpdate(m.GetModelAsJSON())
}

// Posting the model's considered update
func (p *TCDMModelPoster) PostConsidering(m TCDMModel) {
	p.modelPoster.PostJSONArtefactConsidering(m.GetModelAsJSON())
}

// Creating a CDM model poster, which uses a given ModellingBusConnector to post the model
func CreateCDMPoster(ModellingBusConnector connect.TModellingBusConnector, modelID string) TCDMModelPoster {
	// Setting up new CDM model poster
	cdmPosterModel := TCDMModelPoster{}
	cdmPosterModel.modelPoster = connect.CreateModellingBusArtefactConnector(ModellingBusConnector, ModelJSONVersion, modelID)

	// Return the created CDM model poster
	return cdmPosterModel
}
