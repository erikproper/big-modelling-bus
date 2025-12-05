/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Languages/Conceptual Domain Modelling, Version 1
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.11.2025
 *
 */

package cdm_v1

import (
	"encoding/json"

	"github.com/erikproper/big-modelling-bus.go.v1/connect"
	"github.com/erikproper/big-modelling-bus.go.v1/generics"
)

const (
	ModelJSONVersion = "cdm-1.0-1.0"
)

type (
	TRelationReading struct {
		InvolvementTypes []string `json:"involvement types"`
		ReadingElements  []string `json:"reading elements"`
	}

	TCDMModel struct {
		ModelName                  string                                 `json:"model name"`
		ModellingBusArtefactPoster connect.TModellingBusArtefactConnector `json:"-"`
		TypeIDCount                int                                    `json:"-"`
		InstanceIDCount            int                                    `json:"-"`

		// For types
		TypeName map[string]string `json:"type names"`

		// For concrete individual types
		ConcreteIndividualTypes map[string]bool `json:"concrete individual types"`

		// For quality types
		QualityTypes        map[string]bool   `json:"quality types"`
		DomainOfQualityType map[string]string `json:"domains of quality types"`

		// For involvement types
		InvolvementTypes              map[string]bool   `json:"involvement types"`
		BaseTypeOfInvolvementType     map[string]string `json:"base types of involvement types"`
		RelationTypeOfInvolvementType map[string]string `json:"relation types of involvement types"`

		// For relation types
		RelationTypes                     map[string]bool             `json:"relation types"`
		InvolvementTypesOfRelationType    map[string]map[string]bool  `json:"involvement types of relation types"`
		AlternativeReadingsOfRelationType map[string]map[string]bool  `json:"alternative readings of relation types"`
		PrimaryReadingOfRelationType      map[string]string           `json:"primary readings of relation types"`
		ReadingDefinition                 map[string]TRelationReading `json:"reading definition"`
	}
)

func (m *TCDMModel) Clean() {
	m.ModelName = ""
	m.ConcreteIndividualTypes = map[string]bool{}
	m.QualityTypes = map[string]bool{}
	m.RelationTypes = map[string]bool{}
	m.InvolvementTypes = map[string]bool{}

	m.TypeName = map[string]string{}
	m.DomainOfQualityType = map[string]string{}
	m.BaseTypeOfInvolvementType = map[string]string{}
	m.RelationTypeOfInvolvementType = map[string]string{}
	m.InvolvementTypesOfRelationType = map[string]map[string]bool{}
	m.AlternativeReadingsOfRelationType = map[string]map[string]bool{}
	m.PrimaryReadingOfRelationType = map[string]string{}
	m.ReadingDefinition = map[string]TRelationReading{}
}

func (m *TCDMModel) NewElementID() string {
	return generics.GetTimestamp()
}

func (m *TCDMModel) SetModelName(name string) {
	m.ModelName = name
}

func (m *TCDMModel) AddConcreteIndividualType(name string) string {
	id := m.NewElementID()
	m.ConcreteIndividualTypes[id] = true
	m.TypeName[id] = name

	return id
}

func (m *TCDMModel) AddQualityType(name, domain string) string {
	id := m.NewElementID()
	m.QualityTypes[id] = true
	m.TypeName[id] = name
	m.DomainOfQualityType[id] = domain

	return id
}

func (m *TCDMModel) AddInvolvementType(name string, base string) string {
	id := m.NewElementID()
	m.InvolvementTypes[id] = true
	m.TypeName[id] = name
	m.BaseTypeOfInvolvementType[id] = base

	return id
}

func (m *TCDMModel) AddRelationType(name string, involvementTypes ...string) string {
	id := m.NewElementID()
	m.RelationTypes[id] = true
	m.TypeName[id] = name

	m.InvolvementTypesOfRelationType[id] = map[string]bool{}
	for _, involvementType := range involvementTypes {
		m.RelationTypeOfInvolvementType[involvementType] = id
		m.InvolvementTypesOfRelationType[id][involvementType] = true
	}

	m.AlternativeReadingsOfRelationType[id] = map[string]bool{}

	return id
}

func (m *TCDMModel) AddRelationTypeReading(relationType string, stringsAndInvolvementTypes ...string) string {
	reading := TRelationReading{}

	isReadingString := true
	for _, element := range stringsAndInvolvementTypes {
		if isReadingString {
			reading.ReadingElements = append(reading.ReadingElements, element)
		} else {
			reading.InvolvementTypes = append(reading.InvolvementTypes, element)
		}
		isReadingString = !isReadingString
	}

	readingID := m.NewElementID()
	m.AlternativeReadingsOfRelationType[relationType][readingID] = true
	m.ReadingDefinition[readingID] = reading

	if m.PrimaryReadingOfRelationType[relationType] == "" {
		m.PrimaryReadingOfRelationType[relationType] = readingID
	}

	return readingID
	// Does require a check to see if all InvolvementTypesss of the relation have been used ... and used only once
	// But ... as this is only "Hello World" for now, so we won't do so yet.
}

/*
 *
 * Initialisation and creation
 *
 */

func CreateCDMModel() TCDMModel {
	CDMModel := TCDMModel{}
	CDMModel.Clean()

	return CDMModel
}

/*
 *
 * Posting models to the artefactBus
 *
 */

func CreateCDMPoster(ModellingBusConnector connect.TModellingBusConnector, modelID string) TCDMModel {
	CDMPosterModel := CreateCDMModel()

	// Note: One ModellingBusConnector can be used for different artefacts with different json versions.
	CDMPosterModel.ModellingBusArtefactPoster = connect.CreateModellingBusArtefactConnector(ModellingBusConnector, ModelJSONVersion)
	CDMPosterModel.ModellingBusArtefactPoster.PrepareForPosting(modelID)

	return CDMPosterModel
}

func (m *TCDMModel) PostState() {
	m.ModellingBusArtefactPoster.PostJSONArtefactState(json.Marshal(m))
}

func (m *TCDMModel) PostUpdate() {
	m.ModellingBusArtefactPoster.PostUpdateJSONArtefact(json.Marshal(m))
}

func (m *TCDMModel) PostConsidering() {
	m.ModellingBusArtefactPoster.PostConsideringJSONArtefact(json.Marshal(m))
}

/*
 *
 * Reading models from the artefactBus
 *
 */

// Note: One ModellingBusConnector can be used for different models of different kinds.
func CreateCDMListener(ModellingBusConnector connect.TModellingBusConnector) connect.TModellingBusArtefactConnector {
	ModellingBusCDMModelListener := connect.CreateModellingBusArtefactConnector(ModellingBusConnector, ModelJSONVersion)

	return ModellingBusCDMModelListener
}

func (m *TCDMModel) GetStateFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(artefactBus.CurrentContent, m)

	return err == nil
}

func (m *TCDMModel) GetUpdatedFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(artefactBus.UpdatedContent, m)

	return err == nil
}

func (m *TCDMModel) GetConsideredFromBus(artefactBus connect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(artefactBus.ConsideredContent, m)

	return err == nil
}
