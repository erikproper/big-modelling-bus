/*
 *
 * Package: cdm
 * Module:  cdm
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.10.2025
 *
 */

package cdm

import (
	"encoding/json"
	"modelling_bus_1.0/mbconnect"
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
		ModelName                    string                                   `json:"model name"`
		ModellingBusArtefactReporter mbconnect.TModellingBusArtefactConnector `json:"-"`
		TypeIDCount                  int                                      `json:"-"`
		InstanceIDCount              int                                      `json:"-"`

		// For types
		TypeName map[string]string `json:"type names"`

		LastNewID string `json:"-"`

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
		RelationTypes                  map[string]bool             `json:"relation types"`
		InvolvementTypesOfRelationType map[string]map[string]bool  `json:"involvement types of relation types"`
		ReadingsOfRelationType         map[string]map[string]bool  `json:"readings of relation types"`
		ReadingDefinition              map[string]TRelationReading `json:"reading definition"`
	}
)

func (m *TCDMModel) Clean() {
	m.ModelName = ""
	m.LastNewID = ""
	m.ConcreteIndividualTypes = map[string]bool{}
	m.QualityTypes = map[string]bool{}
	m.RelationTypes = map[string]bool{}
	m.InvolvementTypes = map[string]bool{}

	m.TypeName = map[string]string{}
	m.DomainOfQualityType = map[string]string{}
	m.BaseTypeOfInvolvementType = map[string]string{}
	m.RelationTypeOfInvolvementType = map[string]string{}
	m.InvolvementTypesOfRelationType = map[string]map[string]bool{}
	m.ReadingsOfRelationType = map[string]map[string]bool{}
	m.ReadingDefinition = map[string]TRelationReading{}
}

func (m *TCDMModel) NewElementID() string {
	// Check should be at the busconnector level ...
	return m.ModellingBusArtefactReporter.ModellingBusConnector.GetNewID()
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

	m.ReadingsOfRelationType[id] = map[string]bool{}

	return id
}

func (m *TCDMModel) AddRelationTypeReading(relationType string, stringsAndInvolvementTypes ...string) {
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
	m.ReadingsOfRelationType[relationType][readingID] = true
	m.ReadingDefinition[readingID] = reading

	// Does require a check to see if all InvolvementTypesss of the relation have been used ... and used only once
	// But ... as this is only "Hello World" for now, we won't do so yet.
}

/*
 *
 * Initialisation and creation
 *
 */

func (m *TCDMModel) Initialise() {
	m.Clean()
}

func CreateCDMModel() TCDMModel {
	CDMModel := TCDMModel{}
	CDMModel.Clean()

	return CDMModel
}

/*
 *
 * Posting the model to the bus
 *
 */

func CreateCDMPoster(ModellingBusConnector mbconnect.TModellingBusConnector, modelID string) TCDMModel {
	CDMPosterModel := CreateCDMModel()

	CDMPosterModel.ModellingBusArtefactReporter = mbconnect.CreateModellingBusModelConnector(ModellingBusConnector, ModelJSONVersion)
	CDMPosterModel.ModellingBusArtefactReporter.PrepareForPosting(modelID)

	return CDMPosterModel
}

func (m *TCDMModel) ConnectoToBus(ModellingBusConnector mbconnect.TModellingBusConnector, modelID string) {
	m.ModellingBusArtefactReporter.Initialise(ModellingBusConnector, ModelJSONVersion)
	m.ModellingBusArtefactReporter.PrepareForPosting(modelID)
}

func (m *TCDMModel) PostState() {
	m.ModellingBusArtefactReporter.PostState(json.Marshal(m))
}

func (m *TCDMModel) PostUpdate() {
	m.ModellingBusArtefactReporter.PostUpdate(json.Marshal(m))
}

func (m *TCDMModel) PostConsidering() {
	m.ModellingBusArtefactReporter.PostConsidering(json.Marshal(m))
}

/*
 *
 * Reading models from the bus
 *
 */

func CreateCDMListener(ModellingBusConnector mbconnect.TModellingBusConnector) mbconnect.TModellingBusArtefactConnector {
	ModellingBusCDMModelListener := mbconnect.CreateModellingBusModelConnector(ModellingBusConnector, ModelJSONVersion)

	return ModellingBusCDMModelListener
}

func (m *TCDMModel) GetStateFromBus(bus mbconnect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(bus.ArtefactCurrentContent, m)

	return err == nil
}

func (m *TCDMModel) GetUpdatedFromBus(bus mbconnect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(bus.ArtefactUpdatedContent, m)

	return err == nil
}

func (m *TCDMModel) GetConsideredFromBus(bus mbconnect.TModellingBusArtefactConnector) bool {
	m.Clean()
	err := json.Unmarshal(bus.ArtefactConsideredContent, m)

	return err == nil
}
