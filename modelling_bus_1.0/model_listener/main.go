package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"modelling_bus_1.0/cdm"
	"modelling_bus_1.0/mbconnect"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	latexFileExtension = ".tex"
	defaultIni         = "mobile.ini"
)

func reportError(message string, err error) {
	fmt.Println(message+":", err)
}

type TCDMModelLaTeXWriter struct {
	CurrentModel,
	UpdatedModel,
	ConsideredModel cdm.TCDMModel

	CurrentReadings,
	UpdatedReadings,
	ConsideredReadings map[string]string

	latexFile,
	workFolder string

	LaTeXfile *os.File

	errorReporter mbconnect.TErrorReporter
}

/*
 *  Aggregate data across the model versions
 */

func (l *TCDMModelLaTeXWriter) MergeIDSets(mp func(cdm.TCDMModel) map[string]bool) map[string]bool {
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

func (l *TCDMModelLaTeXWriter) QualityTypes() map[string]bool {
	return l.MergeIDSets(func(m cdm.TCDMModel) map[string]bool {
		return m.QualityTypes
	})
}

func (l *TCDMModelLaTeXWriter) ConcreteIndividualTypes() map[string]bool {
	return l.MergeIDSets(func(m cdm.TCDMModel) map[string]bool {
		return m.ConcreteIndividualTypes
	})
}

func (l *TCDMModelLaTeXWriter) RelationTypes() map[string]bool {
	return l.MergeIDSets(func(m cdm.TCDMModel) map[string]bool {
		return m.RelationTypes
	})
}

func (l *TCDMModelLaTeXWriter) InvolvementTypesOfRelationType(relationType string) map[string]bool {
	return l.MergeIDSets(func(m cdm.TCDMModel) map[string]bool {
		return m.InvolvementTypesOfRelationType[relationType]
	})
}

func (l *TCDMModelLaTeXWriter) ReadingsOfRelationType(relationType string) map[string]bool {
	return l.MergeIDSets(func(m cdm.TCDMModel) map[string]bool {
		return m.ReadingsOfRelationType[relationType]
	})
}

/*
 *  Rendering elements across the model versions
 */

const (
	toAdd          = "{\\color{green} %s}"
	toDelete       = "{\\color{red} \\sout{\\sout{%s}}}"
	considerAdd    = "{\\color{lime} %s}"
	considerDelete = "{\\color{orange} \\sout{\\sout{%s}}}"
)

func ApplyFormatting(format, value string) string {
	if value == "" {
		return ""
	} else {
		return fmt.Sprintf(format, value)
	}
}

func (l *TCDMModelLaTeXWriter) RenderElement(s func(cdm.TCDMModel) string) string {
	current := s(l.CurrentModel)
	updated := s(l.UpdatedModel)
	considered := s(l.ConsideredModel)

	if considered == updated {
		if updated == current {
			return current
		} else {
			return ApplyFormatting(toDelete, current) + ApplyFormatting(toAdd, updated)
		}
	} else {
		if updated == current {
			return ApplyFormatting(considerDelete, updated) + ApplyFormatting(considerAdd, considered)
		} else {
			return ApplyFormatting(toDelete, current) + ApplyFormatting(considerDelete, updated) + ApplyFormatting(considerAdd, considered)
		}
	}
}

func (l *TCDMModelLaTeXWriter) RenderModelName() string {
	return l.RenderElement(func(m cdm.TCDMModel) string {
		return m.ModelName
	})
}

func (l *TCDMModelLaTeXWriter) RenderTypeNameOfBaseTypeOfInvolvementType(involvementType string) string {
	return l.RenderElement(func(m cdm.TCDMModel) string {
		return m.TypeName[m.BaseTypeOfInvolvementType[involvementType]]
	})
}

func (l *TCDMModelLaTeXWriter) RenderDomainNameOfQualityType(typeID string) string {
	return l.RenderElement(func(m cdm.TCDMModel) string {
		return m.DomainOfQualityType[typeID]
	})
}

func (l *TCDMModelLaTeXWriter) RenderTypeName(typeID string) string {
	return l.RenderElement(func(m cdm.TCDMModel) string {
		return m.TypeName[typeID]
	})
}

func (l *TCDMModelLaTeXWriter) RenderRelationTypeReading(reading string) string {
	return l.RenderElement(func(m cdm.TCDMModel) string {
		readingString := ""
		for involvementPosition, involvementType := range m.ReadingDefinition[reading].InvolvementTypes {
			if involvementPosition == 0 {
				readingString += m.ReadingDefinition[reading].ReadingElements[involvementPosition]
			}
			readingString += " " +
				m.TypeName[m.BaseTypeOfInvolvementType[involvementType]] + " ... " +
				m.ReadingDefinition[reading].ReadingElements[involvementPosition+1]
		}
		return strings.TrimSpace(readingString)
	})
}

/*
 * LaTeX
 */

func (l *TCDMModelLaTeXWriter) WriteLaTeX(format string, parameters ...any) {
	l.LaTeXfile.WriteString(fmt.Sprintf(format, parameters...))
}

func (l *TCDMModelLaTeXWriter) WriteTypesToLaTeX(sectionTitle string, types map[string]bool, writeTypeToLaTeX func(string)) {
	empty := true
	for tpe, included := range types {
		if included {
			if empty {
				l.WriteLaTeX("\\section{" + sectionTitle + "}\n")
				l.WriteLaTeX("\\begin{itemize}\n")
			} else {
				l.WriteLaTeX("\n")
			}
			empty = false

			writeTypeToLaTeX(tpe)
		}
	}
	if !empty {
		l.WriteLaTeX("\\end{itemize}\n")
		l.WriteLaTeX("\n")
	}
}

func (l *TCDMModelLaTeXWriter) WriteModelToLaTeX() {
	l.LaTeXfile, _ = os.Create(l.workFolder + "/" + l.latexFile + latexFileExtension)

	l.WriteLaTeX("\\documentclass[a4paper]{article}\n")
	l.WriteLaTeX("\\usepackage{a4wide}\n")
	l.WriteLaTeX("\\usepackage{xcolor}\n")
	l.WriteLaTeX("\\usepackage{ulem}\n")
	l.WriteLaTeX("\n")
	l.WriteLaTeX("\\title{CDM Model: %s}\n", l.RenderModelName())
	l.WriteLaTeX("\\author{~~}\n")
	l.WriteLaTeX("\n")
	l.WriteLaTeX("\\begin{document}\n")
	l.WriteLaTeX("\\maketitle\n")
	l.WriteLaTeX("\n")

	l.WriteTypesToLaTeX("Quality types", l.QualityTypes(), func(qualityType string) {
		l.WriteLaTeX("    \\item {\\sf %s} with domain {\\sf %s}\n", l.RenderTypeName(qualityType), l.RenderDomainNameOfQualityType(qualityType))
	})

	l.WriteTypesToLaTeX("Concrete individual types", l.ConcreteIndividualTypes(), func(concreteIndividualType string) {
		l.WriteLaTeX("    \\item {\\sf %s}\n", l.RenderTypeName(concreteIndividualType))
	})

	l.WriteTypesToLaTeX("Relation types", l.RelationTypes(), func(relationType string) {
		l.WriteLaTeX("    \\item {\\sf %s: $\\{$ ", l.RenderTypeName(relationType))
		sep := ""
		for involvementType, included := range l.InvolvementTypesOfRelationType(relationType) {
			if included {
				l.WriteLaTeX("%s%s %s", sep, l.RenderTypeNameOfBaseTypeOfInvolvementType(involvementType), l.RenderTypeName(involvementType))
				sep = "; "
			}
		}
		l.WriteLaTeX(" $\\}$}\n")
		if len(l.ReadingsOfRelationType(relationType)) > 0 {
			l.WriteLaTeX("\n")
			l.WriteLaTeX("          Reading(s):\n")
			l.WriteLaTeX("          \\begin{itemize}\n")
			readingPosition := 0
			for reading := range l.ReadingsOfRelationType(relationType) {
				if readingPosition > 0 {
					l.WriteLaTeX("\n")
				}
				readingPosition++
				l.WriteLaTeX("              \\item {\\sf %s}\n", l.RenderRelationTypeReading(reading))
			}
			l.WriteLaTeX("          \\end{itemize}\n")
		}
	})
	l.WriteLaTeX("\\end{document}\n")

	l.LaTeXfile.Close()
}

func (l *TCDMModelLaTeXWriter) CreatePDF() {
	cmd := exec.Command("pdflatex", l.latexFile+latexFileExtension)
	cmd.Dir = l.workFolder
	cmd.Run()
}

func (l *TCDMModelLaTeXWriter) Initialise(config string, errorReporter mbconnect.TErrorReporter) {
	l.errorReporter = errorReporter

	cfg, err := ini.Load(config)
	if err != nil {
		l.errorReporter("Failed to read config file:", err)
		return
	}

	l.workFolder = cfg.Section("").Key("work").String()
	l.latexFile = cfg.Section("").Key("latex").String()

	l.CurrentModel = cdm.CreateCDMModel()
	l.UpdatedModel = cdm.CreateCDMModel()
	l.ConsideredModel = cdm.CreateCDMModel()
}

func CreateCDMLaTeXWriter(config string, errorReporter mbconnect.TErrorReporter) TCDMModelLaTeXWriter {
	CDMModelLaTeXWriter := TCDMModelLaTeXWriter{}
	CDMModelLaTeXWriter.Initialise(config, errorReporter)

	return CDMModelLaTeXWriter
}

func (l *TCDMModelLaTeXWriter) ListenToModellingBus(ModellingBusModelListener mbconnect.TModellingBusModelConnector, agentId, modelID string) {
	ModellingBusModelListener.ListenToStatePostings(agentId, modelID, func() {
		fmt.Println("Received state")
		l.CurrentModel.GetStateFromBus(ModellingBusModelListener)
		l.UpdatedModel.GetUpdatedFromBus(ModellingBusModelListener)
		l.ConsideredModel.GetConsideredFromBus(ModellingBusModelListener)
		l.WriteModelToLaTeX()
		l.CreatePDF()
	})

	ModellingBusModelListener.ListenToUpdatePostings(agentId, modelID, func() {
		fmt.Println("Received update")
		l.UpdatedModel.GetUpdatedFromBus(ModellingBusModelListener)
		l.ConsideredModel.GetConsideredFromBus(ModellingBusModelListener)
		l.WriteModelToLaTeX()
		l.CreatePDF()
	})

	ModellingBusModelListener.ListenToConsideringPostings(agentId, modelID, func() {
		fmt.Println("Received considered")
		l.ConsideredModel.GetConsideredFromBus(ModellingBusModelListener)
		l.WriteModelToLaTeX()
		l.CreatePDF()
	})
}

func main() {
	config := defaultIni
	if len(os.Args) > 1 {
		config = os.Args[1]
	}

	fmt.Println("Using config:", config)

	ModellingBusConnector := mbconnect.CreateModellingBusConnector(config, reportError)
	ModellingBusModelListener := cdm.CreateCDMListener(ModellingBusConnector)

	CDMLaTeXWriter := CreateCDMLaTeXWriter(config, reportError)
	CDMLaTeXWriter.ListenToModellingBus(ModellingBusModelListener, "cdm-tester", "0001")

	for true {
		time.Sleep(1)
	}
}
