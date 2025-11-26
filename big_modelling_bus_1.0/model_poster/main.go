package main

import (
	"bufio"
	"fmt"
	"modelling_bus_1.0/mbconnect"
	"modelling_bus_1.0/mblanguages/cdm"
	"os"
)

const (
	defaultIni = "mobile.ini"
)

func Pause() {
	fmt.Println("Press any key")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
}

func ReportProgress(message string) {
	fmt.Println("PROGRESS:", message)
}

func ReportError(message string) {
	fmt.Println("ERROR:", message)
}

func main() {
	reporter := mbconnect.CreateReporter(ReportError, ReportProgress)

	config := defaultIni
	if len(os.Args) > 1 {
		config = os.Args[1]
	}

	// Note: the config data can be used to contain config data for different aspects
	configData := mbconnect.LoadConfig(config, reporter)

	// Note: One ModellingBusConnector can be used for different models of different kinds.
	ModellingBusConnector := mbconnect.CreateModellingBusConnector(configData, reporter)
	ModellingBusConnector.DeleteExperiment()

	//		ModellingBusConnector.PostRawArtefact("context", "golang", "test", "main.go")
	//		fmt.Println(ModellingBusConnector.GetRawArtefact("cdm-tester", "context", "golang", "test", "local.go"))
	//		fmt.Println(ModellingBusConnector.GetRawArtefact("cdm-tester", "context", "golang", "test", "local.go"))
	//		ModellingBusConnector.DeleteRawArtefact("context", "golang", "test.go")

	// Note that the 0001 is for local use. No issue to e.g. make this into 0001/02 to indicate version numbers
	CDMModellingBusPoster := cdm.CreateCDMPoster(ModellingBusConnector, "0001")

	CDMModellingBusPoster.SetModelName("Empty university")

	fmt.Println("1) empty model")
	CDMModellingBusPoster.PostState()
	fmt.Println("Posted state")
	Pause()

	Student := CDMModellingBusPoster.AddConcreteIndividualType("Student")
	StudyProgramme := CDMModellingBusPoster.AddConcreteIndividualType("Study Programme")
	StudentName := CDMModellingBusPoster.AddQualityType("Student Name", "string")
	StudyProgrammeName := CDMModellingBusPoster.AddQualityType("Study Programme Name", "string")
	CDMModellingBusPoster.SetModelName("Basic university")

	fmt.Println("2) basic model")
	CDMModellingBusPoster.PostUpdate()
	fmt.Println("Posted update")
	Pause()

	fmt.Println("3) basic model")
	CDMModellingBusPoster.PostState()
	fmt.Println("Posted state")
	Pause()

	StudyProgrammeStudied := CDMModellingBusPoster.AddInvolvementType("studied by", StudyProgramme)
	StudentStudying := CDMModellingBusPoster.AddInvolvementType("studying", Student)
	Studies := CDMModellingBusPoster.AddRelationType("Studies", StudyProgrammeStudied, StudentStudying)
	CDMModellingBusPoster.AddRelationTypeReading(Studies, "", StudentStudying, "studies", StudyProgrammeStudied, "")
	CDMModellingBusPoster.AddRelationTypeReading(Studies, "", StudyProgrammeStudied, "studied by", StudentStudying, "")

	StudentReferred := CDMModellingBusPoster.AddInvolvementType("referred", Student)
	StudentNameReferring := CDMModellingBusPoster.AddInvolvementType("referring", StudentName)
	StudentNaming := CDMModellingBusPoster.AddRelationType("Student Naming", StudentReferred, StudentNameReferring)
	CDMModellingBusPoster.AddRelationTypeReading(StudentNaming, "", StudentReferred, "has", StudentNameReferring, "")
	CDMModellingBusPoster.AddRelationTypeReading(StudentNaming, "", StudentNameReferring, "of", StudentReferred, "")

	StudyProgrammeReferred := CDMModellingBusPoster.AddInvolvementType("referred", StudyProgramme)
	StudyProgrammeNameReferring := CDMModellingBusPoster.AddInvolvementType("referring", StudyProgrammeName)
	StudyProgrammeNaming := CDMModellingBusPoster.AddRelationType("Programme Naming", StudyProgrammeReferred, StudyProgrammeNameReferring)
	CDMModellingBusPoster.AddRelationTypeReading(StudyProgrammeNaming, "", StudyProgrammeReferred, "goes by", StudyProgrammeNameReferring, "")
	CDMModellingBusPoster.AddRelationTypeReading(StudyProgrammeNaming, "", StudyProgrammeNameReferring, "of", StudyProgrammeReferred, "")
	CDMModellingBusPoster.SetModelName("University")

	fmt.Println("4) larger model")
	CDMModellingBusPoster.PostUpdate()
	fmt.Println("Posted update")
	Pause()

	// Reference modes

	// CONSTRAINTS
	//
	// always do a push_model after a read from local FS!
	// push_model
	// push_update

	fmt.Println("5) final model")
	CDMModellingBusPoster.PostState()
}
