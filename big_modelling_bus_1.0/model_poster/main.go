package main

import (
	"bufio"
	"fmt"
	"modelling_bus_1.0/cdm"
	"modelling_bus_1.0/mbconnect"
	"os"
)

func reportError(message string, err error) {
	fmt.Println(message+":", err)
}

func Pause() {
	fmt.Println("Press any key")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
}

func main() {
	config := "mobile.ini"
	if len(os.Args) > 1 {
		config = os.Args[1]
	}

	fmt.Println("Using config:", config)

	ModellingBusConnector := mbconnect.CreateModellingBusConnector(config, reportError)

	ModellingBusConnector.PostRawArtefact("context", "golang", "main.go")

	// Note that the 0001 is for local use. No issue to e.g. make this into 0001/02 to indicate version numbers
	Model := cdm.CreateCDMPoster(ModellingBusConnector, "0001")

	// Use "EventHandler" for incomming work. Such as ... also the low level cleamup request
	//
	// Put parameters in a config struct
	// But then use "github.com/go-ini/ini"
	//
	// A "flush" operator for clients

	// log for errors

	///
	// Model.ModelsBus.ModellingBusConnector.CleanFTPPath("models/0001/cdm-1.0-1.0/state", "2025-10-19-21-04-14-986464")
	//	// Remove older versions of the JSON file from the FTP server
	//	fileInfos, err := client.ReadDir(b.ModellingBusConnectorFTPPath)
	//	for _, fileInfo := range fileInfos {
	//		currentName := fileInfo.Name()
	//		if strings.HasSuffix(currentName, JSONFileExtension) {
	//			if currentName != fileName {
	//				client.Delete(b.ModellingBusConnectorFTPPath + "/" + currentName)
	//			}
	//		}
	//	}
	///

	Model.SetModelName("Empty university")

	fmt.Println("1) empty model")
	Model.PostState()
	fmt.Println("Posted state")
	Pause()

	Student := Model.AddConcreteIndividualType("Student")
	StudyProgramme := Model.AddConcreteIndividualType("Study Programme")
	StudentName := Model.AddQualityType("Student Name", "string")
	StudyProgrammeName := Model.AddQualityType("Study Programme Name", "string")
	Model.SetModelName("Basic university")

	fmt.Println("2) basic model")
	Model.PostUpdate()
	fmt.Println("Posted update")
	Pause()

	fmt.Println("3) basic model")
	Model.PostState()
	fmt.Println("Posted state")
	Pause()

	StudyProgrammeStudied := Model.AddInvolvementType("studied by", StudyProgramme)
	StudentStudying := Model.AddInvolvementType("studying", Student)
	Studies := Model.AddRelationType("Studies", StudyProgrammeStudied, StudentStudying)
	Model.AddRelationTypeReading(Studies, "", StudentStudying, "studies", StudyProgrammeStudied, "")
	Model.AddRelationTypeReading(Studies, "", StudyProgrammeStudied, "studied by", StudentStudying, "")

	StudentReferred := Model.AddInvolvementType("referred", Student)
	StudentNameReferring := Model.AddInvolvementType("referring", StudentName)
	StudentNaming := Model.AddRelationType("Student Naming", StudentReferred, StudentNameReferring)
	Model.AddRelationTypeReading(StudentNaming, "", StudentReferred, "has", StudentNameReferring, "")
	Model.AddRelationTypeReading(StudentNaming, "", StudentNameReferring, "of", StudentReferred, "")

	StudyProgrammeReferred := Model.AddInvolvementType("referred", StudyProgramme)
	StudyProgrammeNameReferring := Model.AddInvolvementType("referring", StudyProgrammeName)
	StudyProgrammeNaming := Model.AddRelationType("Programme Naming", StudyProgrammeReferred, StudyProgrammeNameReferring)
	Model.AddRelationTypeReading(StudyProgrammeNaming, "", StudyProgrammeReferred, "goes by", StudyProgrammeNameReferring, "")
	Model.AddRelationTypeReading(StudyProgrammeNaming, "", StudyProgrammeNameReferring, "of", StudyProgrammeReferred, "")
	Model.SetModelName("University")

	fmt.Println("4) larger model")
	Model.PostUpdate()
	fmt.Println("Posted update")
	Pause()

	// Reference modes

	// CONSTRAINTS
	//
	// always do a push_model after a read from local FS!
	// push_model
	// push_update

	fmt.Println("5) final model")
	Model.PostState()
}
