/*
 *
 * Package: mbconnect
 * Layer:   1
 * Module:  repository_connector
 *
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.11.2025
 *
 */

package mbconnect

import (
	"github.com/secsy/goftp"
	"os"
	"path/filepath"
	"strings"
)

type (
	tModellingBusRepositoryConnector struct {
		port,
		user,
		server,
		password,
		agentRoot,
		experimentRoot,
		localWorkDirectory string
		activeTransfers,
		singleServerMode bool

		createdPaths map[string]bool

		reporter *TReporter
	}
)

type tRepositoryEvent struct {
	Server    string `json:"server,omitempty"`
	Port      string `json:"port,omitempty"`
	FilePath  string `json:"file path,omitempty"`
	Timestamp string `json:"timestamp"`
}

func (r *tModellingBusRepositoryConnector) ftpConnect() (*goftp.Client, error) {
	config := goftp.Config{}
	config.User = r.user
	config.Password = r.password
	config.ActiveTransfers = r.activeTransfers

	serverDefinition := r.server + ":" + r.port
	client, err := goftp.DialConfig(config, serverDefinition)
	if err != nil {
		r.reporter.Error("Error connecting to the FTP server. %s", err)
		return client, err
	}

	return client, err
}

func (r *tModellingBusRepositoryConnector) mkRepositoryFilePath(remoteFilePath string) {
	if !r.createdPaths[remoteFilePath] {
		if client, err := r.ftpConnect(); err == nil {
			pathCovered := ""
			for _, Directory := range strings.Split(remoteFilePath, "/") {
				pathCovered = pathCovered + Directory + "/"
				client.Mkdir(pathCovered)
			}

			client.Close()

			r.createdPaths[remoteFilePath] = true
		}
	}
}

func (r *tModellingBusRepositoryConnector) addFile(topicPath, localFilePath, timestamp string) tRepositoryEvent {
	remoteFilePath := r.agentRoot + "/" + topicPath
	remoteFilePayloadPath := remoteFilePath + "/" + filePayload

	r.mkRepositoryFilePath(remoteFilePath)

	repositoryEvent := tRepositoryEvent{}
	repositoryEvent.Timestamp = timestamp

	file, err := os.Open(filepath.FromSlash(localFilePath))
	if err != nil {
		r.reporter.Error("Error opening File for reading. %s", err)
		return repositoryEvent
	}

	client, err := r.ftpConnect()
	if err != nil {
		return repositoryEvent
	}

	err = client.Store(remoteFilePayloadPath, file)
	if err != nil {
		r.reporter.Error("Error uploading file to ftp server. %s", err)
		r.reporter.Error("For remote file path: %s", remoteFilePayloadPath)
		return repositoryEvent
	}

	client.Close()

	if !r.singleServerMode {
		repositoryEvent.Server = r.server
		repositoryEvent.Port = r.port
	}
	repositoryEvent.FilePath = remoteFilePayloadPath

	return repositoryEvent
}

func deleteRepositoryPath(client *goftp.Client, deletePath string) {
	client.Delete(deletePath)
	fileInfos, _ := client.ReadDir(deletePath)
	if len(fileInfos) > 0 {
		for _, fileInfo := range fileInfos {
			deleteRepositoryPath(client, deletePath+"/"+fileInfo.Name())
		}
		client.Rmdir(deletePath)
	}
}

func (r *tModellingBusRepositoryConnector) deletePath(deletePath string) {
	if client, err := r.ftpConnect(); err == nil {
		deleteRepositoryPath(client, deletePath)
	}
}

func (r *tModellingBusRepositoryConnector) deletePostingPath(topicPath string) {
	r.deletePath(r.agentRoot + "/" + topicPath)
}

func (r *tModellingBusRepositoryConnector) deleteExperiment() {
	r.deletePath(r.experimentRoot)
}

func (r *tModellingBusRepositoryConnector) addJSONAsFile(topicPath string, json []byte, timestamp string) tRepositoryEvent {
	// Define the temporary local file path
	localFilePath := r.localWorkDirectory + "/" + jsonFileName

	// Create a temporary local file with the JSON record
	err := os.WriteFile(filepath.FromSlash(localFilePath), json, 0644)
	if err != nil {
		r.reporter.Error("Error writing to temporary file. %s", err)
	}

	// Cleanup the temporary file afterwards
	defer os.Remove(filepath.FromSlash(localFilePath))

	return r.addFile(topicPath, localFilePath, timestamp)
}

func (r *tModellingBusRepositoryConnector) getFile(repositoryEvent tRepositoryEvent, fileName string) string {
	localFileName := r.localWorkDirectory + "/" + fileName

	config := goftp.Config{}
	config.ActiveTransfers = r.activeTransfers
	serverConnection := ""

	if r.singleServerMode {
		serverConnection = r.server + ":" + r.port

		config.User = r.user
		config.Password = r.password
	} else {
		serverConnection = repositoryEvent.Server + ":" + repositoryEvent.Port
	}

	client, err := goftp.DialConfig(config, serverConnection)
	if err != nil {
		r.reporter.Error("Something went wrong connecting to the FTP server: \"%s\"", err)
		return ""
	}

	// Download a File to local storage
	File, err := os.Create(localFileName)
	if err != nil {
		r.reporter.Error("Something went wrong creating local file: \"%s\"", err)
		return ""
	}

	err = client.Retrieve(repositoryEvent.FilePath, File)
	if err != nil {
		r.reporter.Error("Something went wrong retrieving file: \"%s\"", err)
		r.reporter.Error("Was trying to retrieve: %s", repositoryEvent.FilePath)
		return ""
	}

	return localFileName
}

func createModellingBusRepositoryConnector(experimentID, agentID string, configData *TConfigData, reporter *TReporter) *tModellingBusRepositoryConnector {
	r := tModellingBusRepositoryConnector{}

	r.reporter = reporter

	// Get data from the config file
	r.localWorkDirectory = configData.GetValue("", "work_folder").String()
	r.port = configData.GetValue("ftp", "port").String()
	r.user = configData.GetValue("ftp", "user").String()
	r.server = configData.GetValue("ftp", "server").String()
	r.password = configData.GetValue("ftp", "password").String()
	r.singleServerMode = configData.GetValue("ftp", "single_server_mode").BoolWithDefault(false)
	r.activeTransfers = configData.GetValue("ftp", "active_transfers").BoolWithDefault(false)
	r.experimentRoot = configData.GetValue("ftp", "prefix").String() + "/" + experimentID
	r.agentRoot = r.experimentRoot + "/" + agentID

	r.createdPaths = map[string]bool{}

	if r.singleServerMode {
		r.reporter.Progress("Running the FTP connection in single server mode")
	} else {
		r.reporter.Progress("Running the FTP connection in multi server mode")
	}

	if r.activeTransfers {
		r.reporter.Progress("Running the FTP connection in active transfer mode")
	} else {
		r.reporter.Progress("Running the FTP connection in passive transfer mode")
	}

	return &r
}
