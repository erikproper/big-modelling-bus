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
		ftpPort,
		ftpUser,
		ftpAgentRoot,
		ftpServer,
		ftpPassword,
		ftpLocalWorkDirectory string

		createdPaths map[string]bool

		errorReporter TErrorReporter
	}
)

type tRepositoryEvent struct {
	Server        string `json:"server,omitempty"`
	Port          string `json:"port,omitempty"`
	FilePath      string `json:"file path,omitempty"`
	FileExtension string `json:"file extension,omitempty"`
}

func (r *tModellingBusRepositoryConnector) ftpConnect() (*goftp.Client, error) {
	config := goftp.Config{}
	config.User = r.ftpUser
	config.Password = r.ftpPassword

	ftpServerDefinition := r.ftpServer + ":" + r.ftpPort
	client, err := goftp.DialConfig(config, ftpServerDefinition)
	if err != nil {
		r.errorReporter("Error connecting to the FTP server:", err)
		return client, err
	}

	return client, err
}

func (r *tModellingBusRepositoryConnector) mkRepositoryDirectoryPath(remoteDirectoryPath string) {
	if !r.createdPaths[remoteDirectoryPath] {
		// Connect to the FTP server
		client, err := r.ftpConnect()
		if err != nil {
			r.errorReporter("Couldn't open an FTP connection:", err)
			return
		}

		pathCovered := ""
		for _, Directory := range strings.Split(remoteDirectoryPath, "/") {
			pathCovered = pathCovered + Directory + "/"
			client.Mkdir(pathCovered)
		}

		client.Close()

		r.createdPaths[remoteDirectoryPath] = true
	}
}

func (r *tModellingBusRepositoryConnector) pushFileToRepository(topicPath, fileName, fileExtension, localFilePath string) tRepositoryEvent {
	remoteDirectoryPath := r.ftpAgentRoot + "/" + topicPath
	remoteFilePath := remoteDirectoryPath + "/" + fileName + fileExtension

	r.mkRepositoryDirectoryPath(remoteDirectoryPath)

	repositoryEvent := tRepositoryEvent{}

	// Connect to the FTP server
	client, err := r.ftpConnect()
	{
		if err != nil {
			r.errorReporter("Couldn't open an FTP connection:", err)
			return repositoryEvent
		}

		file, err := os.Open(localFilePath)
		if err != nil {
			r.errorReporter("Error opening File for reading:", err)
			return repositoryEvent
		}

		err = client.Store(remoteFilePath, file)
		if err != nil {
			r.errorReporter("Error uploading File to ftp server:", err)
			return repositoryEvent
		}
	}
	client.Close()

	repositoryEvent.Server = r.ftpServer
	repositoryEvent.Port = r.ftpPort
	repositoryEvent.FilePath = r.ftpAgentRoot + "/" + topicPath + "/" + fileName
	repositoryEvent.FileExtension = fileExtension

	return repositoryEvent
}

func (r *tModellingBusRepositoryConnector) pushJSONAsFileToRepository(topicPath string, json []byte) tRepositoryEvent {
	// Define the temporary local file path
	localFilePath := r.ftpLocalWorkDirectory + "/" + GetTimestamp() + jsonFileExtension

	// Create a temporary local file with the JSON record
	err := os.WriteFile(localFilePath, json, 0644)
	if err != nil {
		r.errorReporter("Error writing to temporary file:", err)
	}

	// Cleanup the temporary file afterwards
	defer os.Remove(localFilePath)

	return r.pushFileToRepository(topicPath, jsonFileName, jsonFileExtension, localFilePath)
}

func (r *tModellingBusRepositoryConnector) cleanRepositoryPath(topicPath, timestamp string) {
	// Connect to the FTP server
	client, err := r.ftpConnect()
	if err != nil {
		r.errorReporter("Couldn't open an FTP connection:", err)
		return
	}

	fileInfos, _ := client.ReadDir(r.ftpAgentRoot + "/" + topicPath)

	// Remove older Files from the FTP server within the topicPath Directory
	for _, fileInfo := range fileInfos {
		if timestamp == "" {
			err = client.Delete(fileInfo.Name())
			if err != nil {
				r.errorReporter("Couldn't delete File:", err)
				return
			}
		} else {
			filePath := fileInfo.Name()
			fileName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))

			if fileName < timestamp {
				client.Delete(filePath)
			}
		}
	}
}

func (r *tModellingBusRepositoryConnector) getFileFromRepository(repositoryEvent tRepositoryEvent, timestamp string) string {
	localFileName := r.ftpLocalWorkDirectory + "/" + timestamp + repositoryEvent.FileExtension
	serverConnection := repositoryEvent.Server + ":" + repositoryEvent.Port

	client, err := goftp.DialConfig(goftp.Config{}, serverConnection)
	if err != nil {
		r.errorReporter("Something went wrong connecting to the FTP server", err)
		return ""
	}

	// Download a File to local storage
	// ====> CHECK need for OS (Dos, Linux, ...) independent "/"
	File, err := os.Create(localFileName)
	if err != nil {
		r.errorReporter("Something went wrong creating local file", err)
		return ""
	}

	err = client.Retrieve(repositoryEvent.FilePath+repositoryEvent.FileExtension, File)
	if err != nil {
		r.errorReporter("Something went wrong retrieving file", err)
		return ""
	}

	return localFileName
}

func (r *tModellingBusRepositoryConnector) oldGetFileFromRepository(server, port, remoteFilePath, localFileName string) {
	client, err := goftp.DialConfig(goftp.Config{}, server+":"+port)
	if err != nil {
		r.errorReporter("Something went wrong connecting to the FTP server", err)
		return
	}

	// Download a File to local storage
	// ====> CHECK need for OS (Dos, Linux, ...) independent "/"
	File, err := os.Create(localFileName)
	if err != nil {
		r.errorReporter("Something went wrong creating local file", err)
		return
	}

	err = client.Retrieve(remoteFilePath, File)
	if err != nil {
		r.errorReporter("Something went wrong retrieving file", err)
		return
	}
}

func createModellingBusRepositoryConnector(topicBase, agentID string, configData *TConfigData, errorReporter TErrorReporter) *tModellingBusRepositoryConnector {
	r := tModellingBusRepositoryConnector{}

	r.errorReporter = errorReporter

	// Get data from the config file
	r.ftpLocalWorkDirectory = configData.GetValue("", "work").String()
	r.ftpPort = configData.GetValue("ftp", "port").String()
	r.ftpUser = configData.GetValue("ftp", "user").String()
	r.ftpServer = configData.GetValue("ftp", "server").String()
	r.ftpPassword = configData.GetValue("ftp", "password").String()
	r.ftpAgentRoot = configData.GetValue("ftp", "prefix").String() + "/" + topicBase + "/" + agentID

	r.createdPaths = map[string]bool{}

	return &r
}
