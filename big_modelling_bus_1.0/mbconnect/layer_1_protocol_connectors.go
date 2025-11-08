/*
 *
 * Package: mbconnect
 * Module:  modelling_bus_connector
 *
 * This package defines the TModellingBusConnector type which takes are of connecting
 * to the FTP server as well as the MQTT broker.
 * ..... ... .. .
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.10.2025
 *
 */

package mbconnect

import (
	"encoding/json"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/secsy/goftp"
	"gopkg.in/ini.v1"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	modellingBusVersion = "bus-version-1.0"
	jsonFileExtension   = ".json"
	maxMQTTMessageSize  = 300
)

type (
	TErrorReporter func(string, error)

	TModellingBusConnector struct {
		ftpPort,
		ftpUser,
		ftpAgentRoot,
		ftpServer,
		ftpPassword,
		ftpPathPrefix,
		ftpLocalWorkFolder string

		mqttUser,
		mqttPort,
		mqttAgentRoot,
		mqttGenericRoot,
		mqttBroker,
		mqttPassword,
		mqttPathPrefix string
		mqttClient mqtt.Client

		AgentID,
		experimentID,
		lastTimeTimestamp string

		timestampCounter int

		errorReporter TErrorReporter
	}
)

/*
 *
 * Internal functionality
 *
 */

/*
 * FTP connection
 */

func (b *TModellingBusConnector) ftpConnect() (*goftp.Client, error) {
	config := goftp.Config{}
	config.User = b.ftpUser
	config.Password = b.ftpPassword

	ftpServerDefinition := b.ftpServer + ":" + b.ftpPort
	client, err := goftp.DialConfig(config, ftpServerDefinition)
	if err != nil {
		b.errorReporter("Error connecting to the FTP server:", err)
		return client, err
	}

	return client, err
}

func (b *TModellingBusConnector) mkFTPFilePath(remoteFolderPath string) {
	// Connect to the FTP server
	client, err := b.ftpConnect()
	if err != nil {
		b.errorReporter("Couldn't open an FTP connection:", err)
		return
	}

	pathCovered := ""
	ftpUploadPath := b.ftpAgentRoot + "/" + remoteFolderPath
	for _, folder := range strings.Split(ftpUploadPath, "/") {
		pathCovered = pathCovered + folder + "/"
		client.Mkdir(pathCovered)
	}

	client.Close()
}

func (b *TModellingBusConnector) postFileToFTP(topicPath, fileName, localFilePath string) {
	remoteFilePath := b.ftpAgentRoot + "/" + topicPath + "/" + fileName

	// Connect to the FTP server
	client, err := b.ftpConnect()
	if err != nil {
		b.errorReporter("Couldn't open an FTP connection:", err)
		return
	}

	file, err := os.Open(localFilePath)
	if err != nil {
		b.errorReporter("Error opening File for reading:", err)
		return
	}

	err = client.Store(remoteFilePath, file)
	if err != nil {
		b.errorReporter("Error uploading File to ftp server:", err)
		return
	}

	client.Close()
}

func (b *TModellingBusConnector) postJSONFileToFTP(topicPath, fileName string, json []byte, timestamp string) {
	// Define the file paths
	localFilePath := b.ftpLocalWorkFolder + "/" + fileName

	// Create a temporary local file with the JSON record
	err := os.WriteFile(localFilePath, json, 0644)
	if err != nil {
		b.errorReporter("Error writing to temporary file:", err)
	}

	b.postFileToFTP(topicPath, fileName, localFilePath)

	// Cleanup the temporary file aftewards
	os.Remove(localFilePath)
}

func (b *TModellingBusConnector) cleanFTPPath(topicPath, timestamp string) {
	// Connect to the FTP server
	client, err := b.ftpConnect()
	if err != nil {
		b.errorReporter("Couldn't open an FTP connection:", err)
		return
	}

	fileInfos, _ := client.ReadDir(b.ftpAgentRoot + "/" + topicPath)

	// Remove older Files from the FTP server within the topicPath folder
	for _, fileInfo := range fileInfos {
		if timestamp == "" {
			err = client.Delete(fileInfo.Name())
			if err != nil {
				b.errorReporter("Couldn't delete File:", err)
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

func (b *TModellingBusConnector) ftpGetFile(server, port, remoteFilePath, localFileName string) {
	client, err := goftp.DialConfig(goftp.Config{}, server+":"+port)
	if err != nil {
		b.errorReporter("Something went wrong connecting to the FTP server", err)
		return
	}

	// Download an File to disk
	// ====> CHECK need to OS (Dos, Linux, ...) independent "/"
	File, err := os.Create(localFileName)
	if err != nil {
		b.errorReporter("Something went wrong creating local file", err)
		return
	}

	err = client.Retrieve(remoteFilePath, File)
	if err != nil {
		b.errorReporter("Something went wrong retrieving file", err)
		return
	}
}

/*
 * MQTT connection
 */

func (b *TModellingBusConnector) connLostHandler(c mqtt.Client, err error) {
	panic(fmt.Sprintf("PANIC; MQTT connection lost, reason: %v\n", err))
}

func (b *TModellingBusConnector) ConnectToMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + b.mqttBroker + ":" + b.mqttPort)
	opts.SetClientID("mqtt-client-" + b.AgentID)
	opts.SetUsername(b.mqttUser)
	opts.SetPassword(b.mqttPassword)
	opts.SetConnectionLostHandler(b.connLostHandler)

	for connected := false; !connected; {
		// Two log channels needed. One for errors, and one for normal progress.
		fmt.Println("Trying to connect to the MQTT broker")

		b.mqttClient = mqtt.NewClient(opts)
		token := b.mqttClient.Connect()
		token.Wait()

		err := token.Error()
		if err != nil {
			b.errorReporter("Error connecting to the MQTT broker:", err)

			time.Sleep(5)
		} else {
			connected = true
		}
	}

	fmt.Println("Connected to the MQTT broker")
}

func (b *TModellingBusConnector) listenToEventsOnMQTT(AgentID, topicPath string, eventHandler mqtt.MessageHandler) {
	mqttTopicPath := b.mqttGenericRoot + "/" + AgentID + "/" + topicPath
	token := b.mqttClient.Subscribe(mqttTopicPath, 1, eventHandler)
	token.Wait()
}

func (b *TModellingBusConnector) postEventToMQTT(topicPath, message string) {
	mqttTopicPath := b.mqttAgentRoot + "/" + topicPath
	token := b.mqttClient.Publish(mqttTopicPath, 0, true, message)
	token.Wait()
}

func (b *TModellingBusConnector) listenForRawFileLinkPostingsOnMQTT(AgentID, topicPath string, postingHandler func(string, string, string, string)) {
	b.listenToEventsOnMQTT(AgentID, topicPath, func(client mqtt.Client, msg mqtt.Message) {
		var rawFileLink TRawFileLink
		/// Use a generic error checker for Unmarshal. Shouldreturn a bool
		err := json.Unmarshal(msg.Payload(), &rawFileLink)
		if err == nil {
			postingHandler(rawFileLink.Server, rawFileLink.Port, rawFileLink.Path, rawFileLink.Timestamp)
		}
	})
}

type TJSONFileLink struct {
	Server      string `json:"server"`
	Port        string `json:"port"`
	Path        string `json:"path"`
	Timestamp   string `json:"timestamp"`
	JSONVersion string `json:"json version"`
}

func (b *TModellingBusConnector) postJSONFileLinkToMQTT(topicPath, jsonFileName, jsonVersion, timestamp string) {
	var jsonFileLink TJSONFileLink

	jsonFileLink.Server = b.ftpServer
	jsonFileLink.Port = b.ftpPort
	jsonFileLink.Path = b.ftpAgentRoot + "/" + topicPath + "/" + jsonFileName
	jsonFileLink.Timestamp = timestamp
	jsonFileLink.JSONVersion = jsonVersion

	jsonData, err := json.Marshal(jsonFileLink)
	if err != nil {
		b.errorReporter("Something went wrong JSONing the link data", err)
		return
	}

	b.postEventToMQTT(topicPath, string(jsonData))
}

func (b *TModellingBusConnector) ListenForJSONFileLinkPostingsOnMQTT(AgentID, topicPath string, postingHandler func(string, string, string, string, string)) {
	b.listenToEventsOnMQTT(AgentID, topicPath, func(client mqtt.Client, msg mqtt.Message) {
		var jsonFileLink TJSONFileLink

		err := json.Unmarshal(msg.Payload(), &jsonFileLink)
		if err == nil {
			postingHandler(jsonFileLink.Server, jsonFileLink.Port, jsonFileLink.Path, jsonFileLink.Timestamp, jsonFileLink.JSONVersion)
		}
	})
}

type TRawFileLink struct {
	Server    string `json:"server"`
	Port      string `json:"port"`
	Path      string `json:"Path"`
	Timestamp string `json:"timestamp"`
}

func (b *TModellingBusConnector) postRawFileLinkToMQTT(topicPath, rawFileName, timestamp string) {
	var rawFileLink TRawFileLink

	rawFileLink.Server = b.ftpServer
	rawFileLink.Port = b.ftpPort
	rawFileLink.Path = b.ftpAgentRoot + "/" + topicPath + "/" + rawFileName
	rawFileLink.Timestamp = timestamp

	data, err := json.Marshal(rawFileLink)
	if err != nil {
		b.errorReporter("Something went wrong JSONing the link data", err)
		return
	}

	b.postEventToMQTT(topicPath, string(data))
}

/*
 * Combined FTP + MQTT connection
 */

func (b *TModellingBusConnector) mkFilePath(remoteFolderPath string) {
	// This may look odd, but it makes it clear that we do not need to do any work for
	// MQTT to ensure a path exists.
	b.mkFTPFilePath(remoteFolderPath)
}

func (b *TModellingBusConnector) mkEventPath(remoteFolderPath string) {
	// Dummy function
	// Models/Files/Requests/... should not know about the needs of the underlying
	// platforms.
	// MQTT does automatically create topic trees, whereas FTP does not ...
}

func (b *TModellingBusConnector) postJSONFile(topicPath, jsonVersion, timestamp string, json []byte) {
	fileName := timestamp + jsonFileExtension

	b.postJSONFileToFTP(topicPath, fileName, json, timestamp)
	b.postJSONFileLinkToMQTT(topicPath, fileName, jsonVersion, timestamp)
}

func (b *TModellingBusConnector) postRawFile(topicPath, fileName, localFilePath, timestamp string) {
	b.postFileToFTP(topicPath, fileName, localFilePath)
	b.postRawFileLinkToMQTT(topicPath, fileName, timestamp)
}

func (b *TModellingBusConnector) listenForJSONFilePostings(AgentID, topicPath string, postingHandler func(string, []byte)) {
	b.ListenForJSONFileLinkPostingsOnMQTT(AgentID, topicPath, func(server, port, path, timestamp, jsonVersion string) {
		tempFilePath := b.ftpLocalWorkFolder + "/" + b.GetTimestamp() + jsonFileExtension

		b.ftpGetFile(server, port, path, tempFilePath)

		jsonPayload, err := os.ReadFile(tempFilePath)
		if err == nil {
			postingHandler(timestamp, jsonPayload)
		} else {
			b.errorReporter("Something went wrong retrieving file", err)
		}

		os.Remove(tempFilePath)
	})
}

/*
 *
 * Externally visible functionality
 *
 */

/*
 * Time stamping and unique IDs
 */

func (b *TModellingBusConnector) GetTimestamp() string {
	CurrenTime := time.Now()

	timeTimestamp := fmt.Sprintf(
		"%04d-%02d-%02d-%02d-%02d-%02d", //-%06d
		CurrenTime.Year(),
		CurrenTime.Month(),
		CurrenTime.Day(),
		CurrenTime.Hour(),
		CurrenTime.Minute(),
		CurrenTime.Second())
	//		CurrenTime.Nanosecond()/1000)

	if timeTimestamp == b.lastTimeTimestamp {
		b.timestampCounter++
	} else {
		b.lastTimeTimestamp = timeTimestamp
		b.timestampCounter = 0
	}

	return fmt.Sprintf("%s-%02d", b.lastTimeTimestamp, b.timestampCounter)
}

func (b *TModellingBusConnector) GetNewID() string {
	return fmt.Sprintf("%s-%s", b.AgentID, b.GetTimestamp())
}

func EventPayloadAllowed (payload []byte) bool {
	return len(payload) <= maxMQTTMessageSize
}

/*
 * Initialisation & creation
 */

func (b *TModellingBusConnector) Initialise(config string, errorReporter TErrorReporter) {
	b.errorReporter = errorReporter

	cfg, err := ini.Load(config)
	if err != nil {
		b.errorReporter("Failed to read config file:", err)
		return
	}

	b.experimentID = cfg.Section("").Key("experiment").String()
	b.AgentID = cfg.Section("").Key("agent").String()
	b.ftpLocalWorkFolder = cfg.Section("").Key("work").String()

	b.ftpPort = cfg.Section("ftp").Key("port").String()
	b.ftpUser = cfg.Section("ftp").Key("user").String()
	b.ftpServer = cfg.Section("ftp").Key("server").String()
	b.ftpPassword = cfg.Section("ftp").Key("password").String()
	b.ftpPathPrefix = cfg.Section("ftp").Key("prefix").String()

	b.mqttPort = cfg.Section("mqtt").Key("port").String()
	b.mqttUser = cfg.Section("mqtt").Key("user").String()
	b.mqttBroker = cfg.Section("mqtt").Key("broker").String()
	b.mqttPassword = cfg.Section("mqtt").Key("password").String()
	b.mqttPathPrefix = cfg.Section("mqtt").Key("prefix").String()

	topicBase := modellingBusVersion + "/" + b.experimentID
	b.mqttGenericRoot = b.mqttPathPrefix + "/" + topicBase
	b.mqttAgentRoot = b.mqttPathPrefix + "/" + topicBase + "/" + b.AgentID
	b.ftpAgentRoot = b.ftpPathPrefix + "/" + topicBase + "/" + b.AgentID

	b.lastTimeTimestamp = ""
	b.timestampCounter = 0

	b.ConnectToMQTT()
}

func CreateModellingBusConnector(config string, errorReporter TErrorReporter) TModellingBusConnector {
	ModellingBusConnector := TModellingBusConnector{}
	ModellingBusConnector.Initialise(config, errorReporter)

	return ModellingBusConnector
}
