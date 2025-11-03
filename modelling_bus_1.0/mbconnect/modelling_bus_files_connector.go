/*
 *
 * Package: mbconnect
 * Module:  modelling_bus_files_connector
 *
 * ..... TModellingBusFileConnector
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.10.2025
 *
 */

package mbconnect

import (
	"path/filepath"
)

const (
	filesPathElement = "files"
)

/*
 *
 * Externally visible functionality
 *
 */

func (b *TModellingBusConnector) PostFile(context, format, localFilePath string) {
	topicPath := filesPathElement +
		"/" + context +
		"/" + format

	b.mkArtefactPath(topicPath)
	timestamp := b.GetTimestamp()
	fileName := timestamp + filepath.Ext(localFilePath)

	b.postRawArtefact(topicPath, fileName, localFilePath, timestamp)
}
