/*
 *
 * Package: mbconnect
 * Layer:   3
 * Module:  raw_artefacts
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
	"path/filepath"
)

const (
	rawArtefactsFilePathElement = "artefacts/file"
)

/*
 *
 * Externally visible functionality
 *
 */

func (b *TModellingBusConnector) PostRawArtefact(context, format, localFilePath string) {
	topicPath := rawArtefactsFilePathElement +
		"/" + context +
		"/" + format
	timestamp := GetTimestamp()

	b.postFile(topicPath, timestamp, filepath.Ext(localFilePath), localFilePath, timestamp)
}
