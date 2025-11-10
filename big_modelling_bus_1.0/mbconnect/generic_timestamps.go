/*
 *
 * Package: mbconnect
 * Layer:   generic
 * Module:  timestamps
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
	"fmt"
	"time"
)

var (
	timestampCounter  int
	lastTimeTimestamp string
)

func GetTimestamp() string {
	CurrenTime := time.Now()

	timeTimestamp := fmt.Sprintf(
		"%04d-%02d-%02d-%02d-%02d-%02d",
		CurrenTime.Year(),
		CurrenTime.Month(),
		CurrenTime.Day(),
		CurrenTime.Hour(),
		CurrenTime.Minute(),
		CurrenTime.Second())

	if timeTimestamp == lastTimeTimestamp {
		timestampCounter++
	} else {
		lastTimeTimestamp = timeTimestamp
		timestampCounter = 0
	}

	return fmt.Sprintf("%s-%02d", lastTimeTimestamp, timestampCounter)
}

func init() {
	timestampCounter = 0
	lastTimeTimestamp = ""
}
