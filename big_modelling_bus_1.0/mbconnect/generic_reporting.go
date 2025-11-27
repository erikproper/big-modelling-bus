/*
 *
 * Package: mbconnect
 * Layer:   generic
 * Module:  reporting
 *
 * This module is concerned with the reporting of errors, progress, etc, to the user.
 * For the moment, it only involves the reporting of progress and errors.
 *
 * Author: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 27.11.2025
 *
 */

package mbconnect

import (
	"fmt"
)

const (
	ProgressLevelBasic    = 1
	ProgressLevelDetailed = 2
	ProgressLevelNoisy    = 3
)

type (
	TErrorReporter    func(string)
	TProgressReporter func(string)

	TReporter struct {
		errorReporter    TErrorReporter
		progressReporter TProgressReporter
	}
)

func (r *TReporter) Error(message string, context ...any) {
	r.errorReporter(fmt.Sprintf(message, context...))
}

func (r *TReporter) Panic(message string, context ...any) {
	r.Error(message+" Panicking.", context...)

	panic("")
}

func (r *TReporter) Progress(level int, message string, context ...any) {
	r.progressReporter(fmt.Sprintf(message, context...))
}

func CreateReporter(errorReporter TErrorReporter, progressReporter TProgressReporter) *TReporter {
	reporter := TReporter{}

	reporter.errorReporter = errorReporter
	reporter.progressReporter = progressReporter

	return &reporter
}
