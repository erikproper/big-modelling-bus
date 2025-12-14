/*
 *
 * Module:    BIG Modelling Bus
 * Package:   Generic
 * Component: Reporting
 *
 * This component is concerned with the reporting of errors, progress, etc, to the user.
 * For the moment, it only involves the reporting of progress and errors, including panics.
 *
 * Author: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: 27.11.2025
 *
 */

package generics

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
		reportingLevel   int
		errorReporter    TErrorReporter
		progressReporter TProgressReporter
	}
)

func (r *TReporter) Error(message string, context ...any) {
	r.errorReporter(fmt.Sprintf(message, context...))
}

func (r *TReporter) ReportError(message string, err error) {
	r.Error(message+" %s", err)
}

func (r *TReporter) MaybeReportError(message string, err error) bool {
	if err != nil {
		r.Error(message, err)

		return false
	}

	return true
}

func (r *TReporter) Panic(message string, context ...any) {
	r.Error(message+" Panicking.", context...)

	panic("")
}

func (r *TReporter) PanicError(message string, err error) {
	r.Panic(message+" %s", err)

	panic("")
}

func (r *TReporter) Progress(level int, message string, context ...any) {
	if level <= r.reportingLevel {
		r.progressReporter(fmt.Sprintf(message, context...))
	}
}

func CreateReporter(level int, errorReporter TErrorReporter, progressReporter TProgressReporter) *TReporter {
	reporter := TReporter{}

	reporter.errorReporter = errorReporter
	reporter.progressReporter = progressReporter
	reporter.reportingLevel = level

	return &reporter
}
