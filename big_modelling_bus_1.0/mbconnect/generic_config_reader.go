/*
 *
 * Package: mbconnect
 * Layer:   generic
 * Module:  config_reader
 *
 * This module reads ini files.
 * It gladly uses the functionality provided by "gopkg.in/ini.v1".
 * Nevertheless, having our own configuration loader makes the rest of the mbconnect code less dependent on potential changes
 *
 * Creator: Henderik A. Proper (e.proper@acm.org), TU Wien, Austria
 *
 * Version of: XX.11.2025
 *
 */

package mbconnect

import (
	"gopkg.in/ini.v1"
)

type (
	TConfigData struct {
		configFile *ini.File
	}

	TConfigValue struct {
		configKey *ini.Key
	}
)

func LoadConfig(filePath string, reporter *TReporter) *TConfigData {
	var (
		err        error
		configData TConfigData
	)

	reporter.Progress("Using config: %s", filePath)
	configData.configFile, err = ini.Load(filePath)

	if err != nil {
		reporter.Panic("Failed to read config file. %s", err)
	}

	return &configData
}

func (c *TConfigData) GetValue(section, key string) *TConfigValue {
	var configValue TConfigValue

	configValue.configKey = c.configFile.Section(section).Key(key)

	return &configValue
}

func (v *TConfigValue) StringWithDefault(defaultString string) string {
	s := v.configKey.String()
	if s == "" {
		return defaultString
	} else {
		return s
	}
}

func (v *TConfigValue) String() string {
	return v.StringWithDefault("")
}

func (v *TConfigValue) BoolWithDefault(defaultBool bool) bool {
	keyBool, err := v.configKey.Bool()
	if err == nil {
		return keyBool
	} else {
		return defaultBool
	}
}

func (v *TConfigValue) Bool() bool {
	return v.BoolWithDefault(false)
}

func (v *TConfigValue) IntWithDefault(defaultInt int) int {
	keyInt, err := v.configKey.Int()
	if err == nil {
		return keyInt
	} else {
		return defaultInt
	}
}

func (v *TConfigValue) Int() int {
	return v.IntWithDefault(0)
}
