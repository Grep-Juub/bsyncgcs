package utils

import (
	"log"
	"os"
)

func SetupLogger(loggerFile string) error {
	if loggerFile != "" {
		logFile, err := os.OpenFile(loggerFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		log.SetOutput(logFile)
	} else {
		log.SetOutput(os.Stdout)
	}
	return nil
}
