package csvtimer

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
	"github.com/skothari-tibco/scheduler"
)

var triggerMd = trigger.NewMetadata(&HandlerSettings{}, &Output{})

func init() {
	trigger.Register(&Trigger{}, &Factory{})
}

type Factory struct {
}

// Metadata implements trigger.Factory.Metadata
func (*Factory) Metadata() *trigger.Metadata {
	return triggerMd
}

// New implements trigger.Factory.New
func (*Factory) New(config *trigger.Config) (trigger.Trigger, error) {
	return &Trigger{}, nil
}

type Trigger struct {
	timers   []*scheduler.Job
	handlers []trigger.Handler
	logger   log.Logger
}

// Init implements trigger.Init
func (t *Trigger) Initialize(ctx trigger.InitContext) error {
	t.handlers = ctx.GetHandlers()
	t.logger = ctx.Logger()

	return nil
}

// Start implements ext.Trigger.Start
func (t *Trigger) Start() error {

	handlers := t.handlers

	for _, handler := range handlers {

		handlerSettings := &HandlerSettings{}
		err := metadata.MapToStruct(handler.Settings(), handlerSettings, true)
		if err != nil {
			return err
		}

		if handlerSettings.RepeatInterval == "" {
			t.scheduleOnce(handler, handlerSettings)
		} else {
			t.scheduleRepeating(handler, handlerSettings)
		}
	}

	return nil
}

// Stop implements ext.Trigger.Stop
func (t *Trigger) Stop() error {

	for _, timer := range t.timers {

		if timer.IsRunning() {
			timer.Quit <- true
		}
	}

	t.timers = nil

	return nil
}

func (t *Trigger) scheduleOnce(handler trigger.Handler, settings *HandlerSettings) error {

	seconds := 0

	if settings.StartInterval != "" {
		d, err := time.ParseDuration(settings.StartInterval)
		if err != nil {
			return fmt.Errorf("unable to parse start delay: %s", err.Error())
		}

		seconds = int(d.Seconds())
		t.logger.Debugf("Scheduling action to run once in %d seconds", seconds)
	}

	timerJob := scheduler.Every(seconds).Seconds()

	fn := func() {
		t.logger.Debug("Executing \"Once\" timer trigger")

		data, err := ReadCsv(settings.FilePath)

		triggerData := &Output{}

		triggerData.Error = ""
		if settings.Header {

			obj := make(map[string]interface{})
			for i := 0; i < len(data); i++ {
				for j := 0; j < len(data[0]); j++ {
					if num, err := strconv.ParseFloat(data[i][j], 64); err == nil {
						obj[data[0][j]] = num
					} else {
						obj[data[0][j]] = data[i][j]
					}
				}
			}

			triggerData.Data = obj

		} else {
			triggerData.Data = data
		}
		t.logger.Debug("Passing data to handler", triggerData.Data)
		_, err = handler.Handle(context.Background(), triggerData)
		if err != nil {
			t.logger.Error("Error running handler: ", err.Error())
		}

		if timerJob != nil {
			timerJob.Quit <- true
		}
	}

	if seconds == 0 {
		t.logger.Debug("Start delay not specified, executing action immediately")
		fn()
	} else {
		timerJob, err := timerJob.NotImmediately().Run(fn)
		if err != nil {
			t.logger.Error("Error scheduling execute \"once\" timer: ", err.Error())
		}

		t.timers = append(t.timers, timerJob)
	}
	t.Stop()

	return nil
}

func (t *Trigger) scheduleRepeating(handler trigger.Handler, settings *HandlerSettings) error {
	t.logger.Info("Scheduling a repeating timer")
	var header []string
	startSeconds := 0

	repeatInterval, _ := strconv.Atoi(settings.RepeatInterval)
	settings.Count = 0
	t.logger.Info("reapeat", repeatInterval)
	t.logger.Debugf("Scheduling action to repeat every %d seconds", repeatInterval)

	fn := func() {
		t.logger.Debug("Executing \"Repeating\" timer")

		triggerData := &Output{}
		data, err := ReadCsvInterval(settings)
		if err != nil {
			t.Stop()

		}

		triggerData.Error = ""
		if settings.Header {
			if len(header) == 0 {
				header = data

			} else {
				obj := make(map[string]interface{})
				for i := 0; i < len(data); i++ {
					if num, err := strconv.ParseFloat(data[i], 64); err == nil {
						obj[header[i]] = num
					} else {
						obj[header[i]] = data[i]
					}

				}

				triggerData.Data = obj
			}

		} else {
			triggerData.Data = data
		}

		t.logger.Debug("Passing data to handler", triggerData.Data)

		_, err = handler.Handle(context.Background(), triggerData)

		settings.Count++

		if err != nil {
			t.logger.Error("Error running handler: ", err.Error())
		}
	}

	if startSeconds == 0 {

		timerJob, err := scheduler.Every(repeatInterval).MilliSeconds().Run(fn)
		if err != nil {
			t.logger.Error("Error scheduling repeating timer: ", err.Error())
		}

		t.timers = append(t.timers, timerJob)
	}

	return nil
}

func ReadCsv(path string) ([][]string, error) {

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()

	if err != nil {
		return nil, err
	}

	return lines, nil
}

func ReadCsvInterval(settings *HandlerSettings) ([]string, error) {

	if settings.Count == 0 {
		data, err := ReadCsv(settings.FilePath)
		if err != nil {
			return nil, err
		}
		settings.Lines = data

		settings.Count++

		return settings.Lines[0], nil
	}
	if settings.Count == len(settings.Lines) {
		return nil, errors.New("Done")
	}
	return settings.Lines[settings.Count], nil

}
