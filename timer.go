package csvtimer

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/skothari-tibco/scheduler"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
)

var COUNT int
var LINES [][]string



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
	COUNT = 0
	t.handlers = ctx.GetHandlers()
	t.logger = ctx.Logger()

	return nil
}

// Start implements ext.Trigger.Start
func (t *Trigger) Start() error {

	handlers := t.handlers

	for _, handler := range handlers {

		s := &HandlerSettings{}
		err := metadata.MapToStruct(handler.Settings(), s, true)
		if err != nil {
			return err
		}

		if s.RepeatInterval == "" {
			t.scheduleOnce(handler, s)
		} else {
			t.scheduleRepeating(handler, s)
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


		triggerData.Data = data

		triggerData.Error = ""


		triggerData.Error = ""
		t.logger.Debug("Passing data to handler", data)
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

	return nil
}

func (t *Trigger) scheduleRepeating(handler trigger.Handler, settings *HandlerSettings) error {
	t.logger.Info("Scheduling a repeating timer")
	
	startSeconds := 0

	repeatInterval, _ := strconv.Atoi(settings.RepeatInterval)
	t.logger.Info("reapeat", repeatInterval)
	t.logger.Debugf("Scheduling action to repeat every %d seconds", repeatInterval)

	fn := func() {
		t.logger.Debug("Executing \"Repeating\" timer")

		triggerData := &Output{}
		data, err := ReadCsvInterval(settings.FilePath)
		if err != nil {
			return
		}
		triggerData.Data = data

		triggerData.Error = ""

		t.logger.Debug("Passing data to handler", data)
		_, err = handler.Handle(context.Background(), triggerData)
		COUNT = COUNT + 1
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

//type PrintJob struct {
//	Msg string
//}
//
//func (j *PrintJob) Run() error {
//	t.logger.Debug(j.Msg)
//	return nil
//}

func ReadCsv(path string) (interface{}, error) {

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()

	if err != nil {
		return "", err
	}

	return lines, nil
}

func ReadCsvInterval(path string) (interface{}, error) {
	if COUNT == 0 {
		data, err := ReadCsv(path)
		if err != nil {
			return nil, err
		}
		LINES = data.([][]string)

		COUNT = COUNT + 1

		return LINES[0], nil
	}
	if COUNT == len(LINES) {
		return nil, errors.New("Done")
	}
	return LINES[COUNT], nil

}