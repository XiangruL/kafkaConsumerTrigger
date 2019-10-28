package main

import (
	"context"
	"fmt"

	"github.com/project-flogo/contrib/activity/log"
	"github.com/project-flogo/contrib/trigger/kafka"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/api"
	"github.com/project-flogo/core/engine"
)

func main() {
	app := TestApp()

	e, err := api.NewEngine(app)

	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	engine.RunEngine(e)
}

func TestApp() *api.App {
	app := api.NewApp()

	newTrigger := app.NewTrigger(&kafka.Trigger{}, &kafka.Settings{
		BrokerUrls: "localhost:9092",
	})

	handler, _ := newTrigger.NewHandler(&kafka.HandlerSettings{
		Topic:  "libtest",
		Offset: 0,
	})

	_, err := handler.NewAction(RunStream)
	if err != nil {
		panic(err)
	}

	//store in map to avoid activity instance recreation
	logAct, _ := api.NewActivity(&log.Activity{})
	activities = map[string]activity.Activity{"log": logAct}

	return app
}

var activities map[string]activity.Activity

func RunStream(ctx context.Context, inputs map[string]interface{}) (map[string]interface{}, error) {
	triggerOut := &kafka.Output{
		Message: inputs["message"].(string),
	}

	// cosume each message
	// // output each message to the log
	_, err := api.EvalActivity(activities["log"], &log.Input{Message: triggerOut.Message})
	if err != nil {
		return nil, err
	}

	return nil, nil
}
