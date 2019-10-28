// Consume messages from a topic named "libtest"
// Transform consumed messages by updating actual_qty < 10 to 0
// Produce consumed messages to another topic "producer"

package main

import (
	"context"
	"fmt"

	"encoding/json"

	kproducer "github.com/project-flogo/contrib/activity/kafka"
	kconsumer "github.com/project-flogo/contrib/trigger/kafka"
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

	newTrigger := app.NewTrigger(&kconsumer.Trigger{}, &kconsumer.Settings{
		BrokerUrls: "localhost:9092",
	})

	handler, _ := newTrigger.NewHandler(&kconsumer.HandlerSettings{
		Topic:  "libtest",
		Offset: 0,
	})

	_, err := handler.NewAction(RunStream)
	if err != nil {
		panic(err)
	}

	//store in map to avoid activity instance recreation
	settings := &kproducer.Settings{BrokerUrls: "localhost:9092", Topic: "producer"}
	producerAct, _ := api.NewActivity(&kproducer.Activity{}, settings)
	activities = map[string]activity.Activity{"producer": producerAct}

	return app
}

var activities map[string]activity.Activity

func RunStream(ctx context.Context, inputs map[string]interface{}) (map[string]interface{}, error) {
	// cosume each message
	triggerOut := &kconsumer.Output{
		Message: inputs["message"].(string),
	}
	// output each message to the log
	activity.GetLogger("log").Info("------Consumed msg: " + triggerOut.Message)

	// transform consumed messages
	var msg map[string]interface{}
	json.Unmarshal([]byte(triggerOut.Message), &msg)
	qty := msg["actual_qty"].(float64)
	if qty < 10 {
		msg["actual_qty"] = 0
	}

	// produce messages to another topic
	outputMsg, _ := json.Marshal(msg)

	_, err := api.EvalActivity(activities["producer"], &kproducer.Input{Message: string(outputMsg)})
	if err != nil {
		return nil, err
	}
	activity.GetLogger("log").Info("++++++Produced msg: " + string(outputMsg))

	return nil, nil
}
