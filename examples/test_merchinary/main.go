package main

import (
	// "fmt"
	"log"


	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	// mlog "github.com/RichardKnop/machinery/v1/log"
	v1tasks "github.com/RichardKnop/machinery/v1/tasks"
	// "github.com/sirupsen/logrus"
	// "github.com/ydf0509/gofunboost/core"
)

func Add(x, y int64) (int64, error) {
	return x + y, nil
}

func main() {


	cnf := config.Config{
		Broker:          "redis://localhost:6379",
		ResultBackend:   "redis://localhost:6379",
		DefaultQueue:    "machinery_tasks2",
		ResultsExpireIn: 3600,
		// Backend: "rpc",
	}

	server, err := machinery.NewServer(&cnf)
	if err != nil {
		panic(err)
	}

	server.RegisterTask("tasks.add", Add)
	log.Println(" start psuh")
	for i := 0; i < 100000; i++ {
		signature := &v1tasks.Signature{
			Name: "tasks.add",
			Args: []v1tasks.Arg{
				{Type: "int64", Value: 2},
				{Type: "int64", Value: 3},
			},
		}

		// task, err := server.SendTask(signature)
		_, err := server.SendTask(signature)
		if err != nil {
			panic(err)
		}
		
		// fmt.Printf("Task sent: %s\n", task.Signature.UUID)
	}
	log.Println(" finish psuh")

	// worker := server.NewWorker("worker_name", 1)
	// go func() {
	// 	err := worker.Launch()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()

	select {}
}
