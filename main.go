package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"os"

	mq "github.com/Priyanka488/monkqueue/mq"
)

type DeleteFileTask struct {
	FileName string `json:"file_name"`
}

var logger *slog.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

func main() {

	// Create a new client

	client := mq.NewClient(mq.RedisConfig{
		Address: "localhost:6379",
	})

	// Enqueue a message
	payload, err := json.Marshal(DeleteFileTask{FileName: "file_1"})
	if err != nil {
		logger.Error("Error marshalling payload", slog.String("error:", err.Error()))
	}
	task := mq.NewTask("delete_file", payload)
	err = client.Enqueue(task)
	if err != nil {
<<<<<<< Updated upstream
		logger.Error("Error enqueuing task", slog.String("error:", err.Error()))
=======
		logger.Error("Error marshalling payload", slog.String("error:", err.Error()))
	}

	task_meta = mq.TaskMeta{
		Payload:        payload,
		MaxRetries:     3,
		CurrentRetries: 0,
		CronExpr:       "*/15 * * * * *", // Every 15 seconds
	}

	task = mq.NewTask("create_file", task_meta)
	tasks = append(tasks, task)

	// Enqueue the tasks
	for _, t := range tasks {
		err := client.Enqueue(t)
		if err != nil {
			logger.Error("Error enqueuing task", slog.String("error:", err.Error()))
		}
>>>>>>> Stashed changes
	}
	logger.Info("Task enqueued", slog.String("task_id:", task.Id))

	// Create a server
	server := mq.NewServer(mq.RedisConfig{
		Address: "localhost:6379",
	}, 10)

	mux := mq.NewServeMux()
	mux.HandleFunc("delete_file", func(task *mq.Task) error {
		var t DeleteFileTask
		err := json.Unmarshal(task.Payload, &t)
		if err != nil {
			return err
		}
		logger.Info("Deleting file...", slog.String("file_name:", t.FileName))
		return nil
	})

	if err := server.Run(mux); err != nil {
		log.Fatal(err)
	}

}
