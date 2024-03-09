package mq

import "github.com/google/uuid"

<<<<<<< Updated upstream
=======
type TaskMeta struct {
	Payload        []byte `json:"payload"`
	MaxRetries     int    `json:"max_retries"`
	CurrentRetries int    `json:"current_retries"`
	CronExpr       string `json:"cron_expr"`
}

>>>>>>> Stashed changes
type Task struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Payload []byte `json:"payload"`
}

func NewTask(name string, payload []byte) Task {
	id := uuid.New().String()
	return Task{Id: id, Name: name, Payload: payload}
}
