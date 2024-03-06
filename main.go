package main

import (
	mq "github.com/Priyanka488/monkqueue/mq"
)

func main() {

	// Create a new client

	client := mq.NewClient(mq.RedisConfig{
		Address: "localhost:6379",
	})

	// Enqueue a message
	client.Enqueue("task_1")

}
