package main

import (
	"errors"
	"fmt"
	"time"
)

// Сколько тасок может обрабатываться одновременно
const POOL_SIZE = 10000

func main() {
	print(process(generate()))

}

type Task struct {
	id            int
	creationTime  time.Time // время создания
	executionTime time.Time // время выполнения
	result        []byte
	err           error
}

// Пайплайн

// Генерация тасок
func generate() <-chan Task {
	output := make(chan Task, POOL_SIZE)
	go func() {
		for {
			task := Task{creationTime: time.Now(), id: int(time.Now().Unix())}
			output <- task // передаем таск на выполнение
		}
	}()

	return output
}

// Обработка
func process(in <-chan Task) <-chan Task {
	output := make(chan Task, POOL_SIZE)

	for i := 0; i < POOL_SIZE; i++ {
		go func() {
			for task := range in {
				task.executionTime = time.Now()
				result, err := executeTask(&task)
				task.result = result
				task.err = err
				output <- task
			}
		}()
	}

	return output
}

// Вывод
func print(in <-chan Task) {
	for task := range in {
		printTask(&task)
	}
}

// Helpers

func executeTask(task *Task) ([]byte, error) {
	// Имитируем какие-то длительные действия
	time.Sleep(time.Millisecond * 150)

	if task.creationTime.Nanosecond()%2 > 0 { // вот такое условие появления ошибочных тасков
		return nil, errors.New("Some error occured")
	}

	if task.creationTime.After(time.Now().Add(-20 * time.Second)) {
		return []byte("task has been successed"), nil
	}

	return nil, errors.New(("something went wrong"))
}

func printTask(task *Task) {
	if task.err != nil {
		fmt.Printf("Task id: %d, created at: %s, executed at: %s, something went wrong: %s \n", task.id, task.creationTime.Format(time.RFC3339), task.executionTime.Format(time.RFC3339Nano), task.err)
	} else {
		fmt.Printf("Task id: %d, created at: %s, executed at: %s, result: %s \n", task.id, task.creationTime.Format(time.RFC3339), task.executionTime.Format(time.RFC3339Nano), task.result)
	}
}
