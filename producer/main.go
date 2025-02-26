package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageEntry struct {
	LogLevel LogLevel `json:"level"`
	Message  string   `json:"msg"`
}

type LogLevel string

const (
	Info    LogLevel = "INFO"
	Debug   LogLevel = "DEBUG"
	Warning LogLevel = "WARN"
	Error   LogLevel = "ERROR"
)

var infoConnection, _ = connect("info-log-topic", 0)
var debugConnection, _ = connect("debug-log-topic", 0)
var warnConnection, _ = connect("warn-log-topic", 0)
var errorConnection, _ = connect("error-log-topic", 0)

var connectionsByLogLevel = map[LogLevel]*kafka.Conn{
	Info:    infoConnection,
	Debug:   debugConnection,
	Warning: warnConnection,
	Error:   errorConnection,
}

// Sample messages with placeholders
var possibleMessages = []string{
	"User %d logged in",
	"Connection to %s failed",
	"Task completed in %dms",
	"Array values: %v",
	"Cache hit ratio: %.2f",
	"Service %s restarted",
}

// Log levels for sample logs
var logLevels = []LogLevel{Info, Debug, Error, Warning}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go generateLogMessages()

	<-ctx.Done()
	infoConnection.Close()
	debugConnection.Close()
	warnConnection.Close()
	errorConnection.Close()
	slog.Info("Producer shutdown!")
}

func connect(topic string, partition int) (*kafka.Conn, error) {
	slog.With("topic", topic).Info("Connected to topic")
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader")
	}
	return conn, err
}

func generateLogMessages() {
	for {
		message := createRandomMessage()
		writeMessageToTopic(message.LogLevel, message)
		time.Sleep(1 * time.Second)
	}
}

func writeMessageToTopic(logLevel LogLevel, msg messageEntry) {
	conn := connectionsByLogLevel[logLevel]
	var err error
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(kafka.Message{Value: []byte(msg.Message)})
	if err != nil {
		slog.With("err", err).Error("Writing messages failed")
	}
}

func createRandomMessage() messageEntry {
	rand.Seed(time.Now().UnixNano())

	// Select a random log level
	level := logLevels[rand.Intn(len(logLevels))]

	// Select a random message template
	msgTemplate := possibleMessages[rand.Intn(len(possibleMessages))]

	// Generate random values based on the message type
	var logMessage string
	switch msgTemplate {
	case "User %d logged in":
		logMessage = fmt.Sprintf(msgTemplate, rand.Intn(1000))
	case "Connection to %s failed":
		logMessage = fmt.Sprintf(msgTemplate, []string{"DB", "API", "Cache"}[rand.Intn(3)])
	case "Task completed in %dms":
		logMessage = fmt.Sprintf(msgTemplate, rand.Intn(5000))
	case "Array values: %v":
		arr := []int{rand.Intn(10), rand.Intn(10), rand.Intn(10)}
		logMessage = fmt.Sprintf(msgTemplate, arr)
	case "Cache hit ratio: %.2f":
		logMessage = fmt.Sprintf(msgTemplate, rand.Float64()*100)
	case "Service %s restarted":
		logMessage = fmt.Sprintf(msgTemplate, []string{"Auth", "Payment", "Search"}[rand.Intn(3)])
	}

	newMessageEntry := messageEntry{
		LogLevel: level,
		Message:  logMessage,
	}

	return newMessageEntry
}
