package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Log levels
var logLevels = []string{"INFO", "DEBUG", "ERROR", "WARN"}

type LogLevel string

const (
	Info    LogLevel = "info"
	Debug   LogLevel = "debug"
	Warning LogLevel = "warn"
	Error   LogLevel = "error"
)

type kafkaTopic struct {
	isEnabled  bool
	connection *kafka.Conn
}

var infoConnection, _ = connect("info-log-topic", 0)
var debugConnection, _ = connect("debug-log-topic", 0)
var warnConnection, _ = connect("warn-log-topic", 0)
var errorConnection, _ = connect("error-log-topic", 0)

var logLevelToTopic = map[LogLevel]kafkaTopic{
	Info: {
		isEnabled:  false,
		connection: infoConnection,
	},
	Debug: {
		isEnabled:  false,
		connection: debugConnection,
	},
	Warning: {
		isEnabled:  false,
		connection: warnConnection,
	},
	Error: {
		isEnabled:  false,
		connection: errorConnection,
	},
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

var (
	infoLogs  []string
	debugLogs []string
	warnLogs  []string
	errorLogs []string

	topicList        []string
	topic            string
	infoTopicInList  bool
	debugTopicInList bool
	warnTopicInList  bool
	errorTopicInList bool
)

type messageEntry struct {
	LogLevel string `json:"level"`
	Message  string `json:"msg"`
}

func main() {
	//Generate 5 messages
	messages := createRandomMessages(5)

	//Convert message strings into messageEntries
	var messageEntries []messageEntry
	for _, msg := range messages {
		newMessageEntry := new(messageEntry)
		json.Unmarshal([]byte(msg), &newMessageEntry)
		messageEntries = append(messageEntries, *newMessageEntry)
	}

	//Sort messageEntries by log level
	sortEntries(messageEntries)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	partition := 0
	//Connect to Kafka based on log level (topic)
	//TODO Refactor with map enum[kafka-topic]
	for _, topicName := range topicList {
		topic = topicName + "-log-topic"
		conn, err := connect(topic, partition)
		if err != nil {
			fmt.Print("Failed to connect")
		}
		defer conn.Close()
		if topicName == "info" {
			slog.With("amount", len(infoLogs)).Info("Sending logs to info topic")
			writeMessages(conn, infoLogs)
		}
		if topicName == "debug" {
			slog.With("amount", len(debugLogs)).Info("Sending logs to debug topic")
			writeMessages(conn, debugLogs)
		}
		if topicName == "warn" {
			slog.With("amount", len(warnLogs)).Info("Sending logs to warn topic")
			writeMessages(conn, warnLogs)
		}
		if topicName == "error" {
			slog.With("amount", len(errorLogs)).Info("Sending logs to error topic")
			writeMessages(conn, errorLogs)
		}
	}

	<-ctx.Done()
	slog.Info("Producer shutdown!")
}

// TODO Refactor with map enum[kafka-topic]
func sortEntries(messageEntries []messageEntry) {
	//Sort messageEntries by log level
	for _, entry := range messageEntries {
		switch entry.LogLevel {
		case "INFO":
			jsonEntry, err := json.Marshal(entry)
			if err != nil {
				slog.Error("Failed to get jsonEntry")
			}
			infoLogs = append(infoLogs, string(jsonEntry))
			if !infoTopicInList {
				topicList = append(topicList, "info")
				infoTopicInList = true
			}
		case "DEBUG":
			jsonEntry, err := json.Marshal(entry)
			if err != nil {
				slog.Error("Failed to get jsonEntry")
			}
			debugLogs = append(debugLogs, string(jsonEntry))
			if !debugTopicInList {
				topicList = append(topicList, "debug")
				debugTopicInList = true
			}
		case "WARN":
			jsonEntry, err := json.Marshal(entry)
			if err != nil {
				slog.Error("Failed to get jsonEntry")
			}
			warnLogs = append(warnLogs, string(jsonEntry))
			if !warnTopicInList {
				topicList = append(topicList, "warn")
				warnTopicInList = true
			}
		case "ERROR":
			jsonEntry, err := json.Marshal(entry)
			if err != nil {
				slog.Error("Failed to get jsonEntry")
			}
			errorLogs = append(errorLogs, string(jsonEntry))
			if !errorTopicInList {
				topicList = append(topicList, "error")
				errorTopicInList = true
			}
		default:
			slog.Error("Missmatching LogLevel")
		}
	}
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

func writeMessages(conn *kafka.Conn, msgs []string) {
	var err error
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for _, msg := range msgs {
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(msg)})
	}
	if err != nil {
		slog.With("err", err).Error("Writing messages failed")
	}
}

func createRandomMessages(amount int) []string {
	var messages []string
	for i := 0; i < amount; i++ {
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

		jsonMessage, _ := json.Marshal(newMessageEntry)

		messages = append(messages, string(jsonMessage))
	}
	return messages
}
