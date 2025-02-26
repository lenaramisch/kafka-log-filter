package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/segmentio/kafka-go"
)

type model struct {
	choices           []string
	cursor            int
	ctx               context.Context
	isShowingConsumer bool
}

type messageEntry struct {
	LogLevel string `json:"level"`
	Message  string `json:"msg"`
}

var (
	topicMap = map[int]string{
		0: "info-log-topic",
		1: "debug-log-topic",
		2: "warn-log-topic",
		3: "error-log-topic",
	}
	counter int
)

// TODO Keep polling
func main() {
	// write logs to file
	file, err := os.OpenFile("logs.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p := tea.NewProgram(initialModel(ctx))
	_, err = p.Run()
	if err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}

	slog.With("messages", counter).Info("Shutting down app, got messages")
}

func (m *model) handleChoice() {
	m.isShowingConsumer = true
	topic := topicMap[m.cursor]
	m.Update(tea.ClearScreen())
	go readWithReader(topic, "consumer-through-kafka 1", m.ctx, &counter)
}

func readWithReader(topic string, groupID string, ctx context.Context, counter *int) {
	slog.With("topic", topic).Info("Trying to read messages...")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: groupID,
		Topic:   topic,
		// more options are available
		MaxWait: 200 * time.Millisecond,
	})

	//Create a deadline
	for {
		msg, err := r.ReadMessage(ctx)
		if string(msg.Value) == "" {
			break
		}
		slog.Info(string(msg.Value))
		if err != nil {
			if err == context.DeadlineExceeded {
				break
			}
			panic(err)
		}

		var newMessageEntry messageEntry
		json.Unmarshal(msg.Value, &newMessageEntry)
		*counter++
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
	fmt.Println("Closed Kafka Reader")
}

func initialModel(ctx context.Context) model {
	return model{
		choices: []string{
			"Show INFO log messages",
			"Show DEBUG log messages",
			"Show WARNING log messages",
			"Show ERROR log messages",
		},
		ctx: ctx,
	}
}

func (m model) Init() tea.Cmd {
	// Just return `nil`, which means "no I/O right now, please."
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	// Is it a key press?
	case tea.KeyMsg:

		// Cool, what was the actual key pressed?
		switch msg.String() {

		// These keys should exit the program.
		case "ctrl+c", "q":
			return m, tea.Quit

		// The "up" and "k" keys move the cursor up
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		// The "down" and "j" keys move the cursor down
		case "down", "j":
			if m.cursor < len(m.choices)-1 {
				m.cursor++
			}

		case "enter":
			if !m.isShowingConsumer {
				m.handleChoice()
			}
			return m, nil
		}
	}

	// Return the updated model to the Bubble Tea runtime for processing.
	// Note that we're not returning a command.
	return m, nil
}

func (m model) View() string {
	if m.isShowingConsumer {
		return ""
	}
	// The header
	s := "Which log level do you want to check for?\n\n"

	// Iterate over our choices
	for i, choice := range m.choices {

		// Is the cursor pointing at this choice?
		cursor := " " // no cursor
		if m.cursor == i {
			cursor = ">" // cursor!
		}

		// Render the row
		s += fmt.Sprintf("%s %s\n", cursor, choice)
	}

	// The footer
	s += "\nPress q to quit.\n"

	// Send the UI for rendering
	return s
}
