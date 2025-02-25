package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/segmentio/kafka-go"
)

type model struct {
	choices  []string
	cursor   int
	selected map[int]struct{}
}

var (
	topic string
)

type messageEntry struct {
	LogLevel string `json:"level"`
	Message  string `json:"msg"`
}

// TODO Keep polling
func main() {
	p := tea.NewProgram(initialModel())
	m, err := p.Run()
	if err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}

	for i := range m.(model).selected {
		switch m.(model).choices[i] {
		case "Show INFO log messages":
			topic = "info-log-topic"
		case "Show DEBUG log messages":
			topic = "debug-log-topic"
		case "Show WARNING log messages":
			topic = "warn-log-topic"
		case "Show ERROR log messages":
			topic = "error-log-topic"
		}
	}

	messageEntries, _ := readWithReader(topic, "consumer-through-kafka 1")
	if messageEntries != nil {
		fmt.Printf("Got %v log messages", len(messageEntries))
	}
}

func readWithReader(topic string, groupID string) ([]*messageEntry, error) {
	slog.With("topic", topic).Info("Trying to read messages...")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 100, //per message
		// more options are available
	})

	//Create a deadline
	//TODO Maybe context instead of deadline?
	readDeadline, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(5*time.Second))
	defer cancel()

	var messageEntries []*messageEntry
	for {
		msg, err := r.ReadMessage(readDeadline)
		if string(msg.Value) == "" {
			slog.Info("No more messages")
			break
		}
		slog.With("msg", msg.Value).Info("Got a new message!")
		if err != nil {
			if err == context.DeadlineExceeded {
				break
			}
			return nil, err
		}

		var newMessageEntry messageEntry
		json.Unmarshal(msg.Value, &newMessageEntry)
		messageEntries = append(messageEntries, &newMessageEntry)
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}

	return messageEntries, nil
}

func initialModel() model {
	return model{
		choices: []string{"Show INFO log messages", "Show DEBUG log messages", "Show WARNING log messages", "Show ERROR log messages"},

		// A map which indicates which choices are selected. We're using
		// the  map like a mathematical set. The keys refer to the indexes
		// of the `choices` slice, above.
		selected: make(map[int]struct{}),
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
			os.Exit(0)

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

		// The "enter" key and the spacebar (a literal space) toggle
		// the selected state for the item that the cursor is pointing at.
		case " ":
			_, ok := m.selected[m.cursor]
			if ok {
				delete(m.selected, m.cursor)
			} else {
				m.selected[m.cursor] = struct{}{}
			}

		case "enter":
			return m, tea.Quit
		}
	}

	// Return the updated model to the Bubble Tea runtime for processing.
	// Note that we're not returning a command.
	return m, nil
}

func (m model) View() string {
	// The header
	s := "Which log level do you want to check for?\n\n"

	// Iterate over our choices
	for i, choice := range m.choices {

		// Is the cursor pointing at this choice?
		cursor := " " // no cursor
		if m.cursor == i {
			cursor = ">" // cursor!
		}

		// Is this choice selected?
		checked := " " // not selected
		if _, ok := m.selected[i]; ok {
			checked = "x" // selected!
		}

		// Render the row
		s += fmt.Sprintf("%s [%s] %s\n", cursor, checked, choice)
	}

	// The footer
	s += "\nPress q to quit.\n"

	// Send the UI for rendering
	return s
}
