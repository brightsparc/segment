package segment

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/xtgo/uuid"
)

type ProjectId func(writeKey string) string

type Segment struct {
	Logger       *log.Logger
	projectId    ProjectId
	destinations []Destination
}

// TODO: Pass in an array of destinations

// NewSegment create new segment handler given project and delivery config
func NewSegment(projectId func(writeKey string) string, destinations []Destination, router *mux.Router) *Segment {
	s := &Segment{
		Logger:       log.New(os.Stderr, "", log.LstdFlags),
		projectId:    projectId,
		destinations: destinations,
	}

	s.Logger.Println("Adding Segment handlers")
	router.HandleFunc("/batch", prometheus.InstrumentHandlerFunc("batch", s.handleBatch)).Methods("POST")
	router.HandleFunc("/{event:p|page|i|identify|t|track}", prometheus.InstrumentHandlerFunc("event", s.handleEvent))

	return s
}

// SegmentMessage fields common to all.
type SegmentMessage struct {
	MessageId    string                 `json:"messageId"`
	Timestamp    time.Time              `json:"timestamp"`
	SentAt       time.Time              `json:"sentAt,omitempty"`
	ProjectId    string                 `json:"projectId"`
	Type         string                 `json:"type"`
	Context      map[string]interface{} `json:"context,omitempty"` // Duplicate here for batch
	Properties   map[string]interface{} `json:"properties,omitempty"`
	Traits       map[string]interface{} `json:"traits,omitempty"`
	Integrations map[string]interface{} `json:"integrations,omitempty"` // Probably won't use
	AnonymousId  string                 `json:"anonymousId,omitempty"`
	UserId       string                 `json:"userId,omitempty"`
	Event        string                 `json:"event,omitempty"`    // Track only
	Category     string                 `json:"category,omitempty"` // Page only
	Name         string                 `json:"name,omitempty"`     // Page only
}

// SegmentBatch contains batch of messages
type SegmentBatch struct {
	MessageId string                 `json:"messageId,omitempty"`
	Timestamp time.Time              `json:"timestamp,omitempty"`
	SentAt    time.Time              `json:"sentAt,omitempty"`
	Context   map[string]interface{} `json:"context,omitempty"`
	Messages  []SegmentMessage       `json:"batch"`
}

// SegmentEvent is single message with write key
type SegmentEvent struct {
	WriteKey string `json:"writeKey,omitempty"` // Read clear, and set proejctId
	SegmentMessage
}

func (s *Segment) handleBatch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var batch SegmentBatch
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&batch)
	if err != nil {
		s.Logger.Println("Batch decode error", err)
		http.Error(w, `{ "success": false }`, http.StatusBadRequest)
		return
	}

	// Get writeKey as Basic auth user
	writeKey, _, ok := r.BasicAuth()
	if !ok {
		s.Logger.Println("Basic Authorization expected")
		http.Error(w, `{ "success": false }`, http.StatusUnauthorized)
		return
	}
	projectId := s.projectId(writeKey)
	if projectId == "" {
		s.Logger.Printf("Unable to get projectId for writeKey: %s\n", writeKey)
		http.Error(w, `{ "success": false }`, http.StatusUnauthorized)
		return
	}

	// Push each of these Segment updating the context
	ctx, cancel := contextTimeout(r)
	defer cancel()
	for _, m := range batch.Messages {
		event := SegmentEvent{
			WriteKey:       writeKey,
			SegmentMessage: m,
		}
		event.ProjectId = projectId
		event.Context = batch.Context
		if err := s.send(ctx, event); err != nil {
			s.Logger.Println("Send error", err)
			http.Error(w, `{ "success": false }`, http.StatusInternalServerError)
			return
		}
	}

	// Write back the metrics
	fmt.Fprintf(w, `{ "success": true }`)
}

func (s *Segment) handleEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var body io.Reader
	if r.Method == "GET" {
		payload := r.FormValue("data")
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			s.Logger.Printf("Expected base64 bayload: %s -- %v\n", payload, err)
			http.Error(w, `{ "success": false }`, http.StatusBadRequest)
			return
		}
		body = bytes.NewReader(data)
	} else {
		body = r.Body
	}

	// Default segment event with writeKey and event type from path
	writeKey, _, _ := r.BasicAuth()
	vars := mux.Vars(r)
	event := SegmentEvent{writeKey, SegmentMessage{Type: vars["event"]}}
	decoder := json.NewDecoder(body)
	err := decoder.Decode(&event)
	if err != nil {
		s.Logger.Println("Event decode error", err)
		http.Error(w, `{ "success": false }`, http.StatusBadRequest)
		return
	}

	// Set the project key
	event.ProjectId = s.projectId(event.WriteKey)
	if event.ProjectId == "" {
		s.Logger.Printf("Unable to get projectId for writeKey: %s \n", event.WriteKey)
		http.Error(w, `{ "success": false }`, http.StatusBadRequest)
		return
	}

	// Get context timeout
	ctx, cancel := contextTimeout(r)
	defer cancel()
	if err = s.send(ctx, event); err != nil {
		s.Logger.Println("Send error", err)
		http.Error(w, `{ "success": false }`, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, `{ "success": true }`)
}

func contextTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	timeout, err := time.ParseDuration(r.FormValue("timeout"))
	if err == nil {
		return context.WithTimeout(context.Background(), timeout)
	} else {
		return context.WithCancel(context.Background()) // No timeout
	}
}

func (s *Segment) send(ctx context.Context, m SegmentEvent) error {
	// Update the timestamp
	if m.Timestamp == (time.Time{}) {
		m.Timestamp = time.Now()
	}
	m.SentAt = time.Now()
	// Set unique messageId if none
	if m.MessageId == "" {
		m.MessageId = uuid.NewRandom().String()
	}

	// Call destination send, breaking on first error respecting timeout
	for _, dest := range s.destinations {
		if err := dest.Send(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

// Run this as go-routine to processes the messages, and optionally send updates
func (s *Segment) Run(ctx context.Context) <-chan error {
	done := make(chan error, len(s.destinations))

	var wg sync.WaitGroup
	wg.Add(len(s.destinations))
	for _, dest := range s.destinations {
		go func(dest Destination) {
			done <- dest.Process(ctx)
			wg.Done()
		}(dest)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	return done
}
