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
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/backo-go"
	"github.com/xtgo/uuid"
)

// ProjectId is the func definition to return string based on writeKey
type ProjectId func(writeKey string) string

// Segment is intialized with proejctId and destinations
type Segment struct {
	Logger       *log.Logger
	projectId    ProjectId
	destinations []Destination
	backo        *backo.Backo
	backoRetry   int
}

// NewSegment create new segment handler given project and delivery config
func NewSegment(projectId ProjectId, destinations []Destination, router *mux.Router) *Segment {
	s := &Segment{
		Logger:       log.New(os.Stderr, "", log.LstdFlags),
		projectId:    projectId,
		destinations: destinations,
		backo:        backo.DefaultBacko(), // 100 milliseconds, up to 10 seconds
		backoRetry:   10,
	}

	s.Logger.Println("Adding Segment handlers")
	router.HandleFunc("/batch", s.handleBatch).Methods("POST")
	router.HandleFunc("/{event:p|page|i|identify|t|track|a|alias|g|group|screen}", s.handleEvent)

	return s
}

// WithLogger propogates the logger down to destinations
func (s *Segment) WithLogger(logger *log.Logger) *Segment {
	if logger != nil {
		for _, dest := range s.destinations {
			dest.WithLogger(logger)
		}
		s.Logger = logger
	}
	return s
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

	fmt.Fprintf(w, `{ "success": true }`)
}

func (s *Segment) handleEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Support GET method with base64 encoded `data` payload
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

	// Default segment event with writeKey and event type from url path
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
	if m.Timestamp == (time.Time{}) {
		m.Timestamp = time.Now()
	}
	m.SentAt = time.Now()
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
func (s *Segment) Run(ctx context.Context) {
	for _, dest := range s.destinations {
		go func(dest Destination) {
			var err error
			for i := 0; i < s.backoRetry; i++ {
				if err = dest.Process(ctx); err == nil {
					break
				}
				s.Logger.Printf("Process retrying in %s due to error: %v\n", s.backo.Duration(i), err)
				s.backo.Sleep(i)
			}
			// Quit if still error after 3 retries
			if err != nil {
				s.Logger.Fatal(err)
			}
		}(dest)
	}
}
