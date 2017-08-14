package segment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Create a summary to track delivery stream latency
	forwarderSuccessCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "forwarder_success_total",
		Help: "Forwarder success total",
	}, []string{"endpoint"})
	forwarderSkipCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "forwarder_skipped_total",
		Help: "Forwarder skipped total",
	}, []string{"endpoint"})
	forwarderFailureCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "forwarder_failure_total",
		Help: "Forwarder failure total",
	}, []string{"endpoint"})
	forwarderLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "forwarder_latency_seconds",
		Help:       "Forwader latency distributions",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"endpoint"})
)

func init() {
	// Init prometheus metrics
	prometheus.MustRegister(forwarderSuccessCounter)
	prometheus.MustRegister(forwarderSkipCounter)
	prometheus.MustRegister(forwarderFailureCounter)
	prometheus.MustRegister(forwarderLatency)
}

// Forwarder type
type Forwarder struct {
	Logger   *log.Logger // Public logger that caller can override
	endpoint string
	messages chan interface{}
}

// NewForwarder creates a new forwarder given endpoint
func NewForwarder(endpoint string) *Forwarder {
	if !strings.HasPrefix(endpoint, "http") {
		log.Fatalf("Expect http(s) endpoint: %q", endpoint)
	}
	return &Forwarder{
		Logger:   log.New(os.Stderr, "", log.LstdFlags),
		endpoint: endpoint,
		messages: make(chan interface{}),
	}
}

// WithLogger initializes with logger
func (f *Forwarder) WithLogger(logger *log.Logger) Destination {
	if logger != nil {
		f.Logger = logger
	}
	return f
}

// Process forwards messages
func (f *Forwarder) Process(ctx context.Context) error {
	log.Println("Started forwarder processing")

	for {
		select {
		case message := <-f.messages:
			t0 := time.Now()
			if err := f.send(ctx, message); err != nil {
				forwarderFailureCounter.WithLabelValues(f.endpoint).Add(float64(1))
				f.Logger.Println(err)
			} else {
				duration := time.Since(t0)
				forwarderSuccessCounter.WithLabelValues(f.endpoint).Add(float64(1))
				forwarderLatency.WithLabelValues(f.endpoint).Observe(duration.Seconds())
				f.Logger.Printf("Forwarded in %s\n", duration)
			}
		case <-ctx.Done():
			f.Logger.Println("Ending forwarder processing")
			return nil
		}
	}
}

// Send pushes messages onto queue
func (f *Forwarder) Send(ctx context.Context, message interface{}) error {
	select {
	case f.messages <- message:
	default:
		forwarderSkipCounter.WithLabelValues(f.endpoint).Add(float64(1))
	}
	return nil
}

func (f *Forwarder) send(ctx context.Context, message interface{}) error {
	m, ok := message.(SegmentEvent)
	if !ok {
		return fmt.Errorf("Expected Segment Event")
	}
	batch := SegmentBatch{
		MessageId: m.MessageId,
		Timestamp: m.Timestamp,
		SentAt:    m.SentAt,
		Context:   m.Context,
		Messages:  []SegmentMessage{m.SegmentMessage},
	}
	b, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	// Create the request for the specific type
	req, err := http.NewRequest("POST", f.endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("error creating request: %s", err)
	}
	req.Header.Add("User-Agent", "brightsparc/segment (version: 1.0)")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(b)))
	req.SetBasicAuth(m.WriteKey, "")

	// Send request
	return httpDo(ctx, req, func(res *http.Response, err error) error {
		if err != nil {
			return fmt.Errorf("Forward error sending request %q -- %v", req.URL.RequestURI(), err)
		}
		defer res.Body.Close()
		if res.StatusCode < 400 {
			return nil
		}
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("Forward error reading response body: %s", err)
		}
		return fmt.Errorf("response %s: %d – %s", res.Status, res.StatusCode, string(body))
	})
}

func httpDo(ctx context.Context, req *http.Request, f func(*http.Response, error) error) error {
	// Run the HTTP request in a goroutine and pass the response to f.
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	c := make(chan error, 1)
	go func() { c <- f(client.Do(req)) }()
	select {
	case <-ctx.Done():
		tr.CancelRequest(req)
		<-c // Wait for f to return.
		return ctx.Err()
	case err := <-c:
		return err
	}
}
