package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// LogStore represents a store for logs.
type LogStore struct {
	Log *Log
}

// ProduceRequest represents a request to produce a new record.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse represents a response after producing a record.
type ProduceResponse struct {
	Offset Offset `json:"offset"`
}

// ConsumeRequest represents a request to consume a record.
type ConsumeRequest struct {
	Offset Offset `json:"offset"`
}

// ConsumeResponse represents a response after consuming a record.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// newLogStore creates a new LogStore instance.
func newLogStore() *LogStore {
	return &LogStore{
		Log: NewLog(),
	}
}

// handleProduce handles the HTTP POST request for producing a new record to the LogStore.
// It reads the JSON payload from the request body, decodes it into a `ProduceRequest` struct,
// appends the record to the LogStore's Log, retrieves the offset where it was appended,
// and constructs a ProduceResponse containing the offset of the appended record and encodes it as JSON in the response body.
//
// It returns an error if decoding fails (Bad Request),
// if the append operation encounters an error (Internal Server Error),
// or if encoding the response fails (Internal Server Error).
func (ls *LogStore) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := ls.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{
		Offset: offset,
	}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleConsume handles the HTTP GET request for consuming a record from the LogStore.
// It reads the offset from the request parameters, retrieves the corresponding record from the LogStore's Log,
// and constructs a ConsumeResponse containing the retrieved record.
// Errors if the offset is not found (Not Found).
func (ls *LogStore) handleConsume(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement handleConsume logic
}

// NewHTTPServer creates a new HTTP server with the specified URI and returns it.
func NewHTTPServer(uri string) *http.Server {
	logStore := newLogStore()
	r := mux.NewRouter()
	r.HandleFunc("/", logStore.handleProduce).Methods("POST")
	r.HandleFunc("/", logStore.handleConsume).Methods("GET")
}
