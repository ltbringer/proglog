package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type LogStore struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset Offset `json:"offset"`
}

type ConsumeRequest struct {
	Offset Offset `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

// Create a new LogStore.
func newLogStore() *LogStore {
	return &LogStore{
		Log: NewLog(),
	}
}

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

func (ls *LogStore) handleConsume(w http.ResponseWriter, r *http.Request) {

}

func NewHTTPServer(uri string) *http.Server {
	logStore := newLogStore()
	r := mux.NewRouter()
	r.HandleFunc("/", logStore.handleProduce).Methods("POST")
	r.HandleFunc("/", logStore.handleConsume).Methods("GET")
}
