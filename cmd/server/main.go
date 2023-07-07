package main

import (
	"log"

	"github.com/ltbringer/proglog/internal/server"
)

func main() {
	s := server.NewHTTPServer(":8080")
	log.Fatal(s.ListenAndServe())
}
