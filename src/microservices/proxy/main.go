package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

var config Config

func main() {
	rand.Seed(time.Now().UnixNano())

	loadConfig()

	log.Printf("Proxy starting on port %s", config.Port)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Events Service URL: %s", config.EventsServiceURL)
	log.Printf("Gradual Migration: %v", config.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", config.MoviesMigrationPercent)

	http.HandleFunc("/", proxyHandler)

	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func loadConfig() {
	config.Port = getEnv("PORT", "8000")
	config.MonolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	config.MoviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	config.EventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	config.GradualMigration = getEnv("GRADUAL_MIGRATION", "false") == "true"

	percentStr := getEnv("MOVIES_MIGRATION_PERCENT", "100")
	percent, err := strconv.Atoi(percentStr)
	if err != nil {
		percent = 100
	}
	config.MoviesMigrationPercent = percent
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	var targetURL string

	switch {
	case strings.HasPrefix(path, "/api/movies"):
		targetURL = chooseMoviesTarget()
		log.Printf("[PROXY] %s %s -> %s (movies)", r.Method, path, targetURL)

	case strings.HasPrefix(path, "/api/events"):
		targetURL = config.EventsServiceURL
		log.Printf("[PROXY] %s %s -> %s (events)", r.Method, path, targetURL)

	default:
		targetURL = config.MonolithURL
		log.Printf("[PROXY] %s %s -> %s (monolith)", r.Method, path, targetURL)
	}

	forwardRequest(targetURL, w, r)
}

func chooseMoviesTarget() string {

	if config.GradualMigration {
		randomNum := rand.Intn(100)
		if randomNum < config.MoviesMigrationPercent {
			log.Printf("[MIGRATION] Random %d < %d%% -> Movies Service", randomNum, config.MoviesMigrationPercent)
			return config.MoviesServiceURL
		}
		log.Printf("[MIGRATION] Random %d >= %d%% -> Monolith", randomNum, config.MoviesMigrationPercent)
		return config.MonolithURL
	}

	return config.MoviesServiceURL
}

func forwardRequest(targetURL string, w http.ResponseWriter, r *http.Request) {
	
	target, err := url.Parse(targetURL)
	if err != nil {
		log.Printf("[ERROR] Failed to parse target URL: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = target.Host

	proxy.ServeHTTP(w, r)
}
