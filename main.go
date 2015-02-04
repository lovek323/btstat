package main

import "code.google.com/p/getopt"

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const CONCURRENT_GOROUTINES = 60        // Run 20 torrents at a time
const TORRENT_MAX_PEER_THRESHOLD = 0.15 // 15% of max peers
const UDP_TIMEOUT = 1                   // Wait 1 second to send/receive UDP data

var app *App = NewApp()

func main() {
	if !parseArgs() {
		return
	}
	runCron()
	app.RunUDPQueryer()
}

func parseArgs() bool {
	flush := getopt.Bool(
		'f',
		"Flush redis stats (leave torrents intact)",
	)
	get := getopt.Bool(
		'g',
		"Get torrents from sources before processing (use on first run)",
	)
	getopt.CommandLine.Parse(os.Args)
	if *flush {
		flushRedis()
		return false
	}
	if *get {
		ProcessLatestKatEntries()
	}
	return true
}

func flushRedis() {
	log.Printf("Flushing redis keys\n")
	redisClient := GetRedisClient()
	result := redisClient.Cmd("KEYS", "*")
	if result.Err != nil {
		log.Fatalf("Could not get torrents: %s\n", result.Err)
	}
	infoHashStrings, err := result.List()
	if err != nil {
		log.Fatalf("Could not get torrents: %s\n", err)
	}
	for index, value := range infoHashStrings {
		if value == "torrents" ||
			strings.HasPrefix(value, "torrents.info.") ||
			strings.HasPrefix(value, "ips.") {
			continue
		}
		redisClient.Cmd("DEL", value)
		fmt.Printf("Deleted %s (%d)\n", value, index)
	}
}
