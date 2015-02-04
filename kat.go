package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"net/http"
	"strings"
)

import "github.com/stathat/go"

type KatEntry struct {
	Infohash *Infohash
	Title    string
	Category string
	Uri      string
}

const KAT_HOURLY_URL = "https://kickass.so/hourlydump.txt.gz"

func GetLatestKatEntries() []KatEntry {
	trackerUrlStrs := map[string]int{
		"udp://12.rarbg.me:80/announce":               200,
		"udp://9.rarbg.com:2710/announce":             50,
		"udp://open.demonii.com:1337/announce":        200,
		"udp://tracker.coppersurfer.tk:6969/announce": 200,
	}
	response, err := http.Get(KAT_HOURLY_URL)
	if err != nil {
		app.Debugf("GetLatestKatEntries()", "Failed to download KAT entries: %s", err)
		return []KatEntry{}
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		log.Fatalf("Failed to read KAT entries body: %s\n", err)
	}
	scanner := bufio.NewScanner(reader)
	entries := make([]KatEntry, 0)
	config := GetConfig()
	for scanner.Scan() {
		values := strings.Split(scanner.Text(), "|")
		for _, category := range config.Kat.Categories {
			if category == values[2] {
				for trackerUrlStr, trackerMaxPeerCount := range trackerUrlStrs {
					tracker, err := NewTracker(trackerUrlStr, trackerMaxPeerCount)
					if err != nil {
						panic(err)
					}
					entries = append(
						entries,
						KatEntry{
							Infohash: NewInfohash(values[0], 1, tracker),
							Title:    values[1],
							Category: values[2],
							Uri:      values[3],
						},
					)
				}
				break
			}
		}
	}
	return entries
}

func ProcessLatestKatEntries() {
	entries := GetLatestKatEntries()
	redisClient, err := app.GetRedisClient()
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		app.Debugf("ProcessLatestKatEntries()", "Entry: %v", entry.Infohash)
		entry.Infohash.UpdateScore(1, redisClient)
		RedisCmd(redisClient, "HSET", fmt.Sprintf("torrents.info.%s", entry.Infohash.String()), "title", entry.Title)
		RedisCmd(
			redisClient,
			"HSET",
			fmt.Sprintf("torrents.info.%s", entry.Infohash.String()),
			"category",
			entry.Category,
		)
		RedisCmd(redisClient, "HSET", fmt.Sprintf("torrents.info.%s", entry.Infohash.String()), "uri", entry.Uri)
	}
	torrentCount := RedisIntCmd(
		redisClient,
		"ZCOUNT",
		"torrents",
		"-inf",
		"+inf",
	)
	redisClient.Cmd("SET", "torrents.count", torrentCount)
	go stathat.PostEZValue("torrents.count", "lovek323@gmail.com", float64(torrentCount))
}
