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
	InfoHash string
	Title    string
	Category string
	Uri      string
}

const KAT_HOURLY_URL = "https://kickass.so/hourlydump.txt.gz"

func GetLatestKatEntries() []KatEntry {
	response, err := http.Get(KAT_HOURLY_URL)
	if err != nil {
		log.Fatalf("Failed to download KAT entries: %s\n", err)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		log.Fatalf("Failed to read KAT entries body: %s\n", err)
	}
	scanner := bufio.NewScanner(reader)
	entries := make([]KatEntry, 1)
	config := GetConfig()
	for scanner.Scan() {
		values := strings.Split(scanner.Text(), "|")
		for _, category := range config.Kat.Categories {
			if category == values[2] {
				entries = append(
					entries,
					KatEntry{
						InfoHash: values[0],
						Title:    values[1],
						Category: values[2],
						Uri:      values[3],
					},
				)
				continue
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
		log.Printf("Entry: %s\n", entry.InfoHash)
		redisClient.Cmd("MULTI")
		redisClient.Cmd("ZADD", "torrents", 1.0, entry.InfoHash)
		redisClient.Cmd(
			"HSET",
			fmt.Sprintf("torrents.info.%s", entry.InfoHash),
			"title",
			entry.Title,
		)
		redisClient.Cmd(
			"HSET",
			fmt.Sprintf("torrents.info.%s", entry.InfoHash),
			"category",
			entry.Category,
		)
		redisClient.Cmd(
			"HSET",
			fmt.Sprintf("torrents.info.%s", entry.InfoHash),
			"uri",
			entry.Uri,
		)
		redisClient.Cmd("EXEC")
	}
	torrentCount := RedisIntCmd(
		redisClient,
		"ZCOUNT",
		"torrents",
		"-inf",
		"+inf",
	)
	redisClient.Cmd("SET", "torrents.count", torrentCount)
	stathat.PostEZValue(
		"torrents.count",
		"lovek323@gmail.com",
		float64(torrentCount),
	)
}
