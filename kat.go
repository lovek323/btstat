package main

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "log"
    "net/http"
    "strings"
)

type KatEntry struct {
    InfoHash string
    Title string
    Category string
    Uri string
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

    /* entries, err := ioutil.ReadAll(reader)

    if err != nil {
        log.Fatalf("Failed to read KAT entries body: %s\n", err)
    } */

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
                        Title: values[1],
                        Category: values[2],
                        Uri: values[3],
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

    redisClient := GetRedisClient()

    for _, entry := range entries {
        log.Printf("Entry: %s\n", entry.InfoHash)

        /**
         * SCORING
         *
         * All torrents will start with a score of 1.0. If peers are found when
         * a torrent is processed, the score will be reduced by 20% (i.e.,
         * multiplied by 0.8), otherwise the score will be reduced by 50%. Once
         * the highest score is less than 0.005, the score will be incremented
         * by 1.0 to avoid eventually running out of precision.
         */

        redisClient.Cmd("MULTI")
        redisClient.Cmd("ZADD", "torrents", 1.0, entry.InfoHash)

        redisClient.Cmd(
            "HSET",
            fmt.Sprintf("torrent.%s", entry.InfoHash),
            "title",
            entry.Title,
        )

        redisClient.Cmd(
            "HSET",
            fmt.Sprintf("torrent.%s", entry.InfoHash),
            "category",
            entry.Category,
        )

        redisClient.Cmd(
            "HSET",
            fmt.Sprintf("torrent.%s", entry.InfoHash),
            "uri",
            entry.Uri,
        )

        redisClient.Cmd("EXEC")
    }
}

