package main

import "code.google.com/p/getopt"
import "github.com/fzzy/radix/redis"
import "github.com/robfig/cron"

import (
    "fmt"
    "log"
    "os"
    "strings"
)

const CONCURRENT_GOROUTINES      = 4
const TORRENT_MAX_PEER_THRESHOLD = 0.15 // 15% of max peers
const UDP_TIMEOUT                = 1 // Wait 1 second to send/receive UDP data

func main() {
    flush := getopt.Bool(
        'f',
        "Flush redis stats (leave torrents and IP data intact)",
    )

    get := getopt.Bool(
        'g',
        "Get torrents from sources before processing (use on first run)",
    )

    getopt.CommandLine.Parse(os.Args)

    if *flush {
        flushRedis()
        return
    }

    if *get {
        ProcessLatestKatEntries()
    }

    // Process kickass torrents hourly delta every half hour
    c := cron.New()
    c.AddFunc("@every 30m", ProcessLatestKatEntries)
    c.Start()

    // runDht()

    runTracker()
}

func updateInfoHashScore(
    infoHashStr string,
    infoHashScore float64,
    redisClient *redis.Client,
) {
    if infoHashScore < 0.005 {

        redisClient.Cmd(
            "ZREM",
            "torrents",
            infoHashStr,
        )

        return
    }

    redisClient.Cmd(
        "ZADD",
        "torrents",
        infoHashScore,
        infoHashStr,
    )
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

