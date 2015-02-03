package main

import (
    "log"
    "strconv"
    "strings"
    "sync"
    "time"
)

import "github.com/nictuku/dht"

func runDht() {
    dhtClient, err := dht.New(nil)

    if err != nil {
        log.Fatalf("Could not create DHT client: %s\n", err)
    }

    go dhtClient.Run()

    var waitGroup sync.WaitGroup

    redisClient := GetRedisClient()

    A:
    result := redisClient.Cmd(
        "ZREVRANGE",
        "torrents",
        0,
        CONCURRENT_GOROUTINES - 1,
        "WITHSCORES",
    )

    if result.Err != nil {
        log.Fatalf("Could not get torrents: %s\n", result.Err)
    }

    infoHashStrings, err := result.List()

    if err != nil {
        log.Fatalf("Could not get torrents: %s\n", err)
    }

    infoHashStr := ""
    infoHashScore := 0.0

    for index, value := range infoHashStrings {
        if index % 2 == 0 {
            infoHashStr = value
            continue
        }
        infoHashScore, err = strconv.ParseFloat(value, 64)
        if err != nil {
            log.Fatalf("Could not parse float %s: %s\n", value, err)
        }
        waitGroup.Add(1)
        go func (infoHashStr string, infoHashScore float64) {
            defer waitGroup.Done()

            runDhtForInfoHash(
                infoHashStr,
                infoHashScore,
                dhtClient,
            )
        }(infoHashStr, infoHashScore)
    }

    time.Sleep(time.Second)
    waitGroup.Wait()
    goto A
}

func runDhtForInfoHash(
    infoHashStr string,
    infoHashScore float64,
    dhtClient *dht.DHT,
) {
    infoHash, err := dht.DecodeInfoHash(infoHashStr)

    if err != nil {
        log.Fatalf("Could not decode info hash %s\n", err)
    }

    tick := time.Tick(time.Second)

    var infoHashPeers map[dht.InfoHash][]string

    M:
    for {
        select {
        case <-tick:
            // Repeat the request until a result appears, querying nodes that
            // haven't been consulted before and finding close-by candidates for
            // the infohash.
            dhtClient.PeersRequest(string(infoHash), false)
        case infoHashPeers = <-dhtClient.PeersRequestResults:
            break M
        case <-time.After(30 * time.Second):
            log.Printf("Could not find new peers: timed out\n")
            return
        }
    }

    redisClient := GetRedisClient()

    for _, peers := range infoHashPeers {
        if len(peers) > 0 {
            updateInfoHashScore(infoHashStr, infoHashScore * 0.8, redisClient)

            for _, peer := range peers {
                peerIpAndPort := dht.DecodePeerAddress(peer)
                peerIpAndPortArr := strings.Split(peerIpAndPort, ":")
                peerIp := peerIpAndPortArr[0]

                processPeerIp(peerIp, infoHashStr, redisClient)
            }
        } else {
            updateInfoHashScore(infoHashStr, infoHashScore * 0.5, redisClient)
        }
    }
}

