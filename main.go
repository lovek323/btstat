package main

import (
    "fmt"
    "log"
    "strings"
    "sync"
    "time"
)

import "github.com/nictuku/dht"

func main() {
    geoIpClient := GeoIpClient{}

    /* entries := GetLatestKatEntries()

    for _, entry := range entries {
        log.Printf("Entry: %s\n", entry.InfoHash)

        redisClient.Cmd("MULTI")
        redisClient.Cmd("SADD", "torrents", entry.InfoHash)

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
    } */

    dhtClient, err := dht.New(nil)

    if err != nil {
        log.Fatalf("Could not create DHT client: %s\n", err)
    }

    go dhtClient.Run()

    var waitGroup sync.WaitGroup

    A:
    log.Printf("Starting new group\n")
    redisClient := GetRedisClient()
    for i := 0; i < 100; i++ {
        result := redisClient.Cmd("SRANDMEMBER", "torrents")

        if result.Err != nil {
            log.Fatalf("Could not get torrent: %s\n", err)
        }

        infoHashStr := result.String()

        if len(infoHashStr) != 40 {
            continue
        }

        waitGroup.Add(1)
        go func () {
            defer waitGroup.Done()

            runDhtForInfoHash(
                infoHashStr,
                geoIpClient,
                dhtClient,
            )
        }()

        time.Sleep(time.Second)
    }
    waitGroup.Wait()
    goto A
}

func runDhtForInfoHash(
    infoHashStr string,
    geoIpClient GeoIpClient,
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
            log.Printf("Asking for peers: %s\n", infoHashStr)
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
            for _, peer := range peers {
                peerIpAndPort := dht.DecodePeerAddress(peer)
                peerIpAndPortArr := strings.Split(peerIpAndPort, ":")
                peerIp := peerIpAndPortArr[0]
                insights := geoIpClient.GetInsights(peerIp, redisClient)
                log.Printf(
                    "Found peer: %s (%s, %s, %s)\n",
                    peerIp,
                    insights.Country.Names.En,
                    insights.City.Names.En,
                    insights.Postal.Code,
                )

                reply := redisClient.Cmd(
                    "SISMEMBER",
                    fmt.Sprintf("torrent.%s.ips", infoHashStr),
                    peerIp,
                )

                if reply.Err != nil {
                    log.Fatalf(
                        "Failed to check whether IP has been recorded: %s\n",
                        reply.Err,
                    )
                }

                isMember, err := reply.Int()

                if err != nil {
                    log.Fatalf(
                        "Could not get int reply to SISMEMBER: %s\n",
                        err,
                    )
                }

                if isMember == 0 {
                    redisClient.Cmd("MULTI")
                    redisClient.Cmd(
                        "HINCRBY",
                        fmt.Sprintf("torrent.%s.countries", infoHashStr),
                        insights.Country.Names.En,
                        1,
                    )
                    redisClient.Cmd(
                        "HINCRBY",
                        fmt.Sprintf("torrent.%s.cities", infoHashStr),
                        fmt.Sprintf(
                            "%s.%s",
                            insights.Country.Names.En,
                            insights.City.Names.En,
                        ),
                        1,
                    )
                    redisClient.Cmd(
                        "HINCRBY",
                        fmt.Sprintf("torrent.%s.postcodes", infoHashStr),
                        fmt.Sprintf(
                            "%s.%s",
                            insights.Country.Names.En,
                            insights.Postal.Code,
                        ),
                        1,
                    )
                    redisClient.Cmd(
                        "SADD",
                        fmt.Sprintf("torrent.%s.ips", infoHashStr),
                        peerIp,
                    )
                    redisClient.Cmd("EXEC")
                }

                reply = redisClient.Cmd(
                    "SISMEMBER",
                    fmt.Sprintf("global.ips", infoHashStr),
                    peerIp,
                )

                if reply.Err != nil {
                    log.Fatalf(
                        "Failed to check whether IP has been recorded: %s\n",
                        reply.Err,
                    )
                }

                isMember, err = reply.Int()

                if err != nil {
                    log.Fatalf(
                        "Could not get int reply to SISMEMBER: %s\n",
                        err,
                    )
                }

                if isMember == 0 {
                    redisClient.Cmd("MULTI")

                    redisClient.Cmd(
                        "HINCRBY",
                        "countries",
                        insights.Country.Names.En,
                        1,
                    )

                    redisClient.Cmd(
                        "HINCRBY",
                        "cities",
                        fmt.Sprintf(
                            "%s.%s",
                            insights.Country.Names.En,
                            insights.City.Names.En,
                        ),
                        1,
                    )

                    redisClient.Cmd(
                        "SADD",
                        "global.ips",
                        peerIp,
                    )

                    redisClient.Cmd("EXEC")
                }
            }
        }
    }
}

