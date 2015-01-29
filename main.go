package main

import (
    "fmt"
    "log"
    "strconv"
    "strings"
    "sync"
    "time"
)

import "github.com/nictuku/dht"
import "github.com/robfig/cron"

const CONCURRENT_GOROUTINES = 1

func main() {
    // Process kickass torrents hourly delta every half hour
    c := cron.New()
    c.AddFunc("@every 30m", ProcessLatestKatEntries)
    c.Start()

    // Also run once at the start
    // ProcessLatestKatEntries()

    runDht()
}

func runDht() {
    geoIpClient := GeoIpClient{}

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
        CONCURRENT_GOROUTINES,
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
                geoIpClient,
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

                ipInfo, err := geoIpClient.GetGeoIpInfo(peerIp, redisClient)

                if err != nil {
                    log.Print(err)
                    continue
                }

                log.Printf(
                    "Found peer: %s (%s, %s, %s)\n",
                    peerIp,
                    ipInfo.Country.Names.En,
                    ipInfo.City.Names.En,
                    ipInfo.Postal.Code,
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
                        "ZINCRBY",
                        fmt.Sprintf("torrent.%s.countries", infoHashStr),
                        1.0,
                        ipInfo.Country.Names.En,
                    )
                    redisClient.Cmd(
                        "ZINCRBY",
                        fmt.Sprintf("torrent.%s.cities", infoHashStr),
                        1.0,
                        fmt.Sprintf(
                            "%s.%s",
                            ipInfo.Country.Names.En,
                            ipInfo.City.Names.En,
                        ),
                    )
                    redisClient.Cmd(
                        "ZINCRBY",
                        fmt.Sprintf("torrent.%s.postcodes", infoHashStr),
                        fmt.Sprintf(
                            "%s.%s",
                            ipInfo.Country.Names.En,
                            ipInfo.Postal.Code,
                        1.0,
                        ),
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
                        "ZINCRBY",
                        "countries",
                        1.0,
                        ipInfo.Country.Names.En,
                    )

                    redisClient.Cmd(
                        "ZINCRBY",
                        "cities",
                        1.0,
                        fmt.Sprintf(
                            "%s.%s",
                            ipInfo.Country.Names.En,
                            ipInfo.City.Names.En,
                        ),
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

