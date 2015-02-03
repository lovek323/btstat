package main

import (
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "errors"
    "fmt"
    "log"
    "math/rand"
    "net"
    "net/url"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

import "github.com/fzzy/radix/redis"
import "github.com/nictuku/nettools"
import "github.com/stathat/go"

const ACTION_CONNECT  = 0
const ACTION_ANNOUNCE = 1
const ACTION_SCRAPE   = 2
const ACTION_ERROR    = 3

const EVENT_NONE = 0
const EVENT_COMPLETED = 1
const EVENT_STARTED = 2
const EVENT_STOPPED = 3

type ClientStatusReport struct {
    Event      string
    InfoHash   string
    PeerId     string
    Port       uint16
    Uploaded   uint64
    Downloaded uint64
    Left       uint64
}

type TrackerResponse struct {
    FailureReason  string `bencode:"failure reason"`
    WarningMessage string `bencode:"warning message"`
    Interval       uint
    MinInterval    uint   `bencode:"min interval"`
    TrackerId      string `bencode:"tracker id"`
    Complete       uint
    Incomplete     uint
    Peers          string
    Peers6         string
}

func queryTracker(
    infoHashStr string,
    trackerUrlStr string,
) (*TrackerResponse, error) {
    trackerUrl, err := url.Parse(trackerUrlStr)
    if err != nil {
        log.Println("Error: Invalid announce URL(", trackerUrlStr, "):", err)
        return nil, err
    }
    switch trackerUrl.Scheme {
    // case "http":
        // fallthrough
    // case "https":
        // return queryHTTPTracker(report, trackerUrl)
    case "udp":
        return scrapeUdpTracker(infoHashStr, trackerUrl)
    default:
        errorMessage := fmt.Sprintf(
            "Unknown scheme %v in %v",
            trackerUrl.Scheme,
            trackerUrlStr,
        )
        log.Println(errorMessage)
        return nil, errors.New(errorMessage)
    }
}

/* func queryHTTPTracker(report ClientStatusReport, u *url.URL) (tr *TrackerResponse, err error) {
    uq := u.Query()
    uq.Add("info_hash", report.InfoHash)
    uq.Add("peer_id", report.PeerId)
    uq.Add("port", strconv.FormatUint(uint64(report.Port), 10))
    uq.Add("uploaded", strconv.FormatUint(report.Uploaded, 10))
    uq.Add("downloaded", strconv.FormatUint(report.Downloaded, 10))
    uq.Add("left", strconv.FormatUint(report.Left, 10))
    uq.Add("compact", "1")

    // Don't report IPv6 address, the user might prefer to keep
    // that information private when communicating with IPv4 hosts.
    if false {
        ipv6Address, err := findLocalIPV6AddressFor(u.Host)
        if err == nil {
            log.Println("our ipv6", ipv6Address)
            uq.Add("ipv6", ipv6Address)
        }
    }

    if report.Event != "" {
        uq.Add("event", report.Event)
    }

    // This might reorder the existing query string in the Announce url
    // This might break some broken trackers that don't parse URLs properly.

    u.RawQuery = uq.Encode()

    tr, err = getTrackerInfo(dialer, u.String())
    if tr == nil || err != nil {
        log.Println("Error: Could not fetch tracker info:", err)
    } else if tr.FailureReason != "" {
        log.Println("Error: Tracker returned failure reason:", tr.FailureReason)
        err = fmt.Errorf("tracker failure %s", tr.FailureReason)
    }
    return
}

func findLocalIPV6AddressFor(hostAddr string) (local string, err error) {
    // Figure out our IPv6 address to talk to a given host.
    host, hostPort, err := net.SplitHostPort(hostAddr)
    if err != nil {
        host = hostAddr
        hostPort = "1234"
    }
    dummyAddr := net.JoinHostPort(host, hostPort)
    log.Println("Looking for host ", dummyAddr)
    conn, err := net.Dial("udp6", dummyAddr)
    if err != nil {
        log.Println("No IPV6 for host ", host, err)
        return "", err
    }
    defer conn.Close()
    localAddr := conn.LocalAddr()
    local, _, err = net.SplitHostPort(localAddr.String())
    if err != nil {
        local = localAddr.String()
    }
    return
} */

func scrapeUdpTracker(
    infoHashStr string,
    u *url.URL,
) (*TrackerResponse, error) {
    serverAddr, err := net.ResolveUDPAddr("udp", u.Host)
    if err != nil {
        return nil, err
    }
    conn, err := net.DialUDP("udp", nil, serverAddr)
    if err != nil {
        return nil, err
    }
    defer func() { conn.Close() }()

    var connectionID uint64
    err = conn.SetDeadline(time.Now().Add(UDP_TIMEOUT * time.Second))
    if err != nil {
        return nil, err
    }

    connectionID, err = connectToUDPTracker(conn)
    if err != nil {
        return nil, err
    }
    if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
        return nil, nerr
    }
    if err != nil {
        return nil, err
    }

    return getScrapeResponseFromUdpTracker(conn, connectionID, infoHashStr)
}

func connectToUDPTracker(conn *net.UDPConn) (connectionID uint64, err error) {
    var connectionRequest_connectionID uint64 = 0x41727101980
    var action uint32 = 0
    transactionID := rand.Uint32()

    connectionRequest := new(bytes.Buffer)
    err = binary.Write(connectionRequest, binary.BigEndian, connectionRequest_connectionID)
    if err != nil {
        return
    }
    err = binary.Write(connectionRequest, binary.BigEndian, action)
    if err != nil {
        return
    }
    err = binary.Write(connectionRequest, binary.BigEndian, transactionID)
    if err != nil {
        return
    }

    _, err = conn.Write(connectionRequest.Bytes())
    if err != nil {
        return
    }

    connectionResponseBytes := make([]byte, 16)

    var connectionResponseLen int
    connectionResponseLen, err = conn.Read(connectionResponseBytes)
    if err != nil {
        return
    }
    if connectionResponseLen != 16 {
        err = fmt.Errorf("Unexpected response size %d", connectionResponseLen)
        return
    }
    connectionResponse := bytes.NewBuffer(connectionResponseBytes)
    var connectionResponseAction uint32
    err = binary.Read(connectionResponse, binary.BigEndian, &connectionResponseAction)
    if err != nil {
        return
    }
    if connectionResponseAction != ACTION_CONNECT {
        err = fmt.Errorf("Unexpected response action %d", connectionResponseAction)
        return
    }
    var connectionResponseTransactionID uint32
    err = binary.Read(connectionResponse, binary.BigEndian, &connectionResponseTransactionID)
    if err != nil {
        return
    }
    if connectionResponseTransactionID != transactionID {
        err = fmt.Errorf("Unexpected response transactionID %x != %x",
            connectionResponseTransactionID, transactionID)
        return
    }

    err = binary.Read(connectionResponse, binary.BigEndian, &connectionID)
    if err != nil {
        return
    }
    return
}

func getScrapeResponseFromUdpTracker(
    conn *net.UDPConn,
    connectionID uint64,
    infoHashStr string,
) (*TrackerResponse, error) {
    scrapeRequest := new(bytes.Buffer)

    // Connection ID
    err := binary.Write(scrapeRequest, binary.BigEndian, connectionID)
    if err != nil { return nil, err }

    // Action
    var action uint32 = ACTION_ANNOUNCE
    err = binary.Write(scrapeRequest, binary.BigEndian, action)
    if err != nil { return nil, err }

    // Transaction ID
    transactionID := rand.Uint32()
    err = binary.Write(scrapeRequest, binary.BigEndian, transactionID)
    if err != nil { return nil, err }

    // Info hash
    infoHashBytes, _ := hex.DecodeString(infoHashStr)
    err = binary.Write(scrapeRequest, binary.BigEndian, infoHashBytes)
    if err != nil { return nil, err }

    // Peer ID
    peerId := "-tt" + strconv.Itoa(os.Getpid()) + "_" +
        strconv.FormatInt(rand.Int63(), 10)
    peerId = peerId[0:20]
    err = binary.Write(scrapeRequest, binary.BigEndian, []byte(peerId))
    if err != nil { return nil, err }

    // Downloaded
    var downloaded int64 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, downloaded)
    if err != nil { return nil, err }

    // Left
    var left int64 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, left)
    if err != nil { return nil, err }

    // Uploaded
    var uploaded int64 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, uploaded)
    if err != nil { return nil, err }

    // Event
    var event int32 = EVENT_NONE
    err = binary.Write(scrapeRequest, binary.BigEndian, event)
    if err != nil { return nil, err }

    // IP address
    var ipAddress uint32 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, ipAddress)
    if err != nil { return nil, err }

    // Key
    var key int32 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, key)
    if err != nil { return nil, err }

    // Num want
    var numWant int32 = 200
    err = binary.Write(scrapeRequest, binary.BigEndian, numWant)
    if err != nil { return nil, err }

    // Port
    var port int16 = 0
    err = binary.Write(scrapeRequest, binary.BigEndian, port)
    if err != nil { return nil, err }

    _, err = conn.Write(scrapeRequest.Bytes())
    if err != nil { return nil, err }

    responseBytes := make([]byte, 20 + 200 * 6)
    responseLen, err := conn.Read(responseBytes)
    if err != nil { return nil, err }

    if responseLen < 16 {
        return nil, fmt.Errorf("Unexpected response size %d", responseLen)
    }

    response := bytes.NewBuffer(responseBytes)

    // Action
    var responseAction uint32
    err = binary.Read(response, binary.BigEndian, &responseAction)
    if err != nil { return nil, err }
    if action != ACTION_ANNOUNCE {
        return nil, fmt.Errorf("Unexpected response action %d", action)
    }

    // Transaction ID
    var responseTransactionID uint32
    err = binary.Read(response, binary.BigEndian, &responseTransactionID)
    if err != nil { return nil, err }
    if transactionID != responseTransactionID {
        return nil, fmt.Errorf(
            "Unexpected response transactionID %x",
            responseTransactionID,
        )
    }

    // Interval
    var interval uint32
    err = binary.Read(response, binary.BigEndian, &interval)
    if err != nil { return nil, err }

    // Leechers
    var leechers uint32
    err = binary.Read(response, binary.BigEndian, &leechers)
    if err != nil { return nil, err }

    // Seeders
    var seeders uint32
    err = binary.Read(response, binary.BigEndian, &seeders)
    if err != nil { return nil, err }

    /* fmt.Printf(
        "Action: %d, Transaction ID: %d, Interval: %d, Leechers: %d, Seeders: %d\n",
        responseAction,
        responseTransactionID,
        interval,
        leechers,
        seeders,
    ) */

    peerCount := (responseLen - 20) / 6
    peerDataBytes := make([]byte, 6 * peerCount)
    err = binary.Read(response, binary.BigEndian, &peerDataBytes)
    if err != nil { return nil, err }

    return &TrackerResponse{
        Interval:   uint(interval),
        Complete:   uint(seeders),
        Incomplete: uint(leechers),
        Peers:      string(peerDataBytes),
    }, nil
}

func runTracker() {
    geoIpClient := GeoIpClient{}

    var waitGroup sync.WaitGroup

    redisClient := GetRedisClient()
    B:
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
            redisClient := GetRedisClient()

            defer redisClient.Close()
            defer waitGroup.Done()

            trackerUrlStrs := map[string]int{
                "udp://12.rarbg.me:80/announce": 200,
                "udp://9.rarbg.com:2710/announce": 50,
                "udp://open.demonii.com:1337/announce": 200,
                "udp://tracker.coppersurfer.tk:6969/announce": 200,
                "udp://tracker.leechers-paradise.org:6969/announce": 200,
                "udp://tracker.token.ro:80/announce": 200,

                // Not working:
                // "udp://tracker.openbittorrent.com:80": 200,
                // "udp://tracker.publicbt.com:80": 200,
                // "udp://tracker.istole.it:80": 200,
            }

            // If a list of trackers doesn't exist (or it is empty) for
            // this torrent, add each of the trackers to the list. These
            // will be removed if the tracker returns out of the threshold.
            setTrackerUrlStrs := RedisStrsCmd(
                redisClient,
                "SMEMBERS",
                fmt.Sprintf("torrents.%s.trackers", infoHashStr),
            )

            var currentTrackerUrlStrs = make(
                map[string]int,
                len(trackerUrlStrs),
            )

            if len(setTrackerUrlStrs) == 0 {
                for trackerUrlStr, _ := range trackerUrlStrs {
                    redisClient.Cmd(
                        "SADD",
                        fmt.Sprintf("torrents.%s.trackers", infoHashStr),
                        trackerUrlStr,
                    )

                    currentTrackerUrlStrs[trackerUrlStr] = trackerUrlStrs[trackerUrlStr]
                }

                currentTrackerUrlStrs = trackerUrlStrs
            } else {
                for _, trackerUrlStr := range setTrackerUrlStrs {
                    currentTrackerUrlStrs[trackerUrlStr] = trackerUrlStrs[trackerUrlStr]
                }
            }

            for trackerUrlStr, maxPeerCount := range currentTrackerUrlStrs {
                runTrackerForInfoHash(
                    infoHashStr,
                    infoHashScore,
                    geoIpClient,
                    trackerUrlStr,
                    maxPeerCount,
                    redisClient,
                )
            }

            if RedisCmd(
                redisClient,
                "GET",
                fmt.Sprintf("torrents.%s.processed", infoHashStr),
            ).Type == redis.NilReply {
                // log.Printf("Marking torrent as processed: %s\n", infoHashStr)

                RedisCmd(
                    redisClient,
                    "SETEX",
                    fmt.Sprintf("torrents.%s.processed", infoHashStr),
                    strconv.FormatInt(int64(time.Hour.Seconds()), 10),
                    time.Now().Format("2006-01-02 15:04:05"),
                )

                // This torrent has already been recorded as being processed
                // within the last hour.
                stathat.PostEZCount(
                    "torrents.processed",
                    "lovek323@gmail.com",
                    1,
                )
            }
        }(infoHashStr, infoHashScore)
    }

    time.Sleep(time.Second)
    waitGroup.Wait()
    goto B
}

func runTrackerForInfoHash(
    infoHashStr string,
    infoHashScore float64,
    geoIpClient GeoIpClient,
    trackerUrlStr string,
    trackerMaxPeerCount int,
    redisClient *redis.Client,
) {
    response, err := queryTracker(infoHashStr, trackerUrlStr)

    if err != nil {
        // No need to log this since we record it on StatHat:
        // log.Printf("Could not query UDP tracker '%s': %s\n", trackerUrlStr, err)
        stathat.PostEZCount("errors.query-udp-tracker", "lovek323@gmail.com", 1)
        RedisCmd(
            redisClient,
            "SREM",
            fmt.Sprintf("torrents.%s.trackers", infoHashStr),
            trackerUrlStr,
        )
        updateInfoHashScore(infoHashStr, infoHashScore * 0.5, redisClient)
        return
    }

    peers := response.Peers

    if len(peers) > 0 {
        const peerLen = 6

        newPeers := 0

        for i := 0; i < len(peers); i += peerLen {
            ipPortStr := nettools.BinaryToDottedPort(peers[i : i+peerLen])
            ipPortArr := strings.Split(ipPortStr, ":")
            ipStr := ipPortArr[0]

            if processPeerIp(ipStr, infoHashStr, redisClient) {
                newPeers++
            }
        }

        var newInfoHashScore float64

        peerThreshold := int(
            float64(trackerMaxPeerCount) * float64(TORRENT_MAX_PEER_THRESHOLD),
        )

        peerCount := len(peers) / peerLen
        hasMaxPeers := peerCount >= trackerMaxPeerCount
        hasEnoughNewPeers := newPeers > peerThreshold

        if hasMaxPeers && hasEnoughNewPeers {
            newInfoHashScore = infoHashScore * 1.2
        } else {
            RedisCmd(
                redisClient,
                "SREM",
                fmt.Sprintf("torrents.%s.trackers", infoHashStr),
                trackerUrlStr,
            )

            newInfoHashScore = infoHashScore * 0.8
        }

        updateInfoHashScore(infoHashStr, newInfoHashScore, redisClient)

        if (newPeers > 0) {
            log.Printf(
                "P: %03d (%03d) H: %s S: %e (%e) T: %s\n",
                peerCount,
                newPeers,
                infoHashStr,
                infoHashScore,
                newInfoHashScore,
                trackerUrlStr,
            )
        }
    } else {
        RedisCmd(
            redisClient,
            "SREM",
            fmt.Sprintf("torrents.%s.trackers", infoHashStr),
            trackerUrlStr,
        )

        updateInfoHashScore(infoHashStr, infoHashScore * 0.5, redisClient)
    }
}

