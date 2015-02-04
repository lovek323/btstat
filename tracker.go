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
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/stathat/go"

const ACTION_CONNECT = 0
const ACTION_ANNOUNCE = 1
const ACTION_SCRAPE = 2
const ACTION_ERROR = 3

const EVENT_NONE = 0
const EVENT_COMPLETED = 1
const EVENT_STARTED = 2
const EVENT_STOPPED = 3

type tracker struct {
	url          *url.URL
	conn         *net.UDPConn
	connID       uint64
	maxPeerCount int
}

type trackerResponse struct {
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

func NewTracker(urlStr string, maxPeerCount int) (*tracker, error) {
	t := new(tracker)
	url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	t.url = url
	t.maxPeerCount = maxPeerCount
	return t, nil
}

func (t *tracker) query(infoHashStr string) (*trackerResponse, error) {
	switch t.url.Scheme {
	case "udp":
		return t.scrapeUDP(infoHashStr)
	default:
		errorMessage := fmt.Sprintf(
			"Unknown scheme %v in %v",
			t.url.Scheme,
			t.url.String(),
		)
		// log.Println(errorMessage)
		return nil, errors.New(errorMessage)
	}
}

func (t *tracker) scrapeUDP(infoHashStr string) (*trackerResponse, error) {
	serverAddr, err := net.ResolveUDPAddr("udp", t.url.Host)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}
	t.conn = conn
	defer func() { t.conn.Close() }()

	err = t.conn.SetDeadline(time.Now().Add(UDP_TIMEOUT * time.Second))
	if err != nil {
		return nil, err
	}

	err = t.connectUDP()
	if err != nil {
		return nil, err
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return nil, nerr
	}
	if err != nil {
		return nil, err
	}

	return t.getUDPScrapeResponse(infoHashStr)
}

func (t *tracker) connectUDP() error {
	var connReq_connID uint64 = 0x41727101980
	var action uint32 = 0
	transactionID := rand.Uint32()

	connReq := new(bytes.Buffer)
	err := binary.Write(connReq, binary.BigEndian, connReq_connID)
	if err != nil {
		return err
	}
	err = binary.Write(connReq, binary.BigEndian, action)
	if err != nil {
		return err
	}
	err = binary.Write(connReq, binary.BigEndian, transactionID)
	if err != nil {
		return err
	}
	_, err = t.conn.Write(connReq.Bytes())
	if err != nil {
		return err
	}

	connectionResponseBytes := make([]byte, 16)
	var connRespLen int
	connRespLen, err = t.conn.Read(connectionResponseBytes)
	if err != nil {
		return err
	}
	if connRespLen != 16 {
		err = fmt.Errorf("Unexpected response size %d", connRespLen)
		return err
	}
	connResp := bytes.NewBuffer(connectionResponseBytes)
	var connRespAction uint32
	err = binary.Read(connResp, binary.BigEndian, &connRespAction)
	if err != nil {
		return err
	}
	if connRespAction != ACTION_CONNECT {
		err = fmt.Errorf("Unexpected response action %d", connRespAction)
		return err
	}
	var connRespTransID uint32
	err = binary.Read(connResp, binary.BigEndian, &connRespTransID)
	if err != nil {
		return err
	}
	if connRespTransID != transactionID {
		return fmt.Errorf(
			"Unexpected response transactionID %x != %x",
			connRespTransID,
			transactionID,
		)
	}

	err = binary.Read(connResp, binary.BigEndian, &t.connID)
	if err != nil {
		return err
	}
	return err
}

func (t *tracker) getUDPScrapeResponse(infoHashStr string) (
	*trackerResponse,
	error,
) {
	scrapeRequest := new(bytes.Buffer)

	// Connection ID
	err := binary.Write(scrapeRequest, binary.BigEndian, t.connID)
	if err != nil {
		return nil, err
	}

	// Action
	var action uint32 = ACTION_ANNOUNCE
	err = binary.Write(scrapeRequest, binary.BigEndian, action)
	if err != nil {
		return nil, err
	}

	// Transaction ID
	transactionID := rand.Uint32()
	err = binary.Write(scrapeRequest, binary.BigEndian, transactionID)
	if err != nil {
		return nil, err
	}

	// Info hash
	infoHashBytes, _ := hex.DecodeString(infoHashStr)
	err = binary.Write(scrapeRequest, binary.BigEndian, infoHashBytes)
	if err != nil {
		return nil, err
	}

	// Peer ID
	peerId := "-tt" + strconv.Itoa(os.Getpid()) + "_" +
		strconv.FormatInt(rand.Int63(), 10)
	peerId = peerId[0:20]
	err = binary.Write(scrapeRequest, binary.BigEndian, []byte(peerId))
	if err != nil {
		return nil, err
	}

	// Downloaded
	var downloaded int64 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, downloaded)
	if err != nil {
		return nil, err
	}

	// Left
	var left int64 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, left)
	if err != nil {
		return nil, err
	}

	// Uploaded
	var uploaded int64 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, uploaded)
	if err != nil {
		return nil, err
	}

	// Event
	var event int32 = EVENT_NONE
	err = binary.Write(scrapeRequest, binary.BigEndian, event)
	if err != nil {
		return nil, err
	}

	// IP address
	var ipAddress uint32 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, ipAddress)
	if err != nil {
		return nil, err
	}

	// Key
	var key int32 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, key)
	if err != nil {
		return nil, err
	}

	// Num want
	var numWant int32 = 200
	err = binary.Write(scrapeRequest, binary.BigEndian, numWant)
	if err != nil {
		return nil, err
	}

	// Port
	var port int16 = 0
	err = binary.Write(scrapeRequest, binary.BigEndian, port)
	if err != nil {
		return nil, err
	}

	_, err = t.conn.Write(scrapeRequest.Bytes())
	if err != nil {
		return nil, err
	}

	responseBytes := make([]byte, 20+200*6)
	responseLen, err := t.conn.Read(responseBytes)
	if err != nil {
		return nil, err
	}

	if responseLen < 16 {
		return nil, fmt.Errorf("Unexpected response size %d", responseLen)
	}

	response := bytes.NewBuffer(responseBytes)

	// Action
	var responseAction uint32
	err = binary.Read(response, binary.BigEndian, &responseAction)
	if err != nil {
		return nil, err
	}
	if action != ACTION_ANNOUNCE {
		return nil, fmt.Errorf("Unexpected response action %d", action)
	}

	// Transaction ID
	var responseTransactionID uint32
	err = binary.Read(response, binary.BigEndian, &responseTransactionID)
	if err != nil {
		return nil, err
	}
	if transactionID != responseTransactionID {
		return nil, fmt.Errorf(
			"Unexpected response transactionID %x",
			responseTransactionID,
		)
	}

	// Interval
	var interval uint32
	err = binary.Read(response, binary.BigEndian, &interval)
	if err != nil {
		return nil, err
	}

	// Leechers
	var leechers uint32
	err = binary.Read(response, binary.BigEndian, &leechers)
	if err != nil {
		return nil, err
	}

	// Seeders
	var seeders uint32
	err = binary.Read(response, binary.BigEndian, &seeders)
	if err != nil {
		return nil, err
	}

	peerCount := (responseLen - 20) / 6
	peerDataBytes := make([]byte, 6*peerCount)
	err = binary.Read(response, binary.BigEndian, &peerDataBytes)
	if err != nil {
		return nil, err
	}

	return &trackerResponse{
		Interval:   uint(interval),
		Complete:   uint(seeders),
		Incomplete: uint(leechers),
		Peers:      string(peerDataBytes),
	}, nil
}

func (t *tracker) Process(infohash *Infohash, redisClient *redis.Client) {
	app.Debugf("Tracker.Process()", "Processing %s", infohash.RawString())
	response, err := t.query(infohash.String())
	if err != nil {
		app.Debugf("Tracker.Process()", "Error querying %s: %s", infohash.GetTracker().GetURL().String(), err)
		go stathat.PostEZCount("errors.query-udp-tracker", "lovek323@gmail.com", 1)
		RedisCmd(
			redisClient,
			"SREM",
			fmt.Sprintf("torrents.%s.trackers", infohash.String()),
			t.url.String(),
		)
		infohash.UpdateScore(0.5, redisClient)
		return
	}
	peers, err := ParsePeers(response.Peers)
	if err != nil {
		panic(err)
	}
	if len(peers) == 0 {
		RedisCmd(
			redisClient,
			"SREM",
			fmt.Sprintf("torrents.%s.trackers", infohash.String()),
			t.url.String(),
		)
		infohash.UpdateScore(0.5, redisClient)
	}
	newPeers := 0
	for _, peer := range peers {
		if peer.Process(infohash, redisClient) == PEER_NEW {
			newPeers++
		}
	}
	var scoreModifier float64
	peerThreshold := int(float64(t.maxPeerCount) * float64(TORRENT_MAX_PEER_THRESHOLD))
	hasMaxPeers := len(peers) >= t.maxPeerCount
	hasEnoughNewPeers := newPeers > peerThreshold
	if hasMaxPeers && hasEnoughNewPeers {
		scoreModifier = 1.2
	} else {
		RedisCmd(
			redisClient,
			"SREM",
			fmt.Sprintf("torrents.%s.trackers", infohash.String()),
			t.url.String(),
		)
		scoreModifier = 0.8
	}
	infohash.UpdateScore(scoreModifier, redisClient)
	if newPeers > 0 {
		log.Printf(
			"P: %03d/%03d H: %s S: %e [%d torrents/hr] [%d peers/hr]\n",
			newPeers,
			len(peers),
			infohash.String(),
			infohash.GetScore(),
			app.GetTorrentRateCounter().Rate(),
			app.GetPeerRateCounter().Rate(),
		)
	}
}

func (t *tracker) GetURL() *url.URL {
	return t.url
}
