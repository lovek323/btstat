package main

import (
	"net"
	"strconv"
	"strings"
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/nictuku/nettools"
import "github.com/stathat/go"

const PEER_NEW = true
const PEER_EXISTING = false
const PEER_LENGTH = 6

type Peer struct {
	ip   net.IP
	port uint16
}

func parsePeer(info string) (*Peer, error) {
	ipPortStr := nettools.BinaryToDottedPort(info)
	ipPortArr := strings.Split(ipPortStr, ":")
	ipStr := ipPortArr[0]
	ip := net.ParseIP(ipStr)
	port, err := strconv.ParseUint(ipPortArr[1], 10, 16)
	if err != nil {
		return nil, err
	}
	return &Peer{ip, uint16(port)}, nil
}

func ParsePeers(peersStr string) ([]*Peer, error) {
	peers := make([]*Peer, len(peersStr)/PEER_LENGTH)
	for i := 0; i < len(peersStr); i += PEER_LENGTH {
		peer, err := parsePeer(peersStr[i : i+PEER_LENGTH])
		if err != nil {
			return nil, err
		}
		peers[i/PEER_LENGTH] = peer
	}
	return peers, nil
}

func (p *Peer) Process(infohash *Infohash, redisClient *redis.Client) bool {
	start := time.Now()
	metrics := []*Metric{
		NewMetric("countries.users.sorted", STATS_HAT_YES, "<country-name>"),
		NewMetric("countries.<country-name>.users", STATS_HAT_NO, SORTED_SET_NO),
		NewMetric("torrents.users.sorted", STATS_HAT_NO, "<infohash>"),
		NewMetric("torrents.<infohash>.users", STATS_HAT_NO, SORTED_SET_NO),
		NewMetric("torrents.<infohash>.countries.<country-name>.users", STATS_HAT_NO, SORTED_SET_NO),
		NewMetric("users", STATS_HAT_YES, SORTED_SET_NO),
	}
	newIp := PEER_EXISTING
	for _, metric := range metrics {
		if metric.Register(p, infohash, redisClient) == PEER_NEW {
			newIp = PEER_NEW
			if metric.str == "users" {
				app.GetPeerRateCounter().Incr(1)
			}
		}
	}
	elapsed := time.Since(start)
	go stathat.PostEZValue("timings.Peer.Process", "lovek323@gmail.com", elapsed.Seconds())
	return newIp
}

func (p *Peer) GetIP() net.IP {
	return p.ip
}
