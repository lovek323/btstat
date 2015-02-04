package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/stathat/go"

const FIELD_TRACKER_URL = 0
const FIELD_MAX_PEER_COUNT = 1
const FIELD_INFOHASH = 2

type Infohash struct {
	str     string
	score   float64
	tracker *tracker
}

func NewInfohash(str string, score float64, tracker *tracker) *Infohash {
	return &Infohash{str, score, tracker}
}

func (i *Infohash) UpdateScore(modifier float64, redisClient *redis.Client) {
	if i.score*modifier < 0.005 {
		RedisCmd(redisClient, "ZREM", "torrents", i.RawString())
		return
	}
	i.UpdateScoreRedisOnly(modifier, redisClient)
	// Now update the Infohash object's score property.
	i.score *= modifier
}

func (i *Infohash) UpdateScoreRedisOnly(modifier float64, redisClient *redis.Client) {
	score := i.score * modifier
	RedisCmd(redisClient, "ZADD", "torrents", fmt.Sprintf("%f", score), i.RawString())
}

func (i *Infohash) RawString() string {
	return fmt.Sprintf("%s|%s|%s", i.tracker.url.String(), fmt.Sprintf("%d", i.tracker.maxPeerCount), i.str)
}

func (i *Infohash) String() string {
	return i.str
}

func (i *Infohash) GetScore() float64 {
	return i.score
}

func (i *Infohash) GetTracker() *tracker {
	return i.tracker
}

func (i *Infohash) Process(redisClient *redis.Client) {
	i.GetTracker().Process(i, redisClient)
	reply := RedisCmd(redisClient, "GET", fmt.Sprintf("torrents.%s.processed", i.RawString()))
	if reply.Type == redis.NilReply {
		RedisCmd(
			redisClient,
			"SETEX",
			fmt.Sprintf("torrents.%s.processed", i.String()),
			strconv.FormatInt(int64(time.Hour.Seconds()), 10),
			time.Now().Format("2006-01-02 15:04:05"),
		)
		// This torrent has already been recorded as being processed
		// within the last hour.
		app.GetTorrentRateCounter().Incr(1)
		stathat.PostEZCount("torrents.processed", "lovek323@gmail.com", 1)
	}
}

func ParseInfohash(raw string, score float64) *Infohash {
	fields := strings.Split(raw, "|")
	maxPeerCount, err := strconv.ParseInt(fields[FIELD_MAX_PEER_COUNT], 10, 32)
	if err != nil {
		app.Debugf(
			"ParseInfohash()",
			"Could not parse %s as an integer (original: %s)",
			fields[FIELD_MAX_PEER_COUNT],
			raw,
		)
		// Set to some sane default value
		maxPeerCount = 50
	}
	tracker, err := NewTracker(fields[FIELD_TRACKER_URL], int(maxPeerCount))
	if err != nil {
		panic(err)
	}
	return NewInfohash(fields[FIELD_INFOHASH], score, tracker)
}
