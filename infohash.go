package main

import (
	"fmt"
	"strconv"
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/stathat/go"

type Infohash struct {
	str   string
	score float64
}

func NewInfohash(str string, score float64) *Infohash {
	return &Infohash{str, score}
}

func (i *Infohash) UpdateScore(modifier float64, redisClient *redis.Client) {
	if i.score*modifier < 0.005 {
		RedisCmd(
			redisClient,
			"ZREM",
			"torrents",
			i.str,
		)
		return
	}
	i.UpdateScoreRedisOnly(modifier, redisClient)
	// Now update the Infohash object's score property.
	i.score *= modifier
}

func (i *Infohash) UpdateScoreRedisOnly(modifier float64, redisClient *redis.Client) {
	score := i.score * modifier
	RedisCmd(
		redisClient,
		"ZADD",
		"torrents",
		strconv.FormatFloat(score, 'f', -1, 64),
		i.str,
	)
}

func (i *Infohash) String() string {
	return i.str
}

func (i *Infohash) GetScore() float64 {
	return i.score
}

func (i *Infohash) Run(redisClient *redis.Client) {
	trackerUrlStrs := map[string]int{
		"udp://12.rarbg.me:80/announce":                     200,
		"udp://9.rarbg.com:2710/announce":                   50,
		"udp://open.demonii.com:1337/announce":              200,
		"udp://tracker.coppersurfer.tk:6969/announce":       200,
		"udp://tracker.leechers-paradise.org:6969/announce": 200,
		"udp://tracker.token.ro:80/announce":                200,
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
		fmt.Sprintf("torrents.%s.trackers", i.String()),
	)
	app.Tracef("Infohash", "Trackers for %s: %v", i.String(), setTrackerUrlStrs)
	var currentTrackerUrlStrs = make(
		map[string]int,
		len(trackerUrlStrs),
	)
	if len(setTrackerUrlStrs) == 0 {
		for trackerUrlStr, _ := range trackerUrlStrs {
			RedisCmd(
				redisClient,
				"SADD",
				fmt.Sprintf("torrents.%s.trackers", i.String()),
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
		tracker, err := NewTracker(trackerUrlStr, maxPeerCount)
		if err != nil {
			panic(err)
		}
		tracker.Process(i, redisClient)
	}
	if RedisCmd(
		redisClient,
		"GET",
		fmt.Sprintf("torrents.%s.processed", i.String()),
	).Type == redis.NilReply {
		RedisCmd(
			redisClient,
			"SETEX",
			fmt.Sprintf("torrents.%s.processed", i.String()),
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
}
