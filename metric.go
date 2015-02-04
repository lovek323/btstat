package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/stathat/go"

type Metric struct {
	str            string
	storeInStatHat bool
	sortedSetKey   string
}

const STATS_HAT_YES = true
const STATS_HAT_NO = false
const SORTED_SET_NO = ""

func NewMetric(str string, storeInStatHat bool, sortedSetKey string) *Metric {
	return &Metric{str, storeInStatHat, sortedSetKey}
}

func (m *Metric) String(peer *Peer, infohash *Infohash) (string, error) {
	geoIp, err := app.GetGeoIP()
	if err != nil {
		return "", err
	}
	countryInfo, err := geoIp.getCountryInfo(peer.GetIP())
	if err != nil {
		return "", err
	}

	metricStr := strings.Replace(m.str, "<infohash>", infohash.String(), -1)
	metricStr = strings.Replace(metricStr, "<country-name>", countryInfo.Country.Names["en"], -1)

	return metricStr, nil
}

func (m *Metric) Register(
	peer *Peer,
	infohash *Infohash,
	redisClient *redis.Client,
) bool {
	metricStr, err := m.String(peer, infohash)
	if err != nil {
		panic(err)
	}

	if RedisCmd(
		redisClient,
		"GET",
		fmt.Sprintf("%s.%s", metricStr, peer.GetIP().String()),
	).Type != redis.NilReply {
		// This IP address has been recorded against this metric in the last 30
		// days, don't record it again.
		return PEER_EXISTING
	}

	// Set the value and the TTL on the IP address.
	RedisCmd(
		redisClient,
		"SETEX",
		fmt.Sprintf("%s.%s", metricStr, peer.GetIP().String()),
		strconv.FormatInt(int64((time.Hour*24*30).Seconds()), 10),
		time.Now().Format("2006-01-02 15:04:05"),
	)

	yearMonthStr := time.Now().Format("2006-01")

	if m.sortedSetKey != "" {
		var sortedSetValue string

		if m.sortedSetKey == "<infohash>" {
			sortedSetValue = infohash.String()
		} else if m.sortedSetKey == "<country-name>" {
			geoIp, err := app.GetGeoIP()
			if err != nil {
				panic(err)
			}
			countryInfo, err := geoIp.getCountryInfo(peer.GetIP())
			if err != nil {
				panic(err)
			}
			sortedSetValue = countryInfo.Country.Names["en"]
		} else {
			panic(fmt.Sprintf("Invalid sorted set key: %s", m.sortedSetKey))
		}

		// Record the monthly metric in redis.
		RedisCmd(
			redisClient,
			"ZINCRBY",
			fmt.Sprintf("%s.%s", metricStr, yearMonthStr),
			"1",
			sortedSetValue,
		)

		// Record the global metric in redis.
		RedisCmd(redisClient, "ZINCRBY", metricStr, "1", sortedSetValue)
	} else {
		// Record the monthly metric in redis.
		RedisCmd(
			redisClient,
			"INCR",
			fmt.Sprintf("%s.%s", metricStr, yearMonthStr),
		)

		// Record the global metric in redis.
		RedisCmd(redisClient, "INCR", metricStr)

		// Record the global metric in StatHat.
		if m.storeInStatHat {
			stathat.PostEZCount(metricStr, "lovek323@gmail.com", 1)
		}
	}

	return PEER_NEW
}
