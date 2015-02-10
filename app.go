package main

import (
	"log"
	"strconv"
	"time"
)

import "github.com/fzzy/radix/redis"
import "github.com/getlantern/golog"
import "github.com/paulbellamy/ratecounter"

type App struct {
	geoIp          *geoIp
	redisClient    *redis.Client
	peerCounter    *ratecounter.RateCounter
	torrentCounter *ratecounter.RateCounter
}

const GEOIP_DATABASE_FILENAME = "GeoLite2-Country.mmdb"

func NewApp() *App {
	return new(App)
}

func (a *App) GetGeoIP() (*geoIp, error) {
	if a.geoIp == nil {
		geoIp, err := NewGeoIp(GEOIP_DATABASE_FILENAME)
		if err != nil {
			return nil, err
		}
		a.geoIp = geoIp
	}
	return a.geoIp, nil
}

func (a *App) GetRedisClient() (*redis.Client, error) {
	if a.redisClient == nil {
		redisClient, err := redis.Dial("tcp", "redis1.btstat.internal:6379")
		if err != nil {
			log.Fatalf("Could not connect to redis: %s\n", err)
		}
		a.redisClient = redisClient
	}
	return a.redisClient, nil
}

func (a *App) GetPeerRateCounter() *ratecounter.RateCounter {
	if a.peerCounter == nil {
		a.peerCounter = ratecounter.NewRateCounter(1 * time.Hour)
	}
	return a.peerCounter
}

func (a *App) GetTorrentRateCounter() *ratecounter.RateCounter {
	if a.torrentCounter == nil {
		a.torrentCounter = ratecounter.NewRateCounter(1 * time.Hour)
	}
	return a.torrentCounter
}

func (a *App) RunUDPQueryer() {
	infohashes := make(chan *Infohash)
	for i := 0; i < CONCURRENT_GOROUTINES; i++ {
		go func(i int) {
			app.Debugf("Goroutine %d", "Starting", i)
			redisClient := GetRedisClient()
			defer redisClient.Close()
			for infohash := range infohashes {
				app.Tracef(
					"Goroutine %d",
					"Processing %s on %s",
					i,
					infohash.String(),
					infohash.GetTracker().GetURL().String(),
				)
				infohash.Process(redisClient)
			}
		}(i)
	}
B:
	redisClient, err := a.GetRedisClient()
	if err != nil {
		panic(err)
	}
	defer redisClient.Close()
	infohashStrs, err := RedisStrsCmd(redisClient, "ZREVRANGE", "torrents", "0", "50", "WITHSCORES")
	if err != nil {
		panic(err)
	}
	infohashStr := ""
	for index, value := range infohashStrs {
		if index%2 == 0 {
			infohashStr = value
			continue
		}
		infohashScore, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Fatalf("Could not parse float %s: %s\n", value, err)
		}
		// Set this torrent to have a zero score so it is not plucked by another
		// worker.
		infohash := ParseInfohash(infohashStr, infohashScore)
		infohash.UpdateScoreRedisOnly(0, redisClient)
		// Send it to the channel to be processed.
		infohashes <- infohash
	}
	time.Sleep(time.Second)
	goto B
}

func (a *App) Tracef(prefix string, message string, args ...interface{}) {
	golog.LoggerFor(prefix).Tracef(message, args...)
}

func (a *App) Debugf(prefix string, message string, args ...interface{}) {
	golog.LoggerFor(prefix).Debugf(message, args...)
}
