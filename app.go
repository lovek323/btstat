package main

import (
	"log"
	"strconv"
)

import "github.com/fzzy/radix/redis"
import "github.com/getlantern/golog"

type App struct {
	geoIp       *geoIp
	redisClient *redis.Client
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
		redisClient, err := redis.Dial("tcp", "localhost:6379")
		if err != nil {
			log.Fatalf("Could not connect to redis: %s\n", err)
		}
		a.redisClient = redisClient
	}
	return a.redisClient, nil
}

func (a *App) RunUDPQueryer() {
	infohashes := make(chan *Infohash)
	for i := 0; i < CONCURRENT_GOROUTINES; i++ {
		go func() {
			redisClient := GetRedisClient()
			defer redisClient.Close()
			for infohash := range infohashes {
				infohash.Run(redisClient)
			}
		}()
	}
B:
	redisClient, err := a.GetRedisClient()
	if err != nil {
		panic(err)
	}
	defer redisClient.Close()
	infohashStrs := RedisStrsCmd(
		redisClient,
		"ZREVRANGE",
		"torrents",
		"0",
		strconv.FormatInt(CONCURRENT_GOROUTINES-1, 10),
		"WITHSCORES",
	)
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
		infohash := NewInfohash(infohashStr, infohashScore)
		infohash.UpdateScoreRedisOnly(0, redisClient)
		// Send it to the channel to be processed.
		infohashes <- infohash
	}
	goto B
}

func (a *App) Tracef(prefix string, message string, args ...interface{}) {
	golog.LoggerFor(prefix).Tracef(message, args...)
}
