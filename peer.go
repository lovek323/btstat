package main

import (
    "fmt"
    "log"
    "strconv"
    "strings"
    "time"
)

import "github.com/fzzy/radix/redis"
import "github.com/stathat/go"

func processPeerIp(
    ipStr string,
    infoHashStr string,
    redisClient *redis.Client,
) bool {
    // Key is the metric name with replacement parameters, value is `true`
    // if the metric should be stored in StatHat, `false` otherwise.
    metrics := map[string][]interface{}{
        "countries.users.sorted": []interface{}{false, "<country-name>"},
        "countries.<country-name>.users": []interface{}{true, ""},
        // "countries.<country-name>.cities.<city-name>.users": false,
        // "countries.<country-name>.postcodes.<postcode>.users": false,
        "torrents.users.sorted": []interface{}{false, "<infohash>"},
        "torrents.<infohash>.users": []interface{}{false, ""},
        // "torrents.<infohash>.countries.<country-name>.users": false,
        // "torrents.<infohash>.countries.<country-name>.cities.<city-name>.users": false,
        // "torrents.<infohash>.countries.<country-name>.postcodes.<postcode>.users": false,
        "users": []interface{}{true, ""},
    }

    newIp := false

    for metric, args := range metrics {
        if registerMetric(
            metric,
            ipStr,
            infoHashStr,
            redisClient,
            args[0].(bool), // storeInStatsHat
            args[1].(string), // sorted set parameter
        ) {
            newIp = true
        }
    }

    return newIp
}

func registerMetric(
    metric string,
    ipStr string,
    infoHashStr string,
    redisClient *redis.Client,
    storeInStatHat bool,
    sortedSetKey string,
) bool {
    metric = strings.Replace(metric, "<infohash>", infoHashStr, -1)

    geoIpClient := GeoIpClient{}
    ipInfo, err := geoIpClient.GetGeoIpInfo(ipStr, redisClient)

    if err != nil {
        log.Fatalf("Could not get IP info: %s\n", err)
    }

    metric = strings.Replace(
        metric,
        "<country-name>",
        ipInfo.Country.Names.En,
        -1,
    )

    /* metric = strings.Replace(
        metric,
        "<city-name>",
        ipInfo.City.Names.En,
        -1,
    )

    metric = strings.Replace(
        metric,
        "<postcode>",
        ipInfo.Postal.Code,
        -1,
    ) */

    if RedisCmd(
        redisClient,
        "GET",
        fmt.Sprintf("%s.%s", metric, ipStr),
    ).Type != redis.NilReply {
        // This IP address has been recorded against this metric in the last 30
        // days, don't record it again.
        return false
    }

    // Set the value and the TTL on the IP address.
    RedisCmd(
        redisClient,
        "SETEX",
        fmt.Sprintf("%s.%s", metric, ipStr),
        strconv.FormatInt(int64((time.Hour * 24 * 30).Seconds()), 10),
        time.Now().Format("2006-01-02 15:04:05"),
    )

    yearMonthStr := time.Now().Format("2006-01")

    if sortedSetKey != "" {
        var sortedSetValue string

        if sortedSetKey == "<infohash>" {
            sortedSetValue = infoHashStr
        } else if sortedSetKey == "<country-name>" {
            sortedSetValue = ipInfo.Country.Names.En
        } else {
            return false
        }

        // Record the monthly metric in redis.
        RedisCmd(
            redisClient,
            "ZINCRBY",
            fmt.Sprintf("%s.%s", metric, yearMonthStr),
            "1",
            sortedSetValue,
        )

        // Record the global metric in redis.
        RedisCmd(redisClient, "ZINCRBY", metric, "1", sortedSetValue)
    } else {
        // Record the monthly metric in redis.
        RedisCmd(
            redisClient,
            "INCR",
            fmt.Sprintf("%s.%s", metric, yearMonthStr),
        )

        // Record the global metric in redis.
        RedisCmd(redisClient, "INCR", metric)

        // Record the global metric in StatHat.
        if storeInStatHat {
            stathat.PostEZCount(metric, "lovek323@gmail.com", 1)
        }
    }

    return true
}

