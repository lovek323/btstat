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
    metrics := map[string]bool{
        "countries.<country-name>.users": true,
        "countries.<country-name>.cities.<city-name>.users": false,
        "countries.<country-name>.postcodes.<postcode>.users": false,
        "torrents.<info-hash>.users": false,
        "torrents.<info-hash>.countries.<country-name>.users": false,
        "torrents.<info-hash>.countries.<country-name>.cities.<city-name>.users": false,
        "torrents.<info-hash>.countries.<country-name>.postcodes.<postcode>.users": false,
        "users": true,
    }

    newIp := false

    for metric, storeInStatHat := range metrics {
        newIp = newIp || registerMetric(
            metric,
            ipStr,
            infoHashStr,
            redisClient,
            storeInStatHat,
        )
    }

    return newIp
}

func registerMetric(
    metric string,
    ipStr string,
    infoHashStr string,
    redisClient *redis.Client,
    storeInStatHat bool,
) bool {
    metric = strings.Replace(metric, "<info-hash>", infoHashStr, -1)

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

    metric = strings.Replace(
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
    )

    if RedisIntCmd(
        redisClient,
        "SETNX",
        fmt.Sprintf("%s.%s", metric, ipStr),
        time.Now().Format("2006-01-02 15:04:05"),
    ) != 1 {
        // This IP address has been recorded against this metric in the last 30
        // days, don't record it again.
        return false
    }

    // Set the TTL on the IP address.
    RedisCmd(
        redisClient,
        "EXPIRE",
        fmt.Sprintf("%s.%s", metric, ipStr),
        strconv.FormatInt(int64((time.Hour * 24 * 30).Seconds()), 10),
    )

    yearMonthStr := time.Now().Format("2006-01")

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

    return true
}

