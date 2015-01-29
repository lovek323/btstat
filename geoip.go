package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
)

import "github.com/fzzy/radix/redis"

type GeoIpClient struct { }

type GeoIpError struct {
    Error string
}

type GeoIpCity struct {
    City struct {
        Confidence float64 `json:"confidence"`
        GeonameID  float64 `json:"geoname_id"`
        Names      struct {
            En string `json:"en"`
        } `json:"names"`
    } `json:"city"`
    Continent struct {
        Code      string  `json:"code"`
        GeonameID float64 `json:"geoname_id"`
        Names     struct {
            De    string `json:"de"`
            En    string `json:"en"`
            Es    string `json:"es"`
            Fr    string `json:"fr"`
            Ja    string `json:"ja"`
            Pt_BR string `json:"pt-BR"`
            Ru    string `json:"ru"`
            Zh_CN string `json:"zh-CN"`
        } `json:"names"`
    } `json:"continent"`
    Country struct {
        Confidence float64 `json:"confidence"`
        GeonameID  float64 `json:"geoname_id"`
        IsoCode    string  `json:"iso_code"`
        Names      struct {
            De    string `json:"de"`
            En    string `json:"en"`
            Es    string `json:"es"`
            Fr    string `json:"fr"`
            Ja    string `json:"ja"`
            Pt_BR string `json:"pt-BR"`
            Ru    string `json:"ru"`
            Zh_CN string `json:"zh-CN"`
        } `json:"names"`
    } `json:"country"`
    Location struct {
        AccuracyRadius float64 `json:"accuracy_radius"`
        Latitude       float64 `json:"latitude"`
        Longitude      float64 `json:"longitude"`
        TimeZone       string  `json:"time_zone"`
    } `json:"location"`
    Maxmind struct {
        QueriesRemaining float64 `json:"queries_remaining"`
    } `json:"maxmind"`
    Postal struct {
        Code       string  `json:"code"`
        Confidence float64 `json:"confidence"`
    } `json:"postal"`
    RegisteredCountry struct {
        GeonameID float64 `json:"geoname_id"`
        IsoCode   string  `json:"iso_code"`
        Names     struct {
            De    string `json:"de"`
            En    string `json:"en"`
            Es    string `json:"es"`
            Fr    string `json:"fr"`
            Ja    string `json:"ja"`
            Pt_BR string `json:"pt-BR"`
            Ru    string `json:"ru"`
            Zh_CN string `json:"zh-CN"`
        } `json:"names"`
    } `json:"registered_country"`
    Subdivisions []struct {
        Confidence float64 `json:"confidence"`
        GeonameID  float64 `json:"geoname_id"`
        IsoCode    string  `json:"iso_code"`
        Names      struct {
            En    string `json:"en"`
            Fr    string `json:"fr"`
            Pt_BR string `json:"pt-BR"`
            Ru    string `json:"ru"`
        } `json:"names"`
    } `json:"subdivisions"`
    Traits struct {
        AutonomousSystemNumber       float64 `json:"autonomous_system_number"`
        AutonomousSystemOrganization string  `json:"autonomous_system_organization"`
        IpAddress                    string  `json:"ip_address"`
        Isp                          string  `json:"isp"`
        Organization                 string  `json:"organization"`
        UserType                     string  `json:"user_type"`
    } `json:"traits"`
}

const GEOIP_CITY_PATTERN = "https://geoip.maxmind.com/geoip/v2.1/city/%s"

func (c GeoIpClient) GetGeoIpInfo(
    ip string,
    redisClient *redis.Client,
) (*GeoIpCity, error) {
    config := GetConfig()

    reply := redisClient.Cmd(
        "EXISTS",
        fmt.Sprintf("ips.%s", ip),
    )

    if reply.Err != nil {
        return nil,
            errors.New(
                fmt.Sprintf(
                    "Failed to check whether IP has been recorded: %s\n",
                    reply.Err,
                ),
            )
    }

    exists, err := reply.Int()

    var bytes []byte

    if exists == 1 {
        reply := redisClient.Cmd("GET", fmt.Sprintf("ips.%s", ip))

        if reply.Err != nil {
            return nil,
                errors.New(
                    fmt.Sprintf(
                        "Failed to get IP info from redis: %s\n",
                        reply.Err,
                    ),
                )
        }

        bytes = []byte(reply.String())
    } else {
        log.Printf("Requesting info for %s from Maxmind\n", ip)

        request, err := http.NewRequest(
            "GET",
            fmt.Sprintf(GEOIP_CITY_PATTERN, ip),
            nil,
        )

        if err != nil {
            return nil,
                errors.New(
                    fmt.Sprintf(
                        "Could not create IP info request: %s\n",
                        err,
                    ),
                )
        }

        request.SetBasicAuth(config.GeoIp.UserId, config.GeoIp.LicenseKey)

        client := http.Client{}

        response, err := client.Do(request)

        if err != nil {
            return nil,
                errors.New(
                    fmt.Sprintf(
                        "Could not perform IP info request: %s",
                        err,
                    ),
                )
        }

        bytes, err = ioutil.ReadAll(response.Body)

        if err != nil {
            return nil,
                errors.New(fmt.Sprintf("Could not get IP info JSON: %s", err))
        }

        redisClient.Cmd("SET", fmt.Sprintf("ips.%s", ip), string(bytes))
    }

    geoIpError := GeoIpError{}

    err = json.Unmarshal(bytes, &geoIpError)

    if err != nil {
        return nil,
            errors.New(fmt.Sprintf("Could not parse IP info JSON: %s", err))
    }

    if geoIpError.Error != "" {
        return nil,
            errors.New(
                fmt.Sprintf(
                    "Could not get IP info JSON: %s",
                    geoIpError.Error,
                ),
            )
    }

    ipInfo := GeoIpCity{}

    err = json.Unmarshal(bytes, &ipInfo)

    if err != nil {
        return nil,
            errors.New(fmt.Sprintf("Could not parse IP info JSON: %s", err))
    }

    return &ipInfo, nil
}

