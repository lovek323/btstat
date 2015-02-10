package main

import (
    "encoding/json"
    "io/ioutil"
    "log"
)

type Config struct {
    GeoIp struct {
        UserId     string
        LicenseKey string
    }
    Kat struct {
        Categories []string
    }
    RedisServer string
}

func GetConfig() Config {
    bytes, err := ioutil.ReadFile("config.json")

    if err != nil {
        log.Fatalf("Could not read config JSON: %s\n", err)
    }

    config := Config{}

    err = json.Unmarshal(bytes, &config)

    if err != nil {
        log.Fatalf("Could not parse config JSON: %s\n", string(bytes))
    }

    return config
}

