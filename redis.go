package main

import "log"

import "github.com/fzzy/radix/redis"

func GetRedisClient() *redis.Client {
    redisClient, err := redis.Dial("tcp", "localhost:6379")

    if err != nil {
        log.Fatalf("Could not connect to redis: %s\n", err)
    }

    return redisClient
}
