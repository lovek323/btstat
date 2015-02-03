package main

import (
    "log"
    "strings"
)

import "github.com/fzzy/radix/redis"

func GetRedisClient() *redis.Client {
    redisClient, err := redis.Dial("tcp", "localhost:6379")

    if err != nil {
        log.Fatalf("Could not connect to redis: %s\n", err)
    }

    return redisClient
}

func RedisIntCmd(
    redisClient *redis.Client,
    command string,
    args ...string,
) int {
    argsIface := make([]interface{}, len(args))
    for index, arg := range args {
        argsIface[index] = interface{}(arg)
    }

    reply := redisClient.Cmd(command, argsIface)

    if reply.Err != nil {
        log.Fatalf(
            "Could not execute '%s %s': %s\n",
            command,
            strings.Join(args, " "),
            reply.Err,
        )
    }

    val, err := reply.Int()

    if err != nil {
        log.Fatalf(
            "Could not parse integer reply from '%s': %s (command: %s %s)\n",
            reply.String(),
            err,
            command,
            strings.Join(args, " "),
        )
    }

    return val
}

func RedisStrCmd(
    redisClient *redis.Client,
    command string,
    args ...string,
) string {
    argsIface := make([]interface{}, len(args))
    for index, arg := range args {
        argsIface[index] = interface{}(arg)
    }

    reply := redisClient.Cmd(command, argsIface)

    if reply.Err != nil {
        log.Fatalf(
            "Could not execute '%s': %s\n",
            strings.Join(args, " "),
            reply.Err,
        )
    }

    return reply.String()
}

func RedisStrsCmd(
    redisClient *redis.Client,
    command string,
    args ...string,
) []string {
    argsIface := make([]interface{}, len(args))
    for index, arg := range args {
        argsIface[index] = interface{}(arg)
    }

    reply := redisClient.Cmd(command, argsIface)

    if reply.Err != nil {
        log.Fatalf(
            "Could not execute '%s': %s\n",
            strings.Join(args, " "),
            reply.Err,
        )
    }

    values, err := reply.List()

    if err != nil {
        log.Fatalf(
            "Could not execute '%s': %s\n",
            strings.Join(args, " "),
            err,
        )
    }

    return values
}

func RedisCmd(
    redisClient *redis.Client,
    command string,
    args ...string,
) *redis.Reply {
    argsIface := make([]interface{}, len(args))
    for index, arg := range args {
        argsIface[index] = interface{}(arg)
    }

    reply := redisClient.Cmd(command, argsIface)

    if reply.Err != nil {
        log.Fatalf(
            "Could not execute '%s': %s\n",
            strings.Join(args, " "),
            reply.Err,
        )
    }

    return reply
}

