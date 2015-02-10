package main

import (
	"log"
	"strings"
)

import "github.com/fzzy/radix/redis"

func GetRedisClient() *redis.Client {
	app.Tracef("Redis::NewRedis()", "Opening new redis connection")
	redisClient, err := redis.Dial("tcp", "redis1.btstat.internal:6379")
	if err != nil {
		log.Fatalf("Could not connect to redis: %s\n", err)
	}
	return redisClient
}

func RedisIntCmd(
	redisClient *redis.Client,
	command string,
	args ...string,
) (int, error) {
	app.Tracef("RedisIntCmd()", "Executing command: %s %s", command, strings.Join(args, " "))
	argsIface := make([]interface{}, len(args))
	for index, arg := range args {
		argsIface[index] = interface{}(arg)
	}
	reply := redisClient.Cmd(command, argsIface)
	if reply.Type == redis.ErrorReply {
		if reply.Err.Error() == "use of closed network connection" {
			log.Printf("Reconnecting redis (RedisIntCmd)\n")
			redisClient = GetRedisClient()
			return RedisIntCmd(redisClient, command, args...)
		}
		app.Debugf(
			"Could not execute '%s %s': %s\n",
			command,
			strings.Join(args, " "),
			reply.Err,
		)
		return 0, reply.Err
	}
	val, err := reply.Int()
	if err != nil {
		app.Debugf(
			"Could not parse integer reply from '%s': %s (command: %s %s)\n",
			reply.String(),
			err,
			command,
			strings.Join(args, " "),
		)
		return 0, reply.Err
	}
	return val, nil
}

func RedisStrCmd(
	redisClient *redis.Client,
	command string,
	args ...string,
) (string, error) {
	app.Tracef("RedisStrCmd()", "Executing command: %s %s", command, strings.Join(args, " "))
	argsIface := make([]interface{}, len(args))
	for index, arg := range args {
		argsIface[index] = interface{}(arg)
	}
	reply := redisClient.Cmd(command, argsIface)
	if reply.Type == redis.ErrorReply {
		if reply.Err.Error() == "use of closed network connection" {
			log.Printf("Reconnecting redis (RedisStrCmd)\n")
			redisClient = GetRedisClient()
			return RedisStrCmd(redisClient, command, args...)
		}
		app.Debugf(
			"Could not execute '%s %s': %s\n",
			command,
			strings.Join(args, " "),
			reply.Err,
		)
		return "", reply.Err
	}
	return reply.String(), nil
}

func RedisStrsCmd(
	redisClient *redis.Client,
	command string,
	args ...string,
) ([]string, error) {
	app.Tracef("RedisStrsCmd()", "Executing command: %s %s", command, strings.Join(args, " "))
	argsIface := make([]interface{}, len(args))
	for index, arg := range args {
		argsIface[index] = interface{}(arg)
	}
	reply := redisClient.Cmd(command, argsIface)
	if reply.Type == redis.NilReply {
		return []string{}, nil
	}
	if reply.Type == redis.ErrorReply {
		if reply.Err.Error() == "use of closed network connection" {
			log.Printf("Reconnecting redis (RedisStrsCmd)\n")
			redisClient = GetRedisClient()
			return RedisStrsCmd(redisClient, command, args...)
		}
		if reply.Err.Error() == "wrong type" {
			return []string{}, nil
		}
		app.Debugf(
			"Could not execute '%s %s': %s\n",
			command,
			strings.Join(args, " "),
			reply.Err,
		)
		return []string{}, nil
	}
	if reply.Type == redis.NilReply {
		return []string{}, nil
	}
	values, err := reply.List()
	if err != nil {
		app.Debugf(
			"Could not execute '%s %s': %s\n",
			command,
			strings.Join(args, " "),
			err,
		)
		return []string{}, err
	}
	return values, nil
}

func RedisCmd(
	redisClient *redis.Client,
	command string,
	args ...string,
) (*redis.Reply, error) {
	app.Tracef("RedisCmd()", "Executing command: %s %s", command, strings.Join(args, " "))
	argsIface := make([]interface{}, len(args))
	for index, arg := range args {
		argsIface[index] = interface{}(arg)
	}
	reply := redisClient.Cmd(command, argsIface)
	if reply.Type == redis.ErrorReply {
		if reply.Err.Error() == "use of closed network connection" {
			log.Printf("Reconnecting redis (RedisCmd %s)\n", command)
			redisClient = GetRedisClient()
			return RedisCmd(redisClient, command, args...)
		}
		app.Debugf(
			"Could not execute '%s %s': %s\n",
			command,
			strings.Join(args, " "),
			reply.Err,
		)
		return nil, reply.Err
	}
	return reply, nil
}
