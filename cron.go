package main

import "github.com/robfig/cron"

func runCron() {
	// Process kickass torrents hourly delta every half hour
	c := cron.New()
	c.AddFunc("@every 1h", ProcessLatestKatEntries)
	c.Start()
}
