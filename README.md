# Btstat

## Processing torrents

### Scoring

1. All torrents will start with a score of `1.0`.

2. If no peers are found when a torrent is processed, the score will be reduced
   by 50% (i.e., multiplied by `0.8`) which will push it further down the list
   of torrents that we will process.

3. If fewer than 200 (but more than 0) peers are found, the score will be
   reduced by 80% which will push it further down the list but not as far as
   torrents with no peers.

4. If 200 or more peers are found (where 200 is the maximum UDP trackers seem to
   return) and at least some threshold were new (we use 10% of the max peers for
   this threshold currently), the score will be increased by 20% (i.e.,
   multiplied by `1.2`) to put it back at the top of the queue.

### Number of goroutines

If a torrent's score gets below `0.005` (i.e., `0.5^8`, after the ninth time
with no peers or `0.75^18`, after the nineteenth time with fewer than 200
peers), it will be removed. This will hopefully help keep the number of torrents
we're tracking at a manageable level.

We need to make sure we're actually getting through all the torrents in our
list, so we will store two metrics on StatHat: `torrents.count` and
`torrents.processed`. The first will be a value metric, i.e., the number of
registered torrents and the second will be a count metric, i.e., the number
processed per hour. `torrents.count` should aways be greater than or equal to
`torrents.processed` -- if it is less, we need to increase the number of
goroutines. In the future, it would be good to update this number automatically
by comparing these two stats, but for the time being, it will require manual
intervention.


## Metrics

Individual IP addresses will be recorded against torrents at most once.

IP addresses will be recorded at most once every 30 days for each metric.
That is, an IP address that is continually active will register as a unique
user at most once every 30 days.


### Redis metrics

```
SETNX <metric>.<ip-address> <time-string>
if reply == 1
  EXPIRE users.<ip-address> <time.Hour * 24 * 30>
  INCR <metric>.<year-month>
  INCR <metric>
endif
```

where `<metric>` is one of:

- `countries.<country-name>.users`
- `countries.<country-name>.cities.<city-name>.users`
- `countries.<country-name>.postcodes.<postcode>.users`
- `torrents.<info-hash>.users`
- `torrents.<info-hash>.countries.<country-name>.users`
- `torrents.<info-hash>.countries.<country-name>.cities.<city-name>.users`
- `torrents.<info-hash>.countries.<country-name>.postcodes.<postcode>.users`


### StatHat metrics

StatHat has two types of metrics: counter stats and value stats. Counter stats
sum up counts over time. Value stats average values over time.

We will store each of these country metrics as counter metrics: each time we
call the `INCR` redis command, we will increment the StatHat counter by `1`.

