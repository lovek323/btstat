package main

import (
    "bufio"
    "compress/gzip"
    // "io/ioutil"
    "log"
    "net/http"
    "strings"
)

type KatEntry struct {
    InfoHash string
    Title string
    Category string
    Uri string
}

const KAT_HOURLY_URL = "https://kickass.so/hourlydump.txt.gz"

func GetLatestKatEntries() []KatEntry {
    response, err := http.Get(KAT_HOURLY_URL)

    if err != nil {
        log.Fatalf("Failed to download KAT entries: %s\n", err)
    }

    reader, err := gzip.NewReader(response.Body)

    if err != nil {
        log.Fatalf("Failed to read KAT entries body: %s\n", err)
    }

    /* entries, err := ioutil.ReadAll(reader)

    if err != nil {
        log.Fatalf("Failed to read KAT entries body: %s\n", err)
    } */

    scanner := bufio.NewScanner(reader)

    entries := make([]KatEntry, 1)

    config := GetConfig()

    for scanner.Scan() {
        values := strings.Split(scanner.Text(), "|")

        for _, category := range config.Kat.Categories {
            if category == values[2] {
                entries = append(
                    entries,
                    KatEntry{
                        InfoHash: values[0],
                        Title: values[1],
                        Category: values[2],
                        Uri: values[3],
                    },
                )

                continue
            }
        }
    }

    return entries
}

