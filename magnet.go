package main

import (
    "log"
    "net/url"
)

type MagnetLink struct {
    InfoHash string
    TrackerUri string
    DisplayName string
}

// full URI: magnet:?xt=urn:btih:<infoHash>&dn=<displayName>&tr=<trackerUri>
// infoHash: 4ACA63889990E3AE5B66E8C9B9E0000CD18E5AFA
// displayName: taken+3+2014+1080p+hdrip+x264+aac+jyk
// trackerUri: udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce

func ParseMagnetUri(uri string) MagnetLink {
    url, err := url.Parse(uri)

    if err != nil {
        log.Fatalf("Could not parse magnet link: %s\n", err)
    }

    values := url.Query()

    for key, value := range values {
        log.Printf("%s = %s\n", key, value)
    }

    log.Printf("Parsed magnet link: %s\n", url)

    link := MagnetLink{}

    return link
}

