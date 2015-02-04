package main

import "net"

import "github.com/oschwald/geoip2-golang"

type geoIp struct {
	db *geoip2.Reader
}

func NewGeoIp(databaseFilename string) (*geoIp, error) {
	db, err := geoip2.Open(databaseFilename)
	geoIp := &geoIp{db: db}

	if err != nil {
		return nil, err
	}

	return geoIp, nil
}

func (g *geoIp) getCountryInfo(ipAddress net.IP) (*geoip2.Country, error) {
	return g.db.Country(ipAddress)
}

func (g *geoIp) getCityInfo(ipAddress net.IP) (*geoip2.City, error) {
	return g.db.City(ipAddress)
}
