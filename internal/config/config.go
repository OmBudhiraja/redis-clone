package config

import "net"

type Replica struct {
	ConnAddr *net.Conn
}

type ServerConfig struct {
	Role             string
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplid     string
	MasterReplOffset int
	Replicas         []Replica
}

func New(port, masterHost, masterPort string) *ServerConfig {
	return &ServerConfig{
		Role:             "master",
		Port:             port,
		MasterHost:       masterHost,
		MasterPort:       masterPort,
		MasterReplid:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset: 0,
		Replicas:         make([]Replica, 0),
	}
}
