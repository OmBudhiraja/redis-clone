package config

import (
	"flag"
	"net"
	"os"
	"sync"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

type Replica struct {
	ConnAddr       net.Conn
	Offset         int
	ExpectedOffset int
}

type ServerConfig struct {
	Role                          string
	Port                          string
	RDBDir                        string
	RDBFileName                   string
	MasterHost                    string
	MasterPort                    string
	MasterReplid                  string
	MasterReplOffset              int
	Replicas                      []*Replica
	ReplicaWriteQueue             chan []string
	HandeshakeCompletedWithMaster bool
	sync.RWMutex
}

func New() *ServerConfig {
	port := flag.String("port", "6379", "Port to bind to")
	masterHost := flag.String("replicaof", "", "Host of master server")

	rdbFileDir := flag.String("dir", "", "Directory to store RDB file")
	rdbFileName := flag.String("dbfilename", "", "Name of RDB file")

	flag.Parse()

	masterPort := getMasterPort(masterHost)
	role := RoleMaster

	if *masterHost == "" || masterPort == "" {
	} else {
		role = RoleSlave

	}

	return &ServerConfig{
		Role:              role,
		Port:              *port,
		RDBDir:            *rdbFileDir,
		RDBFileName:       *rdbFileName,
		MasterHost:        *masterHost,
		MasterPort:        masterPort,
		MasterReplid:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset:  0,
		Replicas:          make([]*Replica, 0),
		ReplicaWriteQueue: make(chan []string, 100),
	}
}

func getMasterPort(masterHost *string) string {
	if *masterHost == "" {
		return ""
	}

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == *masterHost {
			if i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		}
	}
	return ""
}
