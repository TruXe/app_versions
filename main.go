package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	MASTER_HOST = "hl2master.steampowered.com:27011"
	APP_ID      = "221100" // DayZ

	WORKERS = 500
	TIMEOUT = 3 * time.Second
)

type Server struct {
	IP         string `json:"ip"`
	Port       uint16 `json:"port"`
	Name       string `json:"name"`
	Map        string `json:"map"`
	Players    uint8  `json:"players"`
	MaxPlayers uint8  `json:"maxPlayers"`
	Bots       uint8  `json:"bots"`
	Password   bool   `json:"password"`
	Ping       int64  `json:"ping"`
	Version    string `json:"version"`
}

var (
	results []Server
	lock    sync.Mutex
)

func main() {
	fmt.Println("Fetching DayZ servers from Steam master...")

	servers, err := FetchMasterServers()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d servers\n", len(servers))

	serverChan := make(chan string, len(servers))

	wg := sync.WaitGroup{}

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(serverChan, &wg)
	}

	for _, s := range servers {
		serverChan <- s
	}
	close(serverChan)

	wg.Wait()

	fmt.Printf("Online servers: %d\n", len(results))

	data, _ := json.MarshalIndent(results, "", "  ")

	if err := os.WriteFile("servers.json", data, 0644); err != nil {
		panic(err)
	}

	fmt.Println("Saved servers.json")
}

func worker(ch <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for addr := range ch {
		server, err := QueryServer(addr)
		if err != nil {
			continue
		}

		lock.Lock()
		results = append(results, *server)
		lock.Unlock()
	}
}

func FetchMasterServers() ([]string, error) {
	conn, err := net.Dial("udp", MASTER_HOST)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var servers []string

	last := "0.0.0.0:0"

	for {
		packet := BuildMasterPacket(last)

		conn.SetDeadline(time.Now().Add(TIMEOUT))

		if _, err := conn.Write(packet); err != nil {
			return nil, err
		}

		// UDP delivers one datagram per Read; use a generous buffer so a
		// full master reply is never truncated mid-entry.
		buf := make([]byte, 65535)

		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}

		if n < 6 {
			break
		}

		// Master reply header is FF FF FF FF 66 0A, payload is 6-byte
		// IP(4)+port(2) entries.
		data := buf[6:n]

		foundEnd := false
		lastInBatch := last

		for i := 0; i+6 <= len(data); i += 6 {
			ip := net.IPv4(data[i], data[i+1], data[i+2], data[i+3])
			port := binary.BigEndian.Uint16(data[i+4 : i+6])

			addr := fmt.Sprintf("%s:%d", ip.String(), port)

			// 0.0.0.0:0 is the end-of-list sentinel.
			if addr == "0.0.0.0:0" {
				foundEnd = true
				break
			}

			servers = append(servers, addr)
			lastInBatch = addr
		}

		if foundEnd {
			break
		}

		// No progress means the master stopped paginating; bail to avoid
		// looping forever on the same seed.
		if lastInBatch == last {
			break
		}
		last = lastInBatch
	}

	return unique(servers), nil
}

func BuildMasterPacket(last string) []byte {
	filter := "\\appid\\" + APP_ID

	buf := bytes.Buffer{}

	buf.WriteByte(0x31) // master query
	buf.WriteByte(0xFF) // region: all
	buf.WriteString(last)
	buf.WriteByte(0x00)
	buf.WriteString(filter)
	buf.WriteByte(0x00)

	return buf.Bytes()
}

func QueryServer(addr string) (*Server, error) {
	start := time.Now()

	conn, err := net.DialTimeout("udp", addr, TIMEOUT)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(TIMEOUT))

	if _, err = conn.Write(BuildA2SInfoPacket(nil)); err != nil {
		return nil, err
	}

	buf := make([]byte, 4096)

	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	// Modern servers reply with a challenge (header 'A' = 0x41); resend the
	// A2S_INFO request with the 4-byte challenge appended.
	if n >= 9 && buf[4] == 0x41 {
		challenge := make([]byte, 4)
		copy(challenge, buf[5:9])

		conn.SetDeadline(time.Now().Add(TIMEOUT))
		if _, err = conn.Write(BuildA2SInfoPacket(challenge)); err != nil {
			return nil, err
		}

		n, err = conn.Read(buf)
		if err != nil {
			return nil, err
		}
	}

	server, err := ParseA2SInfo(buf[:n])
	if err != nil {
		return nil, err
	}

	host, portStr, _ := net.SplitHostPort(addr)

	var port uint16
	fmt.Sscanf(portStr, "%d", &port)

	server.IP = host
	server.Port = port
	server.Ping = time.Since(start).Milliseconds()

	return server, nil
}

func BuildA2SInfoPacket(challenge []byte) []byte {
	packet := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x54}
	packet = append(packet, []byte("Source Engine Query")...)
	packet = append(packet, 0x00)

	if challenge != nil {
		packet = append(packet, challenge...)
	}

	return packet
}

func ParseA2SInfo(data []byte) (*Server, error) {
	if len(data) < 6 || data[4] != 0x49 {
		return nil, fmt.Errorf("invalid A2S_INFO response")
	}

	r := bytes.NewReader(data)

	// Skip 4-byte header (FF FF FF FF), 'I' (0x49) and protocol byte.
	r.Seek(6, 0)

	readString := func() string {
		var out []byte
		for {
			b, err := r.ReadByte()
			if err != nil || b == 0x00 {
				break
			}
			out = append(out, b)
		}
		return string(out)
	}

	server := &Server{}

	server.Name = readString() // name
	server.Map = readString()  // map
	readString()               // folder
	readString()               // game

	// App ID (short, little-endian) — not stored.
	r.ReadByte()
	r.ReadByte()

	binary.Read(r, binary.LittleEndian, &server.Players)
	binary.Read(r, binary.LittleEndian, &server.MaxPlayers)
	binary.Read(r, binary.LittleEndian, &server.Bots)

	var serverType, environment, visibility, vac byte
	binary.Read(r, binary.LittleEndian, &serverType)
	binary.Read(r, binary.LittleEndian, &environment)
	binary.Read(r, binary.LittleEndian, &visibility)
	binary.Read(r, binary.LittleEndian, &vac)

	server.Version = readString()
	server.Password = visibility == 1

	return server, nil
}

func unique(input []string) []string {
	keys := make(map[string]bool)

	var result []string

	for _, v := range input {
		if !keys[v] {
			keys[v] = true
			result = append(result, v)
		}
	}

	return result
}
