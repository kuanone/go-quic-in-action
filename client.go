package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/quic-go/quic-go"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type MyData struct {
	HostName string `json:"host_name,omitempty"`
	Ip       string `json:"ip,omitempty"`
	Port     int    `json:"port,omitempty"`
	DeadLine int    `json:"dead_line,omitempty"`
}

const maxStreams = 5

type StreamPool struct {
	connection quic.Connection
	streams    chan quic.Stream
	mu         sync.Mutex
}

func NewStreamPool(connection quic.Connection, maxStreams int) (*StreamPool, error) {
	sp := &StreamPool{
		connection: connection,
		streams:    make(chan quic.Stream, maxStreams),
	}

	for i := 0; i < maxStreams; i++ {
		stream, err := connection.OpenStreamSync(context.Background())
		if err != nil {
			return nil, err
		}
		sp.streams <- stream
	}

	return sp, nil
}

func (sp *StreamPool) GetStream() (quic.Stream, error) {
	select {
	case stream := <-sp.streams:
		return stream, nil
	case <-time.After(5 * time.Second): // 超时时间
		return nil, fmt.Errorf("timeout: no available stream")
	}
}

func (sp *StreamPool) ReturnStream(stream quic.Stream) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	select {
	case sp.streams <- stream:
	default:
		_ = stream.Close() // 如果池已满，关闭stream
	}
}

func GenData(hostname string) ([]byte, error) {
	data := MyData{
		HostName: "mac" + "-" + hostname,
		Ip:       "127.0.0.1",
		Port:     17777,
		DeadLine: 250,
	}

	JsonData, err := json.Marshal(data)
	return JsonData, err
}

var (
	// 通道用于同步发送和接收消息
	sendCh = make(chan []byte)
	recvCh = make(chan []byte)
)

func client3() {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // testing only
		NextProtos:         []string{"h3", "http/1.1"},
	}

	url := "localhost:4242"

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	connection, err := quic.DialAddr(ctx, url, tlsConfig, nil)
	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	defer connection.CloseWithError(quic.ApplicationErrorCode(0), "client exit")

	streamPool, err := NewStreamPool(connection, maxStreams)
	if err != nil {
		log.Fatalf("Failed to create stream pool: %v", err)
	}

	for i := 0; i < 5; i++ {
		go handleStream(ctx, cancel, streamPool)
	}

	for i := 0; i < 100; i++ {
		sendMsg()
	}

	go func() {
		time.Sleep(30 * time.Second)
		sendMsg()
	}()

	select {
	case <-ctx.Done():
		log.Println("Client exiting...")
	}
}

func sendMsg() {
	// 发送ping消息
	data, err := GenData(fmt.Sprintf("%d", time.Now().Unix()))
	if err != nil {
		log.Printf("Failed to generate data: %v", err)
		return
	}
	sendCh <- data
}

func handleStream(ctx context.Context, cancel context.CancelFunc, streamPool *StreamPool) {
	stream, err := streamPool.GetStream()
	if err != nil {
		log.Printf("Failed to open stream: %v", err)
		return
	}

	defer streamPool.ReturnStream(stream)

	errCh := make(chan error)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Printf("Client exiting...1")
				return
			case message := <-sendCh:
				err = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err != nil {
					errCh <- err
					return
				}
				err = writeMessage(stream, message)
				if err != nil {
					errCh <- err
					return
				}
				fmt.Printf("Sent ping: %s\n", message)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Printf("Client exiting...2")
				return
			case err := <-errCh:
				log.Printf("Stream error: %v", err)
				cancel()
				return
			default:
				err = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
				if err != nil {
					errCh <- err
					return
				}
				response, err := readMessage(stream)
				if err != nil {
					if err == io.EOF {
						log.Println("Server closed the connection.")
						return
					}
					errCh <- err
					return
				}
				recvCh <- response
				fmt.Printf("Received pong: %s\n", string(response))
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Client exiting...3")
				return
			case err := <-errCh:
				log.Printf("Stream error: %v", err)
				cancel()
				return
			default:
				// 等待接收响应
				select {
				case <-ctx.Done():
					log.Printf("Client exiting...3.1")
					return
				case response := <-recvCh:
					fmt.Printf("Business logic processing received pong: %s\n", response)
					//todo Business logic processing received pong
				case <-time.After(5 * time.Second): // 超时时间
					log.Println("No response received in 5 seconds, retrying...")
				}

				// time.Sleep(1 * time.Second) // 控制发送间隔
			}
		}
	}()

	wg.Wait()
}

func readMessage(stream quic.Stream) ([]byte, error) {
	var length int32
	err := binary.Read(stream, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(stream, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func writeMessage(stream quic.Stream, message []byte) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, int32(len(message)))
	if err != nil {
		return err
	}
	_, err = buf.Write(message)
	if err != nil {
		return err
	}
	_, err = stream.Write(buf.Bytes())
	return err
}
