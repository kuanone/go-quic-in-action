package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kuanone/go-quic-in-action/pkg/tls"
	"github.com/quic-go/quic-go"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// A wrapper for io.Writer that also logs the message.
type loggingWriter struct{ io.Writer }

func (w loggingWriter) Write(b []byte) (int, error) {
	fmt.Printf("Server: Got '%s'\n", string(b))
	return w.Writer.Write(b)
}

//////////

const (
	addr              = "localhost:4242"
	maxConnections    = 100
	maxStreamsPerConn = 5
	streamBufferSize  = 1024
)

type Server struct {
	shutdownCh  chan struct{}
	listener    *quic.Listener
	connections map[string]quic.Connection
	mu          sync.Mutex
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	bufPool     *BytePool
}

type BytePool struct {
	pool *sync.Pool
	size int
}

func NewBytePool(size int) *BytePool {
	return &BytePool{
		size: size,
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

func (bp *BytePool) Get() []byte {
	return bp.pool.Get().([]byte)[:bp.size]
}

func (bp *BytePool) Put(b []byte) {
	bp.pool.Put(b[:bp.size])
}

func ServerRun() {
	srv := &Server{
		connections: make(map[string]quic.Connection),
		bufPool:     NewBytePool(streamBufferSize),
		shutdownCh:  make(chan struct{}),
	}
	srv.ctx, srv.cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer srv.cancel()

	tlsConfig, err := tls.GenerateTLSConfigWithPkix()
	if err != nil {
		log.Fatalf("Failed to generate TLS config: %v", err)
	}

	srv.listener, err = quic.ListenAddr(addr, tlsConfig, nil)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer srv.listener.Close()

	fmt.Printf("Server is listening on %s\n", addr)

	go srv.acceptConnections()

	<-srv.ctx.Done()
	fmt.Println("Server is shutting down...")
	srv.shutdown()
	fmt.Println("Server shut down gracefully")
}

func (s *Server) acceptConnections() {
	for {

		select {
		case <-s.shutdownCh:
			return
		default:
			connection, err := s.listener.Accept(context.Background())
			if err != nil {
				if errors.Is(err, quic.ErrServerClosed) {
					return
				}

				if s.ctx.Err() != nil {
					break
				}

				log.Printf("Failed to accept connection: %v", err)
				continue
			}

			state := connection.ConnectionState()
			log.Printf("state: %v\n", state)

			s.mu.Lock()
			if len(s.connections) >= maxConnections {
				log.Printf("Too many connections, rejecting: %v", connection.RemoteAddr())
				err := connection.CloseWithError(quic.ApplicationErrorCode(0), "too many connections")
				if err != nil {
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
				continue
			}
			s.connections[connection.RemoteAddr().String()] = connection
			s.mu.Unlock()

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handleConnection(connection)
			}()
		}
	}
}
func isConnClosedError(err error) bool {
	if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
		return true
	}
	if errors.Is(err, quic.ErrServerClosed) {
		return true
	}
	return false
}
func (s *Server) handleConnection(connection quic.Connection) {
	defer func() {
		log.Printf("Closing connection: %v\n", connection.RemoteAddr())
		s.mu.Lock()
		delete(s.connections, connection.RemoteAddr().String())
		s.mu.Unlock()
		select {
		case <-s.shutdownCh:
		default:
			err := connection.CloseWithError(quic.ApplicationErrorCode(0), "connection closed")
			if err != nil {
				log.Printf("Failed to close connection: %v\n", err)
			}
		}
		log.Printf("Connection closed: %v\n", connection.RemoteAddr())
	}()

	streamCount := 0
	for {
		select {
		case <-s.shutdownCh:
			return
		default:
			stream, err := connection.AcceptStream(context.Background())
			if err != nil {
				if ctxErr := connection.Context().Err(); ctxErr != nil {
					log.Printf("connection context error: %v\n", ctxErr)
					return
				}
				log.Printf("Failed to accept stream: %v\n", err)
				return
			}

			streamCount++
			if streamCount > maxStreamsPerConn {
				log.Printf("Too many streams for connection: %v\n", connection.RemoteAddr())
				err := stream.Close()
				if err != nil {
					log.Printf("Failed to close stream: %v\n", err)
					break
				}
				continue
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handleStream(connection.Context(), stream)
			}()
		}
	}
}

func (s *Server) readMessage(stream quic.Stream) ([]byte, error) {
	buf := s.bufPool.Get()
	defer s.bufPool.Put(buf)

	var length int32
	err := binary.Read(stream, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	//buf := make([]byte, length)
	if int(length) > cap(buf) {
		return nil, fmt.Errorf("message length exceeds buffer size")
	}

	buf = buf[:length]

	_, err = io.ReadFull(stream, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (s *Server) writeMessage(stream quic.Stream, message []byte) error {
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

func (s *Server) handleStream(ctx context.Context, stream quic.Stream) {
	defer func(stream quic.Stream) {
		err := stream.Close()
		if err != nil {
			log.Printf("Failed to close stream: %v\n", err)
		}
		log.Printf("Stream closed: %v\n", stream.StreamID())
	}(stream)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stream context done:", ctx.Err())
			return
		default:
			err := stream.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				log.Printf("Failed to set read deadline: %v\n", err)
				return
			}
			buf, err := s.readMessage(stream)
			if err != nil {
				if isTimeoutError(err) {
					log.Printf("Stream read timeout: %v\n", err)
					continue
				}
				if err != io.EOF {
					log.Printf("Failed to read from stream: %v\n", err)
				}
				return
			}

			message := buf
			fmt.Printf("Received ping: %s\n", message)

			// Create a channel to receive the processed data
			resultChan := make(chan []byte)
			errChan := make(chan error)

			// Process the message in a separate goroutine
			go s.processMessage(message, resultChan, errChan)

			// Wait for the processed result or an error
			select {
			case result := <-resultChan:
				//_, err = stream.Write(result)
				err = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err != nil {
					log.Printf("Failed to set write deadline: %v\n", err)
					return
				}
				err = s.writeMessage(stream, result)
				if err != nil {
					log.Printf("Failed to write to stream: %v", err)
					return
				}
				fmt.Printf("Sent pong: %s\n", string(result))
			case err := <-errChan:
				log.Printf("Failed to process message: %v", err)
				return
			case <-ctx.Done():
				log.Println("Stream context done:", ctx.Err())
				return
			}
		}
	}
}

func (s *Server) processMessage(message []byte, resultChan chan []byte, errChan chan error) {
	// Simulate business logic processing time
	time.Sleep(100 * time.Millisecond)

	// Process the message (ping -> pong)
	result := []byte("pong: " + string(message))
	select {
	case resultChan <- result:
	case <-time.After(5 * time.Second):
		errChan <- fmt.Errorf("business logic timeout")
	}
}

func isTimeoutError(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func (s *Server) shutdown() {
	close(s.shutdownCh)
	defer func() {
		err := s.listener.Close()
		if err != nil {
			log.Printf("Failed to close listener: %v\n", err)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, connection := range s.connections {
		err := connection.CloseWithError(quic.ApplicationErrorCode(0x1001), "server shutting down")
		if err != nil {
			log.Printf("Failed to close connection: %v\n", err)
		}
	}

	select {
	case <-s.ctx.Done():
	default:
		s.wg.Wait()
	}
}
