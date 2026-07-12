package managed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type writeRequest struct {
	ctx         context.Context
	messageType int
	payload     []byte
	control     bool
	deadline    time.Time
	result      chan error
}

type pongReceipt struct {
	payload string
	at      time.Time
}

type physicalSession struct {
	// Keep 64-bit atomics first for correct alignment on 32-bit platforms.
	sequence uint64

	owner        *Connection
	generation   uint64
	socket       Socket
	ctx          context.Context
	cancel       context.CancelFunc
	writeQueue   chan writeRequest
	controlQueue chan writeRequest
	pongC        chan pongReceipt
	failureC     chan error
	failOnce     sync.Once
	wg           sync.WaitGroup
	startedAt    time.Time
}

func newPhysicalSession(owner *Connection, parent context.Context, generation uint64, socket Socket) *physicalSession {
	ctx, cancel := context.WithCancel(parent)
	s := &physicalSession{
		owner:        owner,
		generation:   generation,
		socket:       socket,
		ctx:          ctx,
		cancel:       cancel,
		writeQueue:   make(chan writeRequest, owner.opts.WriteQueue),
		controlQueue: make(chan writeRequest, 16),
		pongC:        make(chan pongReceipt, 16),
		failureC:     make(chan error, 1),
		startedAt:    time.Now(),
	}
	s.installControlHandlers()
	return s
}

func (s *physicalSession) installControlHandlers() {
	s.socket.SetPingHandler(func(payload string) error {
		if s.ctx.Err() != nil || !s.owner.isCurrentGeneration(s.generation) {
			return ErrNotReady
		}
		s.owner.emitHeartbeat(HeartbeatEvent{
			Kind:       HeartbeatServerPing,
			Generation: s.generation,
			Payload:    payload,
			At:         time.Now(),
		})

		ctx, cancel := context.WithTimeout(s.ctx, s.owner.opts.Heartbeat.WriteTimeout)
		defer cancel()
		err := s.writeControl(ctx, PongMessage, []byte(payload), s.owner.opts.Heartbeat.WriteTimeout)
		if err != nil {
			wrapped := connectionError(ErrorPongWrite, s.generation, "server_pong", err)
			s.fail(wrapped)
			return wrapped
		}
		s.owner.emitHeartbeat(HeartbeatEvent{
			Kind:       HeartbeatServerPongSent,
			Generation: s.generation,
			Payload:    payload,
			At:         time.Now(),
		})
		return nil
	})

	s.socket.SetPongHandler(func(payload string) error {
		if s.ctx.Err() != nil || !s.owner.isCurrentGeneration(s.generation) {
			return nil
		}
		receipt := pongReceipt{payload: payload, at: time.Now()}
		select {
		case s.pongC <- receipt:
		default:
			// Stale/unsolicited pong frames are not allowed to block the reader.
		}
		return nil
	})
}

func (s *physicalSession) start() {
	s.wg.Add(2)
	go s.writerLoop()
	go s.readerLoop()
	if s.owner.opts.Heartbeat.Enabled {
		s.wg.Add(1)
		go s.heartbeatLoop()
	}
	if s.owner.opts.MaxConnectionAge > 0 {
		s.wg.Add(1)
		go s.maxAgeLoop()
	}
}

func (s *physicalSession) maxAgeLoop() {
	defer s.wg.Done()
	timer := time.NewTimer(s.owner.opts.MaxConnectionAge)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
		s.fail(connectionError(ErrorMaxAgeReached, s.generation, "max_connection_age", fmt.Errorf("physical connection reached max age %s", s.owner.opts.MaxConnectionAge)))
	}
}

func (s *physicalSession) stop() {
	s.cancel()
	_ = s.socket.Close()
	s.wg.Wait()
}

func (s *physicalSession) fail(err error) {
	s.failOnce.Do(func() {
		select {
		case s.failureC <- err:
		default:
		}
		s.cancel()
		_ = s.socket.Close()
	})
}

func (s *physicalSession) writerLoop() {
	defer s.wg.Done()
	for {
		var req writeRequest
		select {
		case <-s.ctx.Done():
			return
		case req = <-s.controlQueue:
		default:
			select {
			case <-s.ctx.Done():
				return
			case req = <-s.controlQueue:
			case req = <-s.writeQueue:
			}
		}

		if req.ctx != nil {
			if err := req.ctx.Err(); err != nil {
				select {
				case req.result <- err:
				default:
				}
				continue
			}
		}

		err := s.performWrite(req)
		select {
		case req.result <- err:
		default:
		}
		if err != nil {
			kind := ErrorWrite
			op := "write_message"
			if req.control && req.messageType == PingMessage {
				kind = ErrorPingWrite
				op = "ping"
			} else if req.control && req.messageType == PongMessage {
				kind = ErrorPongWrite
				op = "pong"
			}
			s.fail(connectionError(kind, s.generation, op, err))
			return
		}
	}
}

func (s *physicalSession) performWrite(req writeRequest) error {
	if req.control {
		return s.socket.WriteControl(req.messageType, req.payload, req.deadline)
	}
	if setter, ok := s.socket.(interface{ SetWriteDeadline(time.Time) error }); ok {
		if err := setter.SetWriteDeadline(req.deadline); err != nil {
			return err
		}
	}
	return s.socket.WriteMessage(req.messageType, req.payload)
}

func (s *physicalSession) enqueueWrite(ctx context.Context, req writeRequest) error {
	queue := s.writeQueue
	if req.control {
		queue = s.controlQueue
	}
	select {
	case <-s.ctx.Done():
		return ErrNotReady
	case <-ctx.Done():
		return ctx.Err()
	case queue <- req:
	}

	select {
	case <-s.ctx.Done():
		select {
		case err := <-req.result:
			return err
		default:
			return ErrNotReady
		}
	case <-ctx.Done():
		return ctx.Err()
	case err := <-req.result:
		return err
	}
}

func (s *physicalSession) writeText(ctx context.Context, payload []byte) error {
	deadline := time.Now().Add(s.owner.opts.Heartbeat.WriteTimeout)
	return s.enqueueWrite(ctx, writeRequest{
		ctx:         ctx,
		messageType: TextMessage,
		payload:     append([]byte(nil), payload...),
		deadline:    deadline,
		result:      make(chan error, 1),
	})
}

func (s *physicalSession) writeControl(ctx context.Context, messageType int, payload []byte, timeout time.Duration) error {
	return s.enqueueWrite(ctx, writeRequest{
		ctx:         ctx,
		messageType: messageType,
		payload:     append([]byte(nil), payload...),
		control:     true,
		deadline:    time.Now().Add(timeout),
		result:      make(chan error, 1),
	})
}

func (s *physicalSession) readerLoop() {
	defer s.wg.Done()
	for {
		messageType, payload, err := s.socket.ReadMessage()
		if err != nil {
			if s.ctx.Err() == nil {
				s.fail(connectionError(ErrorRead, s.generation, "read", err))
			}
			return
		}
		if !s.owner.isCurrentGeneration(s.generation) {
			continue
		}
		frame := Frame{
			Generation: s.generation,
			Type:       messageType,
			Payload:    append([]byte(nil), payload...),
			ReceivedAt: time.Now(),
		}
		select {
		case s.owner.frames <- frame:
		default:
			s.fail(connectionError(
				ErrorFrameBufferFull,
				s.generation,
				"dispatch_frame",
				errors.New("application frame buffer is full"),
			))
			return
		}
	}
}

func (s *physicalSession) heartbeatLoop() {
	defer s.wg.Done()
	timer := time.NewTimer(s.owner.opts.Heartbeat.PingInterval)
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}

		sequence := atomic.AddUint64(&s.sequence, 1)
		payload := fmt.Sprintf("sdk:%d:%d:%d", s.generation, sequence, time.Now().UnixNano())
		sentAt := time.Now()
		ctx, cancel := context.WithTimeout(s.ctx, s.owner.opts.Heartbeat.WriteTimeout)
		err := s.writeControl(ctx, PingMessage, []byte(payload), s.owner.opts.Heartbeat.WriteTimeout)
		cancel()
		if err != nil {
			if s.ctx.Err() == nil {
				s.fail(connectionError(ErrorPingWrite, s.generation, "ping", err))
			}
			return
		}
		s.owner.emitHeartbeat(HeartbeatEvent{
			Kind:       HeartbeatPingSent,
			Generation: s.generation,
			Payload:    payload,
			At:         sentAt,
		})

		pongTimer := time.NewTimer(s.owner.opts.Heartbeat.PongTimeout)
		matched := false
		for !matched {
			select {
			case <-s.ctx.Done():
				if !pongTimer.Stop() {
					select {
					case <-pongTimer.C:
					default:
					}
				}
				return
			case receipt := <-s.pongC:
				if receipt.payload != payload {
					continue
				}
				if !pongTimer.Stop() {
					select {
					case <-pongTimer.C:
					default:
					}
				}
				matched = true
				s.owner.emitHeartbeat(HeartbeatEvent{
					Kind:       HeartbeatPongReceived,
					Generation: s.generation,
					Payload:    payload,
					RTT:        receipt.at.Sub(sentAt),
					At:         receipt.at,
				})
			case <-pongTimer.C:
				s.fail(connectionError(
					ErrorPongTimeout,
					s.generation,
					"wait_pong",
					fmt.Errorf("pong not received within %s", s.owner.opts.Heartbeat.PongTimeout),
				))
				return
			}
		}

		timer.Reset(s.owner.opts.Heartbeat.PingInterval)
	}
}
