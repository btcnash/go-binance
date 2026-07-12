package private

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/btcnash/go-binance/v2/futures"
)

var privateRoots = map[Environment]string{
	EnvironmentMainnet: "wss://fstream.binance.com",
	EnvironmentTestnet: "wss://stream.binancefuture.com",
	EnvironmentDemo:    "wss://fstream.binancefuture.com",
}

type gorillaEndpointDialer struct{}

func (gorillaEndpointDialer) Dial(ctx context.Context, endpoint string) (managedws.Socket, error) {
	return (managedgorilla.Dialer{Endpoint: endpoint}).Dial(ctx)
}

type sourceRuntime struct {
	spec Source

	opMu    sync.Mutex
	mu      sync.Mutex
	key     string
	version uint64
	changed chan struct{}

	emit func(ListenKeyEvent)
}

func newSourceRuntime(source Source, emit func(ListenKeyEvent)) *sourceRuntime {
	return &sourceRuntime{spec: source, changed: make(chan struct{}), emit: emit}
}

func (s *sourceRuntime) current() (string, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.key, s.version
}

func (s *sourceRuntime) ensure(ctx context.Context) (string, uint64, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	key, version := s.current()
	if key != "" {
		return key, version, nil
	}
	key, err := s.spec.Provider.Acquire(ctx)
	if err != nil {
		wrapped := privateError(ErrorAcquire, s.spec.ID, 0, "acquire", fmt.Errorf("%w: %v", ErrListenKeyAcquire, err))
		s.emitEvent(ListenKeyEvent{Kind: ListenKeyAcquireFailed, SourceID: s.spec.ID, Version: version, At: time.Now(), Err: wrapped})
		return "", 0, wrapped
	}
	key = strings.TrimSpace(key)
	if key == "" {
		wrapped := privateError(ErrorAcquire, s.spec.ID, 0, "acquire", fmt.Errorf("%w: provider returned empty key", ErrListenKeyAcquire))
		s.emitEvent(ListenKeyEvent{Kind: ListenKeyAcquireFailed, SourceID: s.spec.ID, Version: version, At: time.Now(), Err: wrapped})
		return "", 0, wrapped
	}
	s.mu.Lock()
	s.key = key
	s.version++
	version = s.version
	oldChanged := s.changed
	s.changed = make(chan struct{})
	close(oldChanged)
	s.mu.Unlock()
	s.emitEvent(ListenKeyEvent{Kind: ListenKeyAcquired, SourceID: s.spec.ID, Version: version, At: time.Now()})
	return key, version, nil
}

func (s *sourceRuntime) invalidate(err error) (string, uint64, bool) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.key == "" {
		version := s.version
		s.mu.Unlock()
		return "", version, false
	}
	old := s.key
	s.key = ""
	s.version++
	version := s.version
	oldChanged := s.changed
	s.changed = make(chan struct{})
	close(oldChanged)
	s.mu.Unlock()
	s.emitEvent(ListenKeyEvent{Kind: ListenKeyInvalidated, SourceID: s.spec.ID, Version: version, At: time.Now(), Err: err})
	return old, version, true
}

func (s *sourceRuntime) waitForKey(ctx context.Context) (string, uint64, error) {
	for {
		s.mu.Lock()
		key, version, changed := s.key, s.version, s.changed
		s.mu.Unlock()
		if key != "" {
			return key, version, nil
		}
		select {
		case <-ctx.Done():
			return "", version, ctx.Err()
		case <-changed:
		}
	}
}

func (s *sourceRuntime) keepAlive(ctx context.Context, key string) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	current, _ := s.current()
	if current == "" || current != key {
		return ErrListenKeyExpired
	}
	return s.spec.Provider.KeepAlive(ctx, key)
}

func (s *sourceRuntime) release(ctx context.Context) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	key := s.key
	version := s.version
	s.key = ""
	if key != "" {
		s.version++
		version = s.version
		oldChanged := s.changed
		s.changed = make(chan struct{})
		close(oldChanged)
	}
	s.mu.Unlock()
	if key == "" {
		return nil
	}
	err := s.spec.Provider.Release(ctx, key)
	kind := ListenKeyReleased
	if err != nil {
		kind = ListenKeyReleaseFailed
	}
	s.emitEvent(ListenKeyEvent{Kind: kind, SourceID: s.spec.ID, Version: version, At: time.Now(), Err: err})
	return err
}

func (s *sourceRuntime) emitEvent(event ListenKeyEvent) {
	if s.emit != nil {
		s.emit(event)
	}
}

type sourceBinding struct {
	SourceID  string
	ListenKey string
	Version   uint64
	Events    []futures.UserDataEventType
	eventSet  map[futures.UserDataEventType]struct{}
}

func (b sourceBinding) accepts(event futures.UserDataEventType) bool {
	if len(b.eventSet) == 0 {
		return true
	}
	_, ok := b.eventSet[event]
	return ok
}

type bindingSnapshot struct {
	sourceIDs       []string
	listenKeySource map[string]string
	listenKeys      []string
	eventCandidates map[futures.UserDataEventType][]string
	wildcardSources []string
	isolatedSource  string
}

func newBindingSnapshot(bindings []sourceBinding) *bindingSnapshot {
	snapshot := &bindingSnapshot{
		sourceIDs:       make([]string, 0, len(bindings)),
		listenKeySource: make(map[string]string, len(bindings)),
		listenKeys:      make([]string, 0, len(bindings)),
		eventCandidates: make(map[futures.UserDataEventType][]string),
	}
	for _, binding := range bindings {
		snapshot.sourceIDs = append(snapshot.sourceIDs, binding.SourceID)
		if binding.ListenKey != "" {
			// Preserve the legacy resolver contract: when duplicate listen keys
			// are supplied, the first binding wins.
			if _, exists := snapshot.listenKeySource[binding.ListenKey]; !exists {
				snapshot.listenKeySource[binding.ListenKey] = binding.SourceID
				snapshot.listenKeys = append(snapshot.listenKeys, binding.ListenKey)
			}
		}
		eventSet := binding.eventSet
		if len(eventSet) == 0 && len(binding.Events) > 0 {
			eventSet = make(map[futures.UserDataEventType]struct{}, len(binding.Events))
			for _, event := range binding.Events {
				eventSet[event] = struct{}{}
			}
		}
		if len(eventSet) == 0 {
			snapshot.wildcardSources = append(snapshot.wildcardSources, binding.SourceID)
			continue
		}
		for event := range eventSet {
			snapshot.eventCandidates[event] = append(snapshot.eventCandidates[event], binding.SourceID)
		}
	}
	sort.Strings(snapshot.sourceIDs)
	sort.Strings(snapshot.wildcardSources)
	for event, candidates := range snapshot.eventCandidates {
		candidates = append(candidates, snapshot.wildcardSources...)
		sort.Strings(candidates)
		snapshot.eventCandidates[event] = candidates
	}
	if len(snapshot.sourceIDs) == 1 {
		snapshot.isolatedSource = snapshot.sourceIDs[0]
	}
	return snapshot
}

func (s *bindingSnapshot) resolve(event futures.UserDataEventType, explicitKey, stream string) (string, []string, SourceResolution) {
	if s == nil {
		return "", nil, SourceResolutionUnmatched
	}
	explicitKey = strings.TrimSpace(explicitKey)
	if sourceID := s.listenKeySource[explicitKey]; explicitKey != "" && sourceID != "" {
		return sourceID, nil, SourceResolutionExplicit
	}
	if stream != "" {
		if sourceID := s.listenKeySource[stream]; sourceID != "" {
			return sourceID, nil, SourceResolutionExplicit
		}
		for _, key := range s.listenKeys {
			if strings.Contains(stream, "listenKey="+url.QueryEscape(key)) {
				return s.listenKeySource[key], nil, SourceResolutionExplicit
			}
		}
	}
	if s.isolatedSource != "" {
		return s.isolatedSource, nil, SourceResolutionIsolated
	}
	candidates := s.eventCandidates[event]
	if len(candidates) == 0 {
		candidates = s.wildcardSources
	}
	if len(candidates) == 1 {
		return candidates[0], nil, SourceResolutionEventFilter
	}
	if len(candidates) == 0 {
		return "", append([]string(nil), s.sourceIDs...), SourceResolutionUnmatched
	}
	return "", append([]string(nil), candidates...), SourceResolutionAmbiguous
}

func (s *bindingSnapshot) allSourceIDs() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.sourceIDs...)
}

type dialSnapshot struct {
	Endpoint string
	Bindings []sourceBinding
}

type privateDialer struct {
	mode             Mode
	root             string
	endpointDialer   EndpointDialer
	invalidDialError func(error) bool
	sources          []*sourceRuntime

	mu       sync.RWMutex
	snapshot dialSnapshot
}

func (d *privateDialer) Dial(ctx context.Context) (managedws.Socket, error) {
	bindings := make([]sourceBinding, 0, len(d.sources))
	for _, source := range d.sources {
		key, version, err := source.ensure(ctx)
		if err != nil {
			return nil, err
		}
		eventSet := make(map[futures.UserDataEventType]struct{}, len(source.spec.Events))
		events := append([]futures.UserDataEventType(nil), source.spec.Events...)
		for _, event := range events {
			eventSet[event] = struct{}{}
		}
		bindings = append(bindings, sourceBinding{
			SourceID:  source.spec.ID,
			ListenKey: key,
			Version:   version,
			Events:    events,
			eventSet:  eventSet,
		})
	}
	endpoint, err := buildPrivateEndpoint(d.root, d.mode, bindings)
	if err != nil {
		return nil, err
	}
	socket, err := d.endpointDialer.Dial(ctx, endpoint)
	if err != nil {
		if d.invalidDialError != nil && d.invalidDialError(err) {
			wrapped := privateError(ErrorExpired, "", 0, "handshake", fmt.Errorf("%w: %v", ErrListenKeyExpired, err))
			for _, source := range d.sources {
				source.invalidate(wrapped)
			}
		}
		return nil, err
	}
	d.mu.Lock()
	d.snapshot = dialSnapshot{Endpoint: endpoint, Bindings: cloneBindings(bindings)}
	d.mu.Unlock()
	return socket, nil
}

func (d *privateDialer) Snapshot() dialSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return dialSnapshot{Endpoint: d.snapshot.Endpoint, Bindings: cloneBindings(d.snapshot.Bindings)}
}

func cloneBindings(in []sourceBinding) []sourceBinding {
	out := make([]sourceBinding, len(in))
	for i, binding := range in {
		out[i] = binding
		out[i].Events = append([]futures.UserDataEventType(nil), binding.Events...)
		out[i].eventSet = make(map[futures.UserDataEventType]struct{}, len(binding.eventSet))
		for event := range binding.eventSet {
			out[i].eventSet[event] = struct{}{}
		}
	}
	return out
}

func buildPrivateEndpoint(root string, mode Mode, bindings []sourceBinding) (string, error) {
	root = strings.TrimRight(strings.TrimSpace(root), "/")
	if root == "" {
		return "", invalidOption("endpoint root is required")
	}
	if strings.HasSuffix(root, "/private") {
		// Already routed.
	} else {
		root += "/private"
	}
	path := "/ws"
	if mode == ModeShared {
		path = "/stream"
	}
	var query strings.Builder
	for i, binding := range bindings {
		if i > 0 {
			query.WriteByte('&')
		}
		query.WriteString("listenKey=")
		query.WriteString(url.QueryEscape(binding.ListenKey))
		if len(binding.Events) > 0 {
			query.WriteString("&events=")
			names := make([]string, 0, len(binding.Events))
			for _, event := range binding.Events {
				names = append(names, string(event))
			}
			query.WriteString(url.QueryEscape(strings.Join(names, "/")))
		}
	}
	if query.Len() == 0 {
		return "", invalidOption("at least one listen key is required")
	}
	return root + path + "?" + strings.ReplaceAll(query.String(), "%2F", "/"), nil
}

func sortedSourceIDs(bindings []sourceBinding) []string {
	ids := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		ids = append(ids, binding.SourceID)
	}
	sort.Strings(ids)
	return ids
}

func defaultInvalidListenKeyDialError(err error) bool {
	var handshakeErr *managedgorilla.HandshakeError
	if !errors.As(err, &handshakeErr) {
		return false
	}
	switch handshakeErr.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return true
	default:
		return false
	}
}

var _ managedws.Dialer = (*privateDialer)(nil)
