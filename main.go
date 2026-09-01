// Package main is the WASM "owner" for the chat app.
//
// It runs the interlude LLM-chat library inside Go wasm and streams the events
// of a streaming "burst" (thinking -> tokens -> completion_ended) to
// JavaScript via a JS callback.
//
// What the owner does:
//   - Runs one streaming "burst" per chat turn: the interlude chat session is
//     executed once and its events (thinking -> tokens -> completion_ended)
//     are streamed to JavaScript one at a time.
//   - A burst is driven by a provider client: either an HTTP-free echo client
//     or a real OpenAI-compatible provider over genuine HTTP+SSE.
//   - For multi-turn dialogues it keeps a per-dialogue, id-keyed parse-cache:
//     each message id is parsed exactly once, and a follow-up turn pulls only
//     the ids not yet cached, in ONE batched call through the owner-local JS
//     `getMessages` bridge (never one call per id).
//
// FFI notes (Go 1.26, GOOS=js GOARCH=wasm):
//
//   - Exports use the //go:wasmexport pragma (the old //export is ignored).
//
//   - The wasmexport ABI does NOT support string result types (string is only
//     permitted as a parameter, passed as (ptr,len) into linear memory), nor
//     interface{} parameters. So:
//
//   - the JS event callback is registered as a JS global (globalThis.__cyrCallback)
//     and retrieved here as a js.Value;
//
//   - payloads (opts JSON) are read from JS globals set by JS before the
//     call (burst opts at globalThis.__cyrOpts; per-dialogue payload at
//     globalThis.__cyrPayload);
//
//   - string RESULTS are published to a JS global (globalThis.__cyrDump);
//
//   - startBurst is VOID: the caller (owner-worker.js) ignores any return
//     and uses the server's generation id; all of its JS crossing happens in
//     a spawned goroutine, NEVER in the export body (a valueGet/Invoke in a
//     value-returning wasmexport wrapper corrupts the wrapper's fixed
//     return frame — the "'5' loadValue" crash).
//
//   - syscall/js has no js.Func in 1.26; we store a js.Value and call .Invoke().
//
//   - Resume-kick: after calling a `start*` export that spawns goroutines you
//     must call the runtime `resume` export to pump the spawned burst
//     goroutines (one kick per burst).
//
//   - The owner-local JS message bridge is registered as a JS global
//     (globalThis.__cyrGetMessages) and retrieved here as a js.Value:
//
//     getMessages(dialogueID string, ids string[]) -> { [id]: rawEventJSON, ... }
//
//   - batched: ALL missing ids in ONE call (never one call per id);
//
//   - missing-id: ids with no content are omitted from the returned object;
//     the wasm side skips them and records a warning;
//
//   - synchronous in the current implementation (the JS stub answers inline,
//     no callbacks).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	intChat "github.com/x2d7/interlude/chat"
	openaiConnect "github.com/x2d7/interlude/connect/openai"
	_ "github.com/x2d7/interlude/connect/openai/config" // registers "openai" in provider.DefaultRegistry via init()
	provider "github.com/x2d7/interlude/provider"
	"syscall/js"
)

// ---------------------------------------------------------------------------
// Echo streaming Client
// ---------------------------------------------------------------------------

// defaultTokenDelay is the built-in burst pacing used when the model does
// not configure a token delay.
const defaultTokenDelay = 30 * time.Millisecond

// echoStream is a Stream[StreamEvent] that yields a fixed, clearly-ordered
// sequence of events, sleeping (per-model tokenDelay, else
// defaultTokenDelay) before each one so the stream is unmistakably
// incremental. It mirrors a real provider stream: Next advances (and yields
// to the event loop), Current returns the last advanced element.
type echoStream struct {
	mu         sync.Mutex
	tokenDelay time.Duration // per-model pacing; <= 0 → defaultTokenDelay
	events     []intChat.StreamEvent
	idx        int
	closed     bool
}

func (s *echoStream) Next(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.idx >= len(s.events) {
		return false
	}
	// Yield to the wasm event loop so events are delivered incrementally,
	// and honor cancellation so stop() is effective. Pacing: the model's
	// configured token delay when set, else the built-in default.
	delay := s.tokenDelay
	if delay <= 0 {
		delay = defaultTokenDelay
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return false
	}
	s.idx++
	return true
}

func (s *echoStream) Current() intChat.StreamEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx == 0 || s.idx > len(s.events) {
		return nil
	}
	return s.events[s.idx-1]
}

func (s *echoStream) Err() error { return nil }

func (s *echoStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// echoClient is a minimal interlude Client. NewStreaming returns an echoStream
// that yields 3 thinking tokens, 5 distinct text tokens, then completion_ended.
// tokenDelay paces the burst to the model's configured token delay; a zero
// value keeps the built-in defaultTokenDelay.
type echoClient struct {
	tokenDelay time.Duration
}

func (c echoClient) NewStreaming(ctx context.Context) intChat.Stream[intChat.StreamEvent] {
	return &echoStream{
		tokenDelay: c.tokenDelay,
		events: []intChat.StreamEvent{
			intChat.NewEventThinking("thinking: a"),
			intChat.NewEventThinking("thinking: b"),
			intChat.NewEventThinking("thinking: c"),
			intChat.NewEventToken("Hello"),
			intChat.NewEventToken(" world"),
			intChat.NewEventToken("! This"),
			intChat.NewEventToken(" is"),
			intChat.NewEventToken(" the echo."),
			intChat.NewEventCompletionEnded(nil),
		},
	}
}

// SyncInput is a no-op: the echo client is stateless, so it returns itself.
func (c echoClient) SyncInput(chat *intChat.Chat) intChat.Client { return c }

// ---------------------------------------------------------------------------
// FFI state (single-threaded wasm)
// ---------------------------------------------------------------------------

var (
	mu        sync.Mutex
	cb        js.Value           // JS event callback function (globalThis.__cyrCallback)
	cancel    context.CancelFunc // cancel func for the active startBurst burst
	cancelGen uint32             // genSeq of the burst that set `cancel` (0 = none)
	genSeq    uint32             // startBurst generation sequence counter
)

// emit sends one event (already mapped to JSON) to the stored JS callback.
func emit(evJSON string) {
	mu.Lock()
	v := cb
	mu.Unlock()
	if v.IsNull() || v.IsUndefined() {
		return
	}
	v.Invoke(evJSON)
}

// mapEvent converts an interlude StreamEvent into (type, payload) using the
// frontend's event type names.
func mapEvent(ev intChat.StreamEvent) (string, any) {
	switch e := ev.(type) {
	case intChat.EventThinking:
		return "thinking", map[string]any{"text": e.Content}
	case intChat.EventToken:
		return "token", map[string]any{"text": e.Content}
	case intChat.EventToolCall:
		return "tool_call", map[string]any{"text": e.Content, "call_id": e.CallID, "name": e.Name}
	case intChat.EventCompletionEnded:
		return "completion_ended", map[string]any{"tool_calls": e.ToolCalls}
	case intChat.EventError:
		msg := ""
		if e.Error != nil {
			msg = e.Error.Error()
		}
		return "error", map[string]any{"error": msg}
	// Best-effort mapping for the other (full-message) events the session emits.
	case intChat.EventCompletionStart:
		return "completion_start", map[string]any{}
	case intChat.EventReasoningMessage:
		return "reasoning_message", map[string]any{"text": e.Content}
	case intChat.EventAssistantMessage:
		return "assistant_message", map[string]any{"text": e.Content}
	case intChat.EventUserMessage:
		return "user_message", map[string]any{"text": e.Content}
	case intChat.EventSystemMessage:
		return "system_message", map[string]any{"text": e.Content}
	case intChat.EventToolCallToken:
		return "tool_call_token", map[string]any{"text": e.Content, "call_id": e.CallID, "name": e.Name}
	case intChat.EventToolCallResolved:
		return "tool_call_resolved", map[string]any{"call_id": e.CallID, "accepted": e.Accepted}
	case intChat.EventToolMessage:
		return "tool_message", map[string]any{"text": e.Content, "call_id": e.CallID, "success": e.Success}
	case intChat.EventRefusal:
		return "refusal", map[string]any{"text": e.Content}
	default:
		return "unknown", map[string]any{}
	}
}

func eventToJSON(ev intChat.StreamEvent) string {
	t, p := mapEvent(ev)
	b, _ := json.Marshal(map[string]any{"type": t, "payload": p})
	return string(b)
}

// ---------------------------------------------------------------------------
// Per-dialogue handles + id-keyed parse cache + batched lazy pull
// ---------------------------------------------------------------------------

// handle is a per-DIALOGUE owner-side conversation: the accumulated Messages
// (the dialogue history) live for the whole dialogue. ctx/cancel are NOT here:
// they are per-BURST (fresh on every Session), otherwise cancelling one burst
// would kill the handle's others.
type handle struct {
	chat        *intChat.Chat      // {Messages, ToolPolicy: ExitAfter} — lives for the dialogue
	cli         intChat.Client     // the burst client (echo or openai)
	dialogueID  string             // the dialogue this handle belongs to
	busy        bool               // one burst at a time per handle
	burstCancel context.CancelFunc // cancel of the ACTIVE burst (nil when idle)
	// Pending burst input, resolved (id-walk + batched pull) INSIDE the
	// consumer goroutine — never inside the export (the gojs Invoke bridge
	// corrupts the wasmexport wrapper's return frame / f-loop state).
	pendingIDs      []string
	pendingUserText string
	addHits         bool
}

var (
	handles    = map[uint32]*handle{}
	nextHandle uint32
	// parseCache: dialogueID -> messageID -> parsed event (parsed exactly once).
	parseCache = map[string]map[string]intChat.StreamEvent{}
)

// dialoguePayload is the JSON payload for goStart/goSend, read from
// globalThis.__cyrPayload (set by JS before the call — the wasmexport ABI
// cannot take a string parameter cleanly, so the payload is delivered through
// the same JS-global mechanism startBurst uses for its opts).
type dialoguePayload struct {
	DialogueID string         `json:"dialogue_id"`
	IDs        []string       `json:"ids"`       // ordered message id-list (branch = single source of truth)
	UserText   string         `json:"user_text"` // new user turn appended after the id-list
	Config     dialogueConfig `json:"config"`    // burst client selection (echo by default)
	Tools      []byte         `json:"tools"`     // reserved: base64 tool stubs (not populated yet)
}

type dialogueConfig struct {
	UseOpenAI bool   `json:"useOpenAI"`
	Endpoint  string `json:"endpoint"`
	APIKey    string `json:"apiKey"`
	Model     string `json:"model"`
}

// newClient selects the burst client from the payload config (mirroring the
// startBurst client selection): useOpenAI -> a real OpenAI-compatible endpoint,
// otherwise the echo client (default; no HTTP needed).
func newClient(cfg dialogueConfig) intChat.Client {
	if cfg.UseOpenAI {
		return &openaiConnect.OpenAIClient{
			Endpoint: cfg.Endpoint,
			APIKey:   cfg.APIKey,
			Model:    cfg.Model,
		}
	}
	return echoClient{}
}

// pullMissing calls the owner-local JS getMessages bridge ONCE with ALL misses
// (batched — never one call per id).
//
// Contract (the bridge is registered by JS as globalThis.__cyrGetMessages):
//
//		getMessages(dialogueID string, ids string[]) -> { [id]: rawEventJSON, ... }
//
//	  - batched: every missed id in this ONE call;
//	  - missing-id: ids with no content are omitted from the returned object;
//	    this function records a warning for each and leaves it out of the result;
//	  - synchronous in the current implementation (the JS stub answers inline).
func pullMissing(dialogueID string, misses []string) map[string]string {
	fn := js.Global().Get("__cyrGetMessages")
	if t := fn.Type(); t != js.TypeFunction {
		fmt.Fprintf(os.Stderr, "owner: __cyrGetMessages not registered (got %s); %d misses skipped (dialogue %s)\n", t, len(misses), dialogueID)
		return nil
	}
	idsAny := make([]any, len(misses))
	for i, id := range misses {
		idsAny[i] = id
	}
	ret := fn.Invoke(dialogueID, idsAny)
	if t := ret.Type(); t != js.TypeObject && t != js.TypeFunction {
		fmt.Fprintf(os.Stderr, "owner: getMessages returned non-object (%s); %d misses skipped (dialogue %s)\n", t, len(misses), dialogueID)
		return nil
	}
	out := make(map[string]string, len(misses))
	for _, id := range misses {
		v := ret.Get(id)
		if v.IsUndefined() || v.IsNull() {
			// missing-id: no content for this id — skip + warn.
			fmt.Fprintf(os.Stderr, "owner: WARNING: getMessages: no content for id %q (dialogue %s); skipped\n", id, dialogueID)
			continue
		}
		out[id] = v.String()
	}
	return out
}

// resolveIDs walks the id-list IN ORDER against the per-dialogue parse cache.
// Cache hits are NOT re-parsed and NOT re-pulled. Misses are pulled in ONE
// batched getMessages call, each raw event is parsed EXACTLY ONCE, stored in
// the cache, and returned.
//
//   - addHits=true (goStart, fresh Messages): the returned slice is the whole
//     id-list history in id-list order (cached events + newly pulled);
//   - addHits=false (goSend, existing Messages): the returned slice contains
//     ONLY the newly pulled misses, in id-list order (cached hits are already
//     in the handle's Messages — adding them again would duplicate history).
func resolveIDs(dialogueID string, ids []string, addHits bool) []intChat.StreamEvent {
	mu.Lock()
	cache, ok := parseCache[dialogueID]
	if !ok {
		cache = map[string]intChat.StreamEvent{}
		parseCache[dialogueID] = cache
	}
	byID := make(map[string]intChat.StreamEvent, len(ids))
	var misses []string
	for _, id := range ids {
		if ev, hit := cache[id]; hit {
			byID[id] = ev
		} else {
			misses = append(misses, id)
		}
	}
	mu.Unlock()

	if len(misses) > 0 {
		rawByID := pullMissing(dialogueID, misses) // ONE batched call for ALL misses
		if rawByID != nil {
			mu.Lock()
			for _, id := range misses {
				raw, ok := rawByID[id]
				if !ok {
					continue // missing-id: pullMissing already warned; skip
				}
				ev, err := intChat.UnmarshalEvent([]byte(raw)) // parse exactly once
				if err != nil {
					fmt.Fprintf(os.Stderr, "owner: WARNING: dialogue %s id %q: UnmarshalEvent: %v; skipped\n", dialogueID, id, err)
					continue
				}
				cache[id] = ev
				byID[id] = ev
			}
			mu.Unlock()
		}
	}

	out := make([]intChat.StreamEvent, 0, len(ids))
	order := ids
	if !addHits {
		order = misses
	}
	for _, id := range order {
		if ev, ok := byID[id]; ok {
			out = append(out, ev)
		}
	}
	return out
}

// emitHandle sends one event envelope (the interlude MarshalEvent JSON) to the
// JS callback as (handleID, envelopeJSON).
func emitHandle(hid uint32, envelopeJSON string) {
	mu.Lock()
	v := cb
	mu.Unlock()
	if v.IsNull() || v.IsUndefined() {
		return
	}
	v.Invoke(hid, envelopeJSON)
}

// run starts one burst on the handle: busy-guard (a second burst on a busy
// handle is a no-op) + per-burst ctx/cancel (fresh on every Session; cancel
// kills only this burst, the handle stays usable for future bursts).
func run(hid uint32) {
	mu.Lock()
	h := handles[hid]
	if h == nil || h.busy {
		if h != nil {
			fmt.Fprintf(os.Stderr, "owner: run(%d): handle busy; burst ignored (busy-guard)\n", hid)
		}
		mu.Unlock()
		return
	}
	h.busy = true
	burstCtx, burstCancel := context.WithCancel(context.Background()) // per-BURST, not per-handle
	h.burstCancel = burstCancel
	mu.Unlock()

	// Single-threaded wasm: consume the session channel in a goroutine and
	// call the JS callback per event. The caller must kick the runtime `resume`
	// export after the start* export returns (one kick per burst) — see the
	// package docs above.
	//
	// The pending id-list is walked (cache + ONE batched getMessages pull)
	// HERE, inside the consumer goroutine, BEFORE the session runs: the
	// gojs Invoke bridge must not run inside the wasmexport wrapper (it
	// corrupts the wrapper's fixed return frame for value-returning exports
	// and the f-loop checkpoint state in general).
	go func() {
		defer func() {
			burstCancel()
			mu.Lock()
			h.busy = false
			h.burstCancel = nil
			mu.Unlock()
		}()
		for _, ev := range resolveIDs(h.dialogueID, h.pendingIDs, h.addHits) {
			h.chat.Messages.AddEvent(ev)
		}
		if h.pendingUserText != "" {
			h.chat.Messages.AddEvent(intChat.NewEventUserMessage(h.pendingUserText))
		}
		for ev := range h.chat.Session(burstCtx, h.cli) {
			b, err := intChat.MarshalEvent(ev)
			if err != nil {
				continue
			}
			emitHandle(hid, string(b))
		}
	}()
}

// ---------------------------------------------------------------------------
// Exported FFI functions (//go:wasmexport)
// ---------------------------------------------------------------------------

// setCallback tells the owner where the JS event callback lives. JS must have
// assigned globalThis.__cyrCallback first. (The wasmexport ABI cannot pass a
// JS function or interface{} value directly.)
//
//go:wasmexport setCallback
func setCallback() {
	mu.Lock()
	defer mu.Unlock()
	cb = js.Global().Get("__cyrCallback")
}

// selectBurstClient picks the streaming client by provider name. "" and
// "mock" keep the HTTP-free echo client (the default; the echo text is
// asserted by e2e). Any other registered provider is built from its config
// via the interlude provider registry (the same mechanism the server uses),
// so adding a provider = registering it (blank import) — no code change here.
// An unregistered/unknown provider returns an error (a LOUD failure, never a
// silent echo fallback).
func selectBurstClient(providerName string, config json.RawMessage, tokenDelay time.Duration) (intChat.Client, error) {
	switch providerName {
	case "", "mock":
		return echoClient{tokenDelay: tokenDelay}, nil
	default:
		data, err := json.Marshal(provider.ProviderEnvelope{Provider: providerName, Config: config})
		if err != nil {
			return nil, err
		}
		return provider.Deserialize(data, provider.DefaultRegistry)
	}
}

// startBurst runs one streaming burst: builds a Chat, adds the conversation
// (the forwarded history + the user message), runs the session, and streams
// every event to the JS callback. The opts JSON is read from
// globalThis.__cyrOpts (set by JS) because the wasmexport ABI cannot take a
// string parameter cleanly.
//
// The export is VOID and performs NO JS crossing in its own body (not even a
// valueGet of __cyrOpts): ALL the work — reading __cyrOpts, unmarshaling,
// building the chat, client selection, the invalid-provider error emit, the
// session run, and the per-event emits — happens inside a single spawned
// goroutine. The gojs bridge must not run inside a value-returning
// wasmexport wrapper: a valueGet/Invoke in the export body corrupts the
// wrapper's fixed return frame (the root cause of the
// "Cannot read properties of undefined (reading '5')" loadValue crash on the
// invalid-provider path). This mirrors the run()/setCallback() pattern.
//
// The per-burst cancel is registered BEFORE the goroutine is spawned and
// cleared (in a defer) when the burst ends, so stop() stays effective for the
// whole burst lifetime.
//
// NOTE on the Go 1.26 js/wasm event loop (important for the harness): the
// export spawns the burst goroutine and returns normally, so it does NOT pump
// it. The caller must kick the runtime `resume` export after startBurst (one
// kick per burst) — an idle timeout is only scheduled when a host timer is
// already pending, so a freshly-spawned burst needs the explicit kick to
// start (2nd+ bursts; the first is pumped by the export's own unwind).
//
//go:wasmexport startBurst
func startBurst() {
	ctx, cancelFn := context.WithCancel(context.Background())

	mu.Lock()
	genSeq++
	genID := genSeq
	cancel = cancelFn
	cancelGen = genID
	mu.Unlock()

	go func() {
		// Cleanup (runs LAST on return/unwind — registered first): cancel the
		// burst context and clear the global cancel only if it is still this
		// burst's (a later burst may have replaced it — funcs can't be
		// compared, so ownership is tracked by generation tag).
		defer func() {
			cancelFn()
			mu.Lock()
			if cancelGen == genID {
				cancel = nil
				cancelGen = 0
			}
			mu.Unlock()
		}()
		// A panic anywhere in the burst (a JS bridge failure, a provider bug)
		// must not take the module down: emit ONE error event and clean up.
		// Registered after the cleanup defer so it runs FIRST on unwind
		// (LIFO) — the emit happens while cancel is still set.
		defer func() {
			if r := recover(); r != nil {
				ev := intChat.NewEventError(fmt.Errorf("wasm owner: burst panicked: %v", r))
				emit(eventToJSON(ev))
			}
		}()

		optsJSON := js.Global().Get("__cyrOpts").String()
		var opts struct {
			UserText     string            `json:"userText"`
			Provider     string            `json:"provider"`
			Config       json.RawMessage   `json:"config"`
			Messages     []json.RawMessage `json:"messages"`
			TokenDelayMS int               `json:"tokenDelayMs"` // model's token delay (ms); 0 → built-in default
		}
		_ = json.Unmarshal([]byte(optsJSON), &opts)

		chat := &intChat.Chat{
			Messages:   intChat.NewMessages(),
			ToolPolicy: intChat.ToolPolicyExitAfter,
		}
		// Add the conversation BEFORE running the session (mirrors the
		// server's addHistoryToChat): each message payload is a raw interlude
		// event JSON.
		for _, m := range opts.Messages {
			ev, err := intChat.UnmarshalEvent(m)
			if err != nil {
				continue // skip a bad event, keep the rest
			}
			chat.Messages.AddEvent(ev)
		}
		if opts.UserText != "" {
			chat.Messages.AddEvent(intChat.NewEventUserMessage(opts.UserText))
		}

		// Select the streaming client BY PROVIDER: "" and "mock" keep the
		// HTTP-free echo client (the default path, paced by the model's
		// configured token delay — tokenDelayMs; 0/absent → the built-in
		// defaultTokenDelay); any other registered provider is built from its
		// config via the interlude provider registry. An unknown provider
		// emits ONE error event (a loud failure, never a silent echo
		// fallback).
		tokenDelay := time.Duration(opts.TokenDelayMS) * time.Millisecond
		client, selErr := selectBurstClient(opts.Provider, opts.Config, tokenDelay)
		if selErr != nil {
			emit(eventToJSON(intChat.NewEventError(fmt.Errorf("wasm owner: provider %q: %v", opts.Provider, selErr))))
			return
		}
		ch := chat.Session(ctx, client)

		// Single-threaded wasm: consume the session channel and call the JS
		// callback per event.
		for ev := range ch {
			emit(eventToJSON(ev))
		}
	}()
}

// stop cancels the active startBurst burst's context (best-effort).
//
//go:wasmexport stop
func stop() {
	mu.Lock()
	defer mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// goStart: the first request of a dialogue. JS sets
// globalThis.__cyrPayload to the dialoguePayload JSON ({dialogue_id, ids, user_text,
// config, tools}) BEFORE calling this export; the export returns the new
// handle id (uint32).
//
// The id-list is walked IN ORDER:
//   - id in the per-dialogue parse cache -> the cached parsed event is added
//     (NO re-parse, NO re-pull);
//   - id not cached -> collected into a misses slice;
//   - if len(misses) > 0: ONE batched getMessages(dialogue_id, misses) call;
//     each returned raw event is UnmarshalEvent'ed EXACTLY ONCE, stored in the
//     cache, and added. A miss id absent from the returned map is skipped and
//     a warning is recorded (missing-id handling).
//
// The id-walk + the batched getMessages pull run INSIDE the consumer
// goroutine (spawned by run, driven by the caller's `resume` kick) — the
// gojs Invoke bridge must not run inside a value-returning wasmexport
// wrapper (it corrupts the wrapper's fixed return frame).
//
// The new user message is appended last, the handle is registered, and the
// burst starts (the run pattern; the caller must kick `resume` afterwards).
//
//go:wasmexport goStart
func goStart() uint32 {
	payloadJSON := js.Global().Get("__cyrPayload").String()
	var p dialoguePayload
	if err := json.Unmarshal([]byte(payloadJSON), &p); err != nil {
		fmt.Fprintf(os.Stderr, "owner: goStart: bad payload: %v\n", err)
		return 0
	}
	if p.DialogueID == "" {
		fmt.Fprintln(os.Stderr, "owner: goStart: empty dialogue_id; refusing")
		return 0
	}

	// NO id-walk / getMessages bridge here: the pull happens inside the
	// consumer goroutine (run -> goroutine -> resolveIDs). The gojs Invoke
	// bridge must not run inside this value-returning wasmexport wrapper.
	msgs := intChat.NewMessages()
	h := &handle{
		chat: &intChat.Chat{
			Messages:   msgs,
			ToolPolicy: intChat.ToolPolicyExitAfter,
		},
		cli:             newClient(p.Config),
		dialogueID:      p.DialogueID,
		pendingIDs:      p.IDs,
		pendingUserText: p.UserText,
		addHits:         true, // full history on the first turn
	}
	mu.Lock()
	nextHandle++
	hid := nextHandle
	handles[hid] = h
	mu.Unlock()

	run(hid) // single-call start: the burst begins immediately
	return hid
}

// goSend: a follow-up on the SAME handle (new user turn). JS sets
// globalThis.__cyrPayload to the dialoguePayload JSON BEFORE calling this export;
// the handle id is passed as the uint32 parameter (the wasmexport ABI maps
// uint32 <-> i32). The payload carries the UPDATED id-list (previous branch +
// the new messages) and the new user_text.
//
// The updated id-list is walked the same way as in goStart (inside the
// consumer goroutine), but with an EXISTING Messages: cache hit -> skipped
// (already in the handle's history — re-adding would duplicate it); miss ->
// ONE batched getMessages pull -> parsed once -> cached -> AddEvent (only
// the newly-missing ones are added). The new user message is appended and
// run(hid) starts the next burst.
//
//go:wasmexport goSend
func goSend(hid uint32) {
	payloadJSON := js.Global().Get("__cyrPayload").String()
	var p dialoguePayload
	if err := json.Unmarshal([]byte(payloadJSON), &p); err != nil {
		fmt.Fprintf(os.Stderr, "owner: goSend: bad payload: %v\n", err)
		return
	}
	mu.Lock()
	h := handles[hid]
	mu.Unlock()
	if h == nil {
		fmt.Fprintf(os.Stderr, "owner: goSend(%d): unknown handle\n", hid)
		return
	}
	// The handle is the source of truth for the dialogue id (the payload's
	// dialogue_id, if present, must agree; we trust the handle).
	// NO bridge here: the id-walk + batched pull run inside the consumer
	// goroutine (run -> goroutine -> resolveIDs).
	h.pendingIDs = p.IDs
	h.pendingUserText = p.UserText
	h.addHits = false // existing handle: only NEW ids are pulled
	run(hid)
}

// goStop cancels the handle's ACTIVE burst (the per-burst ctx). The handle
// itself is NOT killed: it stays registered and usable for future bursts.
//
//go:wasmexport goStop
func goStop(hid uint32) {
	mu.Lock()
	defer mu.Unlock()
	if h := handles[hid]; h != nil && h.burstCancel != nil {
		h.burstCancel()
	}
}

// dumpMessages publishes the handle's message history as a JSON array of
// interlude MarshalEvent envelopes to globalThis.__cyrDump (the wasmexport
// ABI has no string result type, so the string result goes through a JS
// global — the established mechanism). This is the diagnostic that lets the
// harness observe what the owner actually built in the handle's Messages.
//
//go:wasmexport dumpMessages
func dumpMessages(hid uint32) {
	mu.Lock()
	h := handles[hid]
	mu.Unlock()
	if h == nil {
		js.Global().Set("__cyrDump", "null")
		return
	}
	evs := h.chat.Messages.Snapshot()
	raws := make([]json.RawMessage, 0, len(evs))
	for _, ev := range evs {
		b, err := intChat.MarshalEvent(ev)
		if err != nil {
			fmt.Fprintf(os.Stderr, "owner: dumpMessages(%d): MarshalEvent: %v; event skipped\n", hid, err)
			continue
		}
		raws = append(raws, b)
	}
	arr, _ := json.Marshal(raws)
	js.Global().Set("__cyrDump", string(arr))
}

// main blocks forever; the wasm_exec.js event loop keeps the process alive and
// responsive so JS can drive the exported functions.
func main() {
	fmt.Fprintln(os.Stderr, "owner wasm: ready (blocking main)")
	select {}
}
