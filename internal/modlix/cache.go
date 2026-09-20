package modlix

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Shared is the cache layer that outlives one process: Redis in a deployment, a fake in
// tests. Misses and errors are the same thing to a caller — both mean "ask the source".
type Shared interface {
	Get(ctx context.Context, key string) (string, bool)
	Set(ctx context.Context, key, value string)
	// Watch calls onEvict when the platform announces that host resolutions are stale.
	// An empty key means "all of them".
	Watch(ctx context.Context, onEvict func(key string))
}

// RedisShared is the platform's Redis, used the way the platform uses it.
//
// Entries live in one hash per cache, `<prefix>-<cacheName>`, exactly like
// commons/CacheService — so `HGETALL cmn-analyticsSite` reads the same way as every other
// Modlix cache, and one DEL clears it.
//
// It does NOT read the gateway's own `gatewayClientAppCodeType` hash. That would couple this
// engine to another service's serialization format, and a Java CacheObject wrapper is not a
// contract anyone promised us. We keep our own entries and listen to the same announcements.
type RedisShared struct {
	Client *redis.Client
	// Prefix is `redis.cache.prefix` on the Java side. "cmn" in every environment today,
	// and getting it wrong means a cache that works and is never invalidated.
	Prefix string
	TTL    time.Duration
	Log    *slog.Logger
}

const (
	cacheName = "analyticsSite"

	// The cache the platform evicts when a client URL changes. ClientUrlService and
	// AppService both call evictAllFunction on it, which publishes "<hash>:*" — so
	// subscribing costs nothing on the Modlix side: the announcement already happens.
	gatewayHostCache = "gatewayClientAppCodeType"

	// Where those announcements are published. `redis.channel` on the Java side.
	evictionChannel = "evictionChannel"
)

func (r *RedisShared) hash() string { return r.Prefix + "-" + cacheName }

func (r *RedisShared) Get(ctx context.Context, key string) (string, bool) {
	v, err := r.Client.HGet(ctx, r.hash(), key).Result()
	if err != nil {
		// redis.Nil is an ordinary miss. Anything else is Redis being unavailable, which
		// is also a miss: the resolver falls back to the security call, and analytics
		// keeps working more slowly rather than stopping.
		if err != redis.Nil && r.Log != nil {
			r.Log.Warn("cache: redis read failed, falling through to security", "err", err)
		}
		return "", false
	}
	return v, true
}

func (r *RedisShared) Set(ctx context.Context, key, value string) {
	if err := r.Client.HSet(ctx, r.hash(), key, value).Err(); err != nil {
		if r.Log != nil {
			r.Log.Warn("cache: redis write failed", "err", err)
		}
		return
	}
	// A TTL on the hash rather than on each field, because the fields of a Redis hash
	// cannot expire individually before Redis 7.4 and we do not control which server a
	// deployment runs. The whole map is cheap to rebuild — worst case every host pays one
	// security call once per TTL, spread over whenever they next send an event.
	if r.TTL > 0 {
		r.Client.Expire(ctx, r.hash(), r.TTL)
	}
}

// Watch subscribes to the platform's eviction channel.
//
// The message format is `<prefix>-<cacheName>:<key>`, with `*` as the key meaning the whole
// cache. We react to announcements about the GATEWAY's host cache, because that is the one
// the platform already clears when a client URL or an application changes — the same event
// that makes our own entry wrong. This is the entire invalidation story, and it needs no
// change on the Modlix side.
func (r *RedisShared) Watch(ctx context.Context, onEvict func(key string)) {
	sub := r.Client.Subscribe(ctx, evictionChannel)

	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				name, key, found := strings.Cut(msg.Payload, ":")
				if !found {
					continue
				}
				// Only the two caches that can make a host resolution wrong. Every
				// other eviction on this channel belongs to some other service and
				// must not cost us a cache.
				if name != r.Prefix+"-"+gatewayHostCache && name != r.hash() {
					continue
				}
				if key == "*" {
					if err := r.Client.Del(ctx, r.hash()).Err(); err != nil && r.Log != nil {
						r.Log.Warn("cache: clearing after an eviction announcement failed", "err", err)
					}
					onEvict("")
					continue
				}
				r.Client.HDel(ctx, r.hash(), key)
				onEvict(key)
			}
		}
	}()
}

// MemoryShared is the test double, and the fallback when no Redis is configured: a single
// node with a local cache still works, it just cannot be told when to forget.
type MemoryShared struct {
	mu sync.Mutex
	m  map[string]string
}

func NewMemoryShared() *MemoryShared { return &MemoryShared{m: map[string]string{}} }

func (m *MemoryShared) Get(_ context.Context, key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.m[key]
	return v, ok
}

func (m *MemoryShared) Set(_ context.Context, key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = value
}

func (m *MemoryShared) Watch(context.Context, func(string)) {}
