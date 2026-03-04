package options

import (
	"context"
	"net"
	"strings"

	redisrate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

type ConnectRateLimiter struct {
	ipOn  bool
	uidOn bool

	ipPerSecond  int
	uidPerSecond int

	ipKeyPrefix  string
	uidKeyPrefix string

	ipWhitelist []*net.IPNet

	limiter *redisrate.Limiter
}

func NewConnectRateLimiter(opts *Options) *ConnectRateLimiter {
	l := &ConnectRateLimiter{
		ipOn:         opts.ConnectRateLimit.IP.On,
		uidOn:        opts.ConnectRateLimit.UID.On,
		ipPerSecond:  opts.ConnectRateLimit.IP.PerSecond,
		uidPerSecond: opts.ConnectRateLimit.UID.PerSecond,
		ipKeyPrefix:  opts.ConnectRateLimit.IP.KeyPrefix,
		uidKeyPrefix: opts.ConnectRateLimit.UID.KeyPrefix,
		ipWhitelist:  parseIPWhitelist(opts.ConnectRateLimit.IP.Whitelist),
	}

	if !l.ipOn && !l.uidOn {
		return l
	}

	client := redis.NewClient(&redis.Options{
		Addr:     opts.ConnectRateLimit.Redis.Addr,
		Username: opts.ConnectRateLimit.Redis.Username,
		Password: opts.ConnectRateLimit.Redis.Password,
		DB:       opts.ConnectRateLimit.Redis.DB,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
	l.limiter = redisrate.NewLimiter(client)
	return l
}

func (l *ConnectRateLimiter) AllowIP(ctx context.Context, ip string) (bool, error) {
	if !l.ipOn {
		return true, nil
	}
	normalizedIP := normalizeRemoteIP(ip)
	if l.ipWhitelisted(normalizedIP) {
		return true, nil
	}
	return l.allow(ctx, l.ipKeyPrefix+":"+normalizedIP, l.ipPerSecond)
}

func (l *ConnectRateLimiter) AllowUID(ctx context.Context, uid string) (bool, error) {
	if !l.uidOn {
		return true, nil
	}
	if strings.TrimSpace(uid) == "" {
		uid = "__empty_uid__"
	}
	return l.allow(ctx, l.uidKeyPrefix+":"+uid, l.uidPerSecond)
}

func (l *ConnectRateLimiter) allow(ctx context.Context, key string, perSecond int) (bool, error) {
	if l.limiter == nil {
		return true, nil
	}
	r, err := l.limiter.Allow(ctx, key, redisrate.PerSecond(perSecond))
	if err != nil {
		return true, err
	}
	return r.Allowed > 0, nil
}

func (l *ConnectRateLimiter) ipWhitelisted(ip string) bool {
	if len(l.ipWhitelist) == 0 {
		return false
	}
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}
	for _, cidr := range l.ipWhitelist {
		if cidr.Contains(parsedIP) {
			return true
		}
	}
	return false
}

func parseIPWhitelist(values []string) []*net.IPNet {
	if len(values) == 0 {
		return nil
	}
	result := make([]*net.IPNet, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if strings.Contains(v, "/") {
			_, cidr, err := net.ParseCIDR(v)
			if err == nil {
				result = append(result, cidr)
			}
			continue
		}
		ip := net.ParseIP(v)
		if ip == nil {
			continue
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		result = append(result, &net.IPNet{
			IP:   ip,
			Mask: net.CIDRMask(bits, bits),
		})
	}
	return result
}

func normalizeRemoteIP(addr string) string {
	if strings.TrimSpace(addr) == "" {
		return "unknown"
	}
	// For values like "ip:port"
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		addr = host
	}
	parsed := net.ParseIP(addr)
	if parsed != nil {
		return parsed.String()
	}
	return strings.TrimSpace(addr)
}
