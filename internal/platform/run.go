// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// ShutdownTimeout bounds a graceful stop. It must stay below the pod's
// terminationGracePeriodSeconds (30 s by default).
const ShutdownTimeout = 25 * time.Second

// Service is a long-running part of a role. Run blocks until ctx is
// cancelled, then stops gracefully and returns nil.
type Service func(ctx context.Context) error

// Run starts the services and waits. SIGINT or SIGTERM cancels the context;
// so does the first service that fails, and its error is returned.
func Run(ctx context.Context, log *slog.Logger, services ...Service) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)
	for _, s := range services {
		g.Go(func() error { return s(ctx) })
	}
	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("stopped")
	return nil
}

// HTTPService serves handler on addr until ctx is cancelled, then lets
// in-flight requests finish.
func HTTPService(name, addr string, handler http.Handler, log *slog.Logger) Service {
	return func(ctx context.Context) error {
		srv := &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
			// The request context must outlive the cancellation of ctx:
			// in-flight requests are allowed to finish during shutdown.
			BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
		}
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		log.Info("listening", "server", name, "addr", ln.Addr().String())

		errc := make(chan error, 1)
		go func() { errc <- srv.Serve(ln) }()

		select {
		case err := <-errc:
			return err
		case <-ctx.Done():
		}
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			log.Warn("forced shutdown", "server", name, "err", err)
			return srv.Close()
		}
		return nil
	}
}

// Probes serves the Kubernetes probes:
//
//	/healthz  liveness: the process is running
//	/readyz   readiness: every registered check passes and the role is not
//	          shutting down
type Probes struct {
	mu       sync.RWMutex
	checks   map[string]func(context.Context) error
	draining atomic.Bool
}

// NewProbes returns an empty set of probes.
func NewProbes() *Probes {
	return &Probes{checks: map[string]func(context.Context) error{}}
}

// AddCheck registers a readiness check.
func (p *Probes) AddCheck(name string, check func(context.Context) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checks[name] = check
}

// Handler returns the probe endpoints.
func (p *Probes) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		status := http.StatusOK
		result := map[string]string{}
		if p.draining.Load() {
			status = http.StatusServiceUnavailable
			result["shutdown"] = "draining"
		}
		p.mu.RLock()
		for name, check := range p.checks {
			if err := check(ctx); err != nil {
				status = http.StatusServiceUnavailable
				result[name] = err.Error()
			} else {
				result[name] = "ok"
			}
		}
		p.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(result)
	})
	return mux
}

// Service serves the probes on addr. As soon as ctx is cancelled, readiness
// fails, so that the Service stops routing to a pod that is going away, while
// the listener itself stays up until the other servers have drained.
func (p *Probes) Service(addr string, log *slog.Logger) Service {
	inner := HTTPService("probes", addr, p.Handler(), log)
	return func(ctx context.Context) error {
		go func() {
			<-ctx.Done()
			p.draining.Store(true)
		}()
		return inner(ctx)
	}
}
