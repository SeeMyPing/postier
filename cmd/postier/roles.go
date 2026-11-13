// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/dispatch"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi"
	"github.com/SeeMyPing/postier/internal/ingest"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/platform"
	"github.com/SeeMyPing/postier/internal/queue"
	"github.com/SeeMyPing/postier/internal/store"
)

// Settings that hold a credential are read from the environment only, so
// that they never appear in a process list or in a pod spec:
//
//	POSTIER_DATABASE_URL          postier_app role (api)
//	POSTIER_SYSTEM_DATABASE_URL   postier_system role (dispatch, ingest, seed)
//	POSTIER_MIGRATE_DATABASE_URL  owner of the schema (migrate)
//	POSTIER_SIGNING_KEY           HMAC key of return paths and Message-IDs
//	POSTIER_KUMO_INJECT_PASSWORD  basic-auth password of KumoMTA's injection API
//	POSTIER_INGEST_TOKEN          bearer token of the log hook

func runAPI(ctx context.Context, args []string) error {
	var (
		c      common
		listen string
		dbURL  string
	)
	f := platform.NewFlags("api")
	c.register(f)
	f.String(&listen, "listen", ":8080", "listen address of the API")
	f.Secret(&dbURL, "database-url")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{"database-url": dbURL}); err != nil {
		return err
	}
	log, err := c.logger("api")
	if err != nil {
		return err
	}

	pool, err := store.OpenApp(ctx, dbURL, "api")
	if err != nil {
		return err
	}
	defer pool.Close()
	jobs, err := queue.NewInserter(pool.Raw(), log)
	if err != nil {
		return err
	}

	server := httpapi.New(
		accept.NewAuthenticator(pool),
		accept.NewService(pool, jobs, accept.DefaultLimits(), log),
		pool, log,
	)
	probes := platform.NewProbes()
	probes.AddCheck("database", pool.Ping)

	return platform.Run(ctx, log,
		probes.Service(c.probeAddr, log),
		platform.HTTPService("api", listen, server.Handler(), log),
	)
}

func runDispatch(ctx context.Context, args []string) error {
	var (
		c            common
		dbURL        string
		kumoURL      string
		kumoUser     string
		kumoPassword string
		signingKey   string
		concurrency  int
		guardDelay   time.Duration
	)
	f := platform.NewFlags("dispatch")
	c.register(f)
	f.String(&kumoURL, "kumo-url", "", "base URL of KumoMTA's HTTP listener, e.g. http://kumomta:8000")
	f.String(&kumoUser, "kumo-user", "postier", "basic-auth user of the injection API")
	f.Int(&concurrency, "concurrency", 32, "messages injected in parallel, and connections kept open to KumoMTA")
	f.Duration(&guardDelay, "guard-delay", dispatch.DefaultGuardDelay,
		"on a retry, how long to wait for the log record of the previous attempt before injecting again")
	f.Secret(&dbURL, "system-database-url")
	f.Secret(&kumoPassword, "kumo-inject-password")
	f.Secret(&signingKey, "signing-key")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{
		"system-database-url": dbURL, "kumo-url": kumoURL,
		"kumo-inject-password": kumoPassword, "signing-key": signingKey,
	}); err != nil {
		return err
	}
	log, err := c.logger("dispatch")
	if err != nil {
		return err
	}
	signer, err := domain.NewSigner([]byte(signingKey))
	if err != nil {
		return err
	}

	pool, err := store.OpenSystem(ctx, dbURL, "dispatch")
	if err != nil {
		return err
	}
	defer pool.Close()

	worker := &dispatch.InjectWorker{
		Pool:       pool,
		Kumo:       kumo.NewClient(kumoURL, kumoUser, kumoPassword, concurrency),
		Signer:     signer,
		Log:        log,
		GuardDelay: guardDelay,
	}
	client, err := queue.NewWorkerClient(pool.Raw(), queue.WorkerConfig{
		Workers: dispatch.Workers(worker), InjectConcurrency: concurrency, Logger: log,
	})
	if err != nil {
		return err
	}

	probes := platform.NewProbes()
	probes.AddCheck("database", pool.Ping)

	return platform.Run(ctx, log,
		probes.Service(c.probeAddr, log),
		func(ctx context.Context) error {
			if err := client.Start(ctx); err != nil {
				return err
			}
			log.Info("working", "queue", queue.QueueInject, "concurrency", concurrency)
			<-ctx.Done()
			// Let the injections in flight finish, and release the jobs not
			// started: another replica picks them up.
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), platform.ShutdownTimeout)
			defer cancel()
			return client.Stop(sctx)
		},
	)
}

func runIngest(ctx context.Context, args []string) error {
	var (
		c          common
		listen     string
		dbURL      string
		token      string
		signingKey string
		captureDir string
		dropAck    string
	)
	f := platform.NewFlags("ingest")
	c.register(f)
	f.String(&listen, "listen", ":8081", "listen address of the log hook endpoint")
	f.String(&captureDir, "capture-dir", "", "debug: write every received batch to this directory")
	f.String(&dropAck, "chaos-drop-ack", "0",
		"test benches only: share (0 to 1) of committed batches reported as failed, to force redeliveries")
	f.Secret(&dbURL, "system-database-url")
	f.Secret(&token, "ingest-token")
	f.Secret(&signingKey, "signing-key")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{
		"system-database-url": dbURL, "ingest-token": token, "signing-key": signingKey,
	}); err != nil {
		return err
	}
	log, err := c.logger("ingest")
	if err != nil {
		return err
	}
	signer, err := domain.NewSigner([]byte(signingKey))
	if err != nil {
		return err
	}

	pool, err := store.OpenSystem(ctx, dbURL, "ingest")
	if err != nil {
		return err
	}
	defer pool.Close()

	chaos, err := strconv.ParseFloat(dropAck, 64)
	if err != nil || chaos < 0 || chaos > 1 {
		return fmt.Errorf("--chaos-drop-ack: want a number between 0 and 1, got %q", dropAck)
	}
	if chaos > 0 {
		log.Warn("fault injection is on: do not use outside a test bench", "chaos_drop_ack", chaos)
	}
	svc, err := ingest.New(ingest.Config{
		Pool: pool, Signer: signer, Token: token, Log: log, CaptureDir: captureDir, ChaosDropAck: chaos,
	})
	if err != nil {
		return err
	}
	probes := platform.NewProbes()
	probes.AddCheck("database", pool.Ping)

	return platform.Run(ctx, log,
		probes.Service(c.probeAddr, log),
		platform.HTTPService("ingest", listen, svc.Handler(), log),
	)
}

func runMigrate(ctx context.Context, args []string) error {
	var (
		logLevel, logFormat string
		dbURL               string
	)
	f := platform.NewFlags("migrate")
	f.String(&logLevel, "log-level", "info", "log level: debug, info, warn or error")
	f.String(&logFormat, "log-format", "json", "log format: json or text")
	f.Secret(&dbURL, "migrate-database-url")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{"migrate-database-url": dbURL}); err != nil {
		return err
	}
	c := common{logLevel: logLevel, logFormat: logFormat}
	log, err := c.logger("migrate")
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx, dbURL, log); err != nil {
		return err
	}
	log.Log(ctx, slog.LevelInfo, "database is up to date")
	return nil
}
