package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/proxy"
)

func main() {
	configPath := flag.String("config", "configs/router.local.json", "router JSON config")
	flag.Parse()

	cfg, err := proxy.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	handler, err := proxy.NewHandler(cfg)
	if err != nil {
		log.Fatalf("create router handler: %v", err)
	}
	defer handler.Close()

	mainServer := &http.Server{
		Addr:              cfg.Server.ListenAddr,
		Handler:           handler.PublicHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	adminServer := &http.Server{
		Addr:              cfg.Server.AdminAddr,
		Handler:           handler.AdminHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Printf("router proxy listening on %s", cfg.Server.ListenAddr)
		errCh <- listenAndServe(mainServer)
	}()
	go func() {
		log.Printf("router admin listening on %s", cfg.Server.AdminAddr)
		errCh <- listenAndServe(adminServer)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
	case err := <-errCh:
		log.Printf("server stopped: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = mainServer.Shutdown(ctx)
	_ = adminServer.Shutdown(ctx)
}

func listenAndServe(server *http.Server) error {
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("%s: %w", server.Addr, err)
	}
	return nil
}
