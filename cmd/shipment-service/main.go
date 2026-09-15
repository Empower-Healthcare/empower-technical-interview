// shipment-service is the Go gRPC API: GetQuote, CreateShipment, GetShipment.
//
// Ports (Compose defaults): gRPC on :50051, management HTTP on :8080.
package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	parcellabv1 "github.com/empower-healthcare/parcellab/gen/parcellab/v1"
	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/correlation"
	"github.com/empower-healthcare/parcellab/internal/pricing"
	"github.com/empower-healthcare/parcellab/internal/shipmentapi"
)

const shutdownGrace = 10 * time.Second

type serviceConfig struct {
	GRPCAddr       string
	MgmtAddr       string
	DatabaseURL    string
	PricingURL     string
	PricingTimeout time.Duration
}

func (c serviceConfig) Validate() error {
	if c.PricingTimeout <= 0 {
		return fmt.Errorf("PRICING_TIMEOUT %s: must be positive", c.PricingTimeout)
	}
	return nil
}

func main() { app.Main("shipment-service", run) }

func run(ctx context.Context, a app.App) error {
	cfg := serviceConfig{
		GRPCAddr:       a.Env.String("GRPC_ADDR", ":50051"),
		MgmtAddr:       a.Env.String("MGMT_ADDR", ":8080"),
		DatabaseURL:    a.Env.DatabaseURL(),
		PricingURL:     a.Env.String("PRICING_URL", "http://pricing:8000"),
		PricingTimeout: a.Env.Duration("PRICING_TIMEOUT", 2*time.Second),
	}
	if err := a.Env.Err(); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	store, pool, err := a.OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	pricer := pricing.NewClient(cfg.PricingURL, cfg.PricingTimeout, pricing.NewMetrics(a.Registry))

	healthSrv := health.NewServer()
	grpcSrv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		correlation.UnaryServerInterceptor(),
		shipmentapi.Interceptor(shipmentapi.NewMetrics(a.Registry), a.Logger),
	))
	parcellabv1.RegisterShipmentServiceServer(grpcSrv, shipmentapi.New(pricer, store, a.Logger, time.Now))
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	reflection.Register(grpcSrv)

	mgmt, err := a.ListenManagement(ctx, cfg.MgmtAddr, store.Ping)
	if err != nil {
		return err
	}
	defer shutdownManagement(ctx, a, mgmt)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", cfg.GRPCAddr, err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcSrv.Serve(lis) }()
	healthSrv.SetServingStatus(parcellabv1.ShipmentService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	a.Logger.InfoContext(ctx, "gRPC listening",
		"addr", lis.Addr().String(), "pricing_url", cfg.PricingURL, "pricing_timeout", cfg.PricingTimeout.String())

	// Graceful shutdown runs on every exit path: stop advertising health, let
	// in-flight RPCs finish (bounded), then the deferred management shutdown
	// and pool close follow.
	defer func() {
		healthSrv.Shutdown()
		stopCtx, cancel := app.ShutdownContext(ctx, shutdownGrace)
		defer cancel()
		gracefulStop(stopCtx, grpcSrv)
		a.Logger.InfoContext(ctx, "stopped")
	}()

	select {
	case <-ctx.Done():
		a.Logger.InfoContext(ctx, "shutdown signal received")
		return nil
	case err := <-serveErr:
		return fmt.Errorf("gRPC server: %w", err)
	case err := <-mgmt.Failed():
		return err
	}
}

// gracefulStop waits for in-flight RPCs until ctx expires, then forces the stop.
func gracefulStop(ctx context.Context, srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		srv.Stop()
	}
}

func shutdownManagement(ctx context.Context, a app.App, mgmt *app.Management) {
	stopCtx, cancel := app.ShutdownContext(ctx, shutdownGrace)
	defer cancel()
	if err := mgmt.Shutdown(stopCtx); err != nil {
		a.Logger.WarnContext(ctx, "management shutdown", "error", err.Error())
	}
}
