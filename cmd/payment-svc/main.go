package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	paymentsv1 "github.com/Stoogzy/LedgerFlow/gen/go/ledgerflow/payments/v1"
)

// Embedding Unimplemented... gives default "Unimplemented" responses for RPCs
// we haven't written yet, so adding an RPC to the proto never breaks the build.
type paymentServer struct {
	paymentsv1.UnimplementedPaymentServiceServer
	logger *slog.Logger
}

func (s *paymentServer) CreatePayment(ctx context.Context, req *paymentsv1.CreatePaymentRequest) (*paymentsv1.CreatePaymentResponse, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	s.logger.InfoContext(ctx, "create payment received", "idempotency_key", req.GetIdempotencyKey())
	return nil, status.Error(codes.Unimplemented, "ledger writes arrive in Module 2")
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("payment-svc exited with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	paymentsv1.RegisterPaymentServiceServer(srv, &paymentServer{logger: logger})
	reflection.Register(srv) // lets grpcurl/Postman discover the API

	errCh := make(chan error, 1)
	go func() {
		logger.Info("payment-svc listening", "addr", lis.Addr().String())
		errCh <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining in-flight RPCs")
		srv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}