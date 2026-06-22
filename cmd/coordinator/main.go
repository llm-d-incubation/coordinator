package main

import (
	"context"
	"errors"
	"github.com/spf13/pflag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	ctrl "sigs.k8s.io/controller-runtime"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"

	"github.com/llm-d/coordinator/pkg/config"
	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
	"github.com/llm-d/coordinator/pkg/server"
	"github.com/llm-d/coordinator/pkg/steps"
)

func main() {
	configPath := pflag.String("config", "configs/coordinator.yaml", "path to configuration file")
	pflag.Parse()

	logutil.InitSetupLogging()
	log := ctrl.Log.WithName("coordinator")

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error(err, "failed to load config")
		os.Exit(1)
	}

	gwClient := gateway.New(cfg.Gateway)

	steps, err := buildPipeline(cfg, gwClient)
	if err != nil {
		log.Error(err, "failed to build pipeline")
		os.Exit(1)
	}

	p := pipeline.New(steps)
	srv := server.New(cfg.Server, p)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	log.Info("starting coordinator", "addr", cfg.Server.ListenAddr)
	log.Info("graceful shutdown enabled", "timeout", cfg.Server.ShutdownTimeout)

	select {
	case err := <-srvErr:
		stop()
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error(err, "server error")
			os.Exit(1)
		}
	case <-ctx.Done():
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			cancel()
			log.Error(err, "shutdown error")
			os.Exit(1)
		}
		cancel()
	}
}

func mergeConnectorDefaults(params map[string]any, kvConnector, ecConnector string) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		out[k] = v
	}
	if _, ok := out[steps.ParamKVConnector]; !ok && kvConnector != "" {
		out[steps.ParamKVConnector] = kvConnector
	}
	if _, ok := out[steps.ParamECConnector]; !ok && ecConnector != "" {
		out[steps.ParamECConnector] = ecConnector
	}
	return out
}

func buildPipeline(cfg *config.Config, gwClient *gateway.Client) ([]pipeline.Step, error) {
	var steps []pipeline.Step
	for _, stepCfg := range cfg.Pipeline.Steps {
		params := mergeConnectorDefaults(stepCfg.Params, cfg.Pipeline.KVConnector, cfg.Pipeline.ECConnector)
		step, err := pipeline.Build(stepCfg.Type, params)
		if err != nil {
			return nil, err
		}

		// Inject dependencies based on step type
		type gatewayAware interface {
			SetGatewayClient(*gateway.Client)
		}
		if ga, ok := step.(gatewayAware); ok {
			ga.SetGatewayClient(gwClient)
		}

		type renderAware interface {
			SetServiceAddress(string)
		}
		if ra, ok := step.(renderAware); ok {
			ra.SetServiceAddress(cfg.Rendering.Address)
		}

		steps = append(steps, step)
	}
	return steps, nil
}
