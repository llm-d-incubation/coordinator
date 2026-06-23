package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	ctrl "sigs.k8s.io/controller-runtime"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"

	"github.com/llm-d/coordinator/pkg/config"
	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
	"github.com/llm-d/coordinator/pkg/server"
	"github.com/llm-d/coordinator/pkg/steps"
)

func main() {
	configPath := pflag.String("config", "configs/coordinator.yaml", "path to configuration file")

	logOpts := logutil.NewOptions()
	logOpts.AddFlags(pflag.CommandLine)

	pflag.Parse()

	logutil.InitSetupLogging()
	log := ctrl.Log.WithName("coordinator")

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error(err, "failed to load config")
		os.Exit(1)
	}

	// CLI -v wins over config log_level.
	if vFlag := pflag.CommandLine.Lookup("v"); vFlag != nil && !vFlag.Changed {
		logOpts.LogVerbosity = cfg.LogLevel
	}
	if err := logOpts.Validate(); err != nil {
		log.Error(err, "invalid logging options")
		os.Exit(1)
	}
	if err := logOpts.Complete(); err != nil {
		log.Error(err, "failed to complete logging options")
		os.Exit(1)
	}
	logutil.InitLogging(&logOpts.ZapOptions)
	log.Info("log level set", "level", logOpts.LogVerbosity)
	log.Info("pipeline connectors",
		"kv_connector", cfg.Pipeline.KVConnector,
		"ec_connector", cfg.Pipeline.ECConnector)
	// Log presence only: proxy URLs can carry basic-auth credentials
	// (http://user:pass@host) and must not reach startup logs. NO_PROXY is a
	// plain host list, so it is safe to log verbatim.
	log.Info("proxy environment",
		"http_proxy_set", os.Getenv("HTTP_PROXY") != "",
		"https_proxy_set", os.Getenv("HTTPS_PROXY") != "",
		"NO_PROXY", os.Getenv("NO_PROXY"))

	gwClient := gateway.New(cfg.Gateway)

	steps, err := buildPipeline(cfg, gwClient)
	if err != nil {
		log.Error(err, "failed to build pipeline")
		os.Exit(1)
	}

	p := pipeline.New(steps)
	srv := server.New(cfg.Server, p)

	log.Info("starting coordinator", "addr", cfg.Server.ListenAddr)
	log.Info("graceful shutdown enabled", "timeout", cfg.Server.ShutdownTimeout)

	if err := serveUntilSignal(srv, cfg.Server.ShutdownTimeout); err != nil {
		log.Error(err, "server error")
		os.Exit(1)
	}
}

// serveUntilSignal starts srv and blocks until it exits or a signal is received.
// On SIGTERM/SIGINT it initiates a graceful drain bounded by shutdownTimeout.
func serveUntilSignal(srv *server.Server, shutdownTimeout time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	select {
	case err := <-srvErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
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
		if _, ok := params["use_openai_format"]; !ok {
			params["use_openai_format"] = cfg.Gateway.UseOpenAIFormat
		}
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

		steps = append(steps, step)
	}
	return steps, nil
}
