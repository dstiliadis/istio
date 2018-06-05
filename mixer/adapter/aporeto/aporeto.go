//go:generate $GOPATH/src/istio.io/istio/bin/mixer_codegen.sh -f mixer/adapter/aporeto/config/config.proto
package aporeto

import (
	"context"
	"fmt"

	"github.com/aporeto-inc/bireme"
	"istio.io/istio/mixer/adapter/aporeto/config"
	"istio.io/istio/mixer/pkg/adapter"
	"istio.io/istio/mixer/template/authorization"
)

type (
	builder struct {
		adpCfg *config.Params
		h      *handler
	}
	handler struct {
		adapter bireme.Adapter
	}
)

// ensure types implement the requisite interfaces
var _ authorization.HandlerBuilder = &builder{}
var _ authorization.Handler = &handler{}

// adapter.HandlerBuilder#Build
func (b *builder) Build(context context.Context, env adapter.Env) (adapter.Handler, error) {
	env.Logger().Infof("Building aporeto adapter")

	// Create a new Aporeto adapter based on the bireme library
	adp, err := bireme.NewAporetoAdapter(context, b.adpCfg, env)
	if err != nil {
		env.Logger().Errorf("Unable to build Aporeto adapter: %s", err)
		return nil, err
	}

	// Create the basic handler object
	b.h = &handler{
		adapter: adp,
	}

	// Start the adapter in the background as a daemon.
	env.ScheduleDaemon(func() { adp.Run(context) })

	env.Logger().Infof("Succesfully build the Aporeto adapter")
	return b.h, nil
}

// adapter.HandleBuilder#SetAuthorizationTypes
func (b *builder) SetAuthorizationTypes(types map[string]*authorization.Type) {}

// adapter.HandlerBuilder#SetAdapterConfig
func (b *builder) SetAdapterConfig(cfg adapter.Config) {
	b.adpCfg = cfg.(*config.Params)
}

// adapter.HandlerBuilder#Validate
func (b *builder) Validate() (ce *adapter.ConfigErrors) {
	if b.adpCfg.GetNamespace() == "" {
		ce.Append("namespace", fmt.Errorf("Namespace cannot be empty"))
	}
	if b.adpCfg.GetUri() == "" {
		ce.Append("service-uri", fmt.Errorf("Service URI cannot be empty"))
	}
	return
}

// metric.Handler#HandleAuthorization
func (h *handler) HandleAuthorization(ctx context.Context, insts *authorization.Instance) (adapter.CheckResult, error) {
	return h.adapter.Authorize(ctx, insts)
}

// adapter.Handler#Close
func (h *handler) Close() error { return nil }

// GetInfo returns the adapter.Info specific to this adapter.
func GetInfo() adapter.Info {
	info := adapter.Info{
		Name:        "aporeto",
		Description: "Authorization Adapter",
		Impl:        "istio.io/istio/mixer/adapter/aporeto",
		SupportedTemplates: []string{
			authorization.TemplateName,
		},
		NewBuilder: func() adapter.HandlerBuilder {
			return &builder{}
		},
		DefaultConfig: &config.Params{
			Namespace: "/flash",
		},
	}
	return info
}
