//go:generate $GOPATH/src/istio.io/istio/bin/mixer_codegen.sh -f mixer/adapter/aporeto/config/config.proto
package aporeto

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	"github.com/aporeto-inc/trireme-lib/controller/pkg/urisearch"

	"istio.io/istio/mixer/adapter/aporeto/config"
	"istio.io/istio/mixer/pkg/adapter"
	"istio.io/istio/mixer/pkg/status"
	"istio.io/istio/mixer/template/authorization"

	"github.com/aporeto-inc/manipulate"
	"github.com/aporeto-inc/manipulate/maniphttp"
	"github.com/aporeto-inc/tg/tglib"

	mclient "github.com/aporeto-inc/midgard-lib/client"
)

type (
	builder struct {
		adpCfg *config.Params
		h      *handler
	}
	handler struct {
		namespace   string
		manipulator manipulate.Manipulator
		serviceMap  map[string]*urisearch.APICache
		sync.RWMutex
	}
)

// ensure types implement the requisite interfaces
var _ authorization.HandlerBuilder = &builder{}
var _ authorization.Handler = &handler{}

// adapter.HandlerBuilder#Build
func (b *builder) Build(context context.Context, env adapter.Env) (adapter.Handler, error) {
	env.Logger().Debugf("Building aporeto adapter")
	cert, key, err := tglib.ReadCertificate([]byte(b.adpCfg.Certificate), []byte(b.adpCfg.Key), "aporeto")
	if err != nil {
		return nil, fmt.Errorf("Invalid certificate")
	}

	tlsCert, err := tglib.ToTLSCertificate(cert, key)
	if err != nil {
		return nil, fmt.Errorf("Invalid TLS certificate or key")
	}

	rootCAPool := x509.NewCertPool()
	rootCAPool.AppendCertsFromPEM([]byte(b.adpCfg.GetCa()))

	clientTLSConfig := &tls.Config{
		RootCAs:      rootCAPool,
		Certificates: []tls.Certificate{tlsCert},
	}

	m, err := maniphttp.NewHTTPManipulatorWithTokenManager(
		context,
		b.adpCfg.Uri,
		b.adpCfg.Namespace,
		clientTLSConfig,
		mclient.NewMidgardTokenManager(b.adpCfg.Uri, time.Hour*1, clientTLSConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("Cannot initiate connection to Aporeto")
	}

	serviceMap, err := RetrieveServices(context, m, b.adpCfg.Namespace)
	if err != nil {
		return nil, fmt.Errorf("Cannot retrieve service map from Aporeto: %s", err)
	}

	b.h = &handler{
		namespace:   b.adpCfg.Namespace,
		manipulator: m,
		serviceMap:  serviceMap,
	}

	monitor := NewMonitor(m, b.h, b.adpCfg.Namespace)
	env.ScheduleDaemon(func() { monitor.listen(context) })

	env.Logger().Debugf("Succesfully build the Aporeto adapter")
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
	// API Call to validate that we have access to the namespace
	return
}

// metric.Handler#HandleAuthorization
func (h *handler) HandleAuthorization(ctx context.Context, insts *authorization.Instance) (adapter.CheckResult, error) {
	h.RLock()
	defer h.RUnlock()

	labels := mapLabels(insts)
	fmt.Println("HERE ARE MY LABELS ", labels)

	cache, ok := h.serviceMap[insts.Action.Service]
	if !ok {
		return adapter.CheckResult{
			Status: status.WithPermissionDenied("Uknown Service\n"),
		}, nil
	}

	found := cache.FindAndMatchScope(insts.Action.Method, insts.Action.Path, labels)
	if !found {
		return adapter.CheckResult{
			Status: status.WithPermissionDenied("Authorization Rejected By Policy\n"),
		}, nil
	}

	// Accepted
	return adapter.CheckResult{Status: status.OK}, nil
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
