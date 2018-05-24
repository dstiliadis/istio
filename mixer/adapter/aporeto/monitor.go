package aporeto

import (
	"context"

	"github.com/aporeto-inc/elemental"
	"github.com/aporeto-inc/gaia/v1/golang"
	"github.com/aporeto-inc/manipulate"
	"github.com/aporeto-inc/manipulate/maniphttp"
	"github.com/aporeto-inc/trireme-lib/controller/pkg/urisearch"
)

// Monitor is an Aporeto monitor to synchronize policy.
type Monitor struct {
	manipulator manipulate.Manipulator
	h           *handler
	namespace   string
}

// NewMonitor instantiates a new event monitor
func NewMonitor(m manipulate.Manipulator, h *handler, namespace string) *Monitor {
	return &Monitor{
		manipulator: m,
		h:           h,
		namespace:   namespace,
	}
}

func (m *Monitor) listen(ctx context.Context) {

	filter := elemental.NewPushFilter()
	filter.FilterIdentity(gaia.ServiceIdentity.Name)
	filter.FilterIdentity(gaia.RESTAPISpecIdentity.Name)

	subscriber := maniphttp.NewSubscriber(m.manipulator, true)
	subscriber.Start(ctx, filter)

	for {
		select {
		case evt := <-subscriber.Events():
			m.eventHandler(ctx, evt)
		case err := <-subscriber.Errors():
			if manipulate.IsDisconnectedError(err) {
				return
			}
		case status := <-subscriber.Status():
			switch status {
			case manipulate.SubscriberStatusDisconnection:
				break
			case manipulate.SubscriberStatusReconnection:
				m.processAllServices(ctx)
			case manipulate.SubscriberStatusFinalDisconnection:
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// eventHandler is called every time there is a new event from the Aporeto policy
// system.
func (m *Monitor) eventHandler(ctx context.Context, event *elemental.Event) {
	switch event.Identity {
	case gaia.ServiceIdentity.Name:
		service := gaia.NewService()
		if err := event.Decode(&service); err != nil {
			return
		}
		m.processServiceEvent(ctx, event, service)
	case gaia.RESTAPISpecIdentity.Name:
		m.processAllServices(ctx)
	}
}

// processServiceEvent processes service update events. It will update the data structures
// of the handler.
func (m *Monitor) processServiceEvent(ctx context.Context, event *elemental.Event, service *gaia.Service) {
	switch event.Type {
	case elemental.EventCreate, elemental.EventUpdate:
		if err := RetrieveAPISpecInService(ctx, m.manipulator, service); err != nil {
			return
		}
		cache := urisearch.NewAPICache(convertPolicyToRules(service.Endpoints), service.ID, false)

		m.h.Lock()
		defer m.h.Unlock()
		m.h.serviceMap[service.Name] = cache

	case elemental.EventDelete:
		m.h.Lock()
		defer m.h.Unlock()
		delete(m.h.serviceMap, service.Name)
	}
}

// processAllServices will re-sync all services.
func (m *Monitor) processAllServices(ctx context.Context) {
	serviceMap, err := RetrieveServices(ctx, m.manipulator, m.namespace)
	if err != nil {
		return
	}
	m.h.Lock()
	defer m.h.Unlock()
	m.h.serviceMap = serviceMap
}
