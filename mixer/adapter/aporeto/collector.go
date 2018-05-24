package aporeto

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bluele/gcache"
	"istio.io/istio/mixer/template/authorization"

	"github.com/aporeto-inc/gaia/v1/golang"
	"github.com/aporeto-inc/gaia/v1/golang/constants"
	"github.com/aporeto-inc/manipulate"
	"github.com/aporeto-inc/trireme-lib/collector"
	"github.com/aporeto-inc/trireme-lib/policy"
)

var puCache = gcache.New(8192).LRU().Expiration(60 * time.Second).Build()

func collectFlow(ctx context.Context, m manipulate.Manipulator, namespace string, serviceID string, action policy.ActionType, method, path string, insts *authorization.Instance) {
	retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	sourceType := collector.EnpointTypePU
	destType := collector.EnpointTypePU

	source, ok := insts.Subject.Properties["k8s:source"]
	if !ok {
		source = "default"
		sourceType = collector.EndPointTypeExteranlIPAddress
	}
	destination, ok := insts.Action.Properties["k8s:destination"]
	if !ok {
		destination = "default"
		destType = collector.EndPointTypeExteranlIPAddress
	}

	_, sourceID, err := aporetoID(ctx, m, namespace, source.(string))
	if err != nil {
		sourceID = "default"
	}
	destNS, destinationID, err := aporetoID(ctx, m, namespace, destination.(string))
	if err != nil {
		destinationID = "default"
	}

	tags := map[string]string{
		constants.StatsTagKeyNamespace:       destNS,
		constants.StatsTagKeyDestinationID:   destinationID,
		constants.StatsTagKeyDestinationType: destType.String(),
		constants.StatsTagKeyDestinationPort: "80",
		constants.StatsTagKeySourceID:        sourceID,
		constants.StatsTagKeySourceType:      sourceType.String(),
		constants.StatsTagKeyEncrypted:       "y",
		constants.StatsTagKeyAction:          action.String(),
		constants.StatsTagKeyServiceID:       serviceID,
		constants.StatsTagKeyURI:             method + " " + path,
	}

	fields := map[string]interface{}{
		constants.StatsTagKeyL4Protocol:      uint8(6),
		constants.StatsTagKeyServiceType:     policy.ServiceHTTP,
		constants.StatsTagKeyPolicyNamespace: destNS,
	}

	report := gaia.NewReport()
	report.Kind = gaia.ReportKindFlow
	report.Tags = tags
	report.Timestamp = time.Now()
	report.Value = float64(1)
	report.Fields = fields
	mctx := manipulate.NewContext()

	if err := manipulate.Retry(
		retryCtx,
		func() error { return m.Create(mctx, report) },
		func(t int, e error) error { return nil },
	); err != nil {
		return
	}
}

func aporetoID(retryCtx context.Context, m manipulate.Manipulator, namespace string, pod string) (string, string, error) {
	podURL, err := url.Parse(pod)
	if err != nil {
		return "", "", fmt.Errorf("Error parsing pod as url: %s", err)
	}
	if podURL.Scheme != "kubernetes" {
		return "", "", fmt.Errorf("Invalid POD URI")
	}
	parts := strings.SplitN(podURL.Host, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("Invalid POD path: %s", podURL.Host)
	}
	k8sname := parts[0]
	k8snamespace := parts[1]

	id, err := puCache.Get(podURL.Host)
	if err == nil {
		return namespace, id.(string), nil
	}

	mctx := manipulate.NewContext().WithContext(retryCtx)
	mctx.Namespace = namespace
	mctx.Recursive = true
	mctx.Parameters.Add("tag", "@sys:k8s:name="+k8sname)
	mctx.Parameters.Add("tag", "@sys:k8s:namespace="+k8snamespace)

	pus := gaia.ProcessingUnitsList{}
	if err := manipulate.Retry(retryCtx, func() error { return m.RetrieveMany(mctx, &pus) }, nil); err != nil {
		return "", "", fmt.Errorf("Error reading information from aporeto service: %s", err)
	}
	if len(pus) == 0 {
		return "", "", fmt.Errorf("PU not found in Aporeto service")
	}
	if err := puCache.Set(podURL.Host, pus[0].ID); err != nil {
		return "", "", nil
	}

	return pus[0].Namespace, pus[0].ID, nil
}
