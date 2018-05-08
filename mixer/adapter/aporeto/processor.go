package aporeto

import (
	"context"
	"fmt"

	"github.com/aporeto-inc/trireme-lib/controller/pkg/urisearch"
	"istio.io/istio/mixer/template/authorization"

	"github.com/aporeto-inc/gaia/v1/golang"
	"github.com/aporeto-inc/gaia/v1/golang/types"
	"github.com/aporeto-inc/manipulate"
	"github.com/aporeto-inc/trireme-lib/policy"
)

// RetrieveAllServiceObjects retrieves all the services associated with the namespace.
func RetrieveAllServiceObjects(ctx context.Context, m manipulate.Manipulator, namespace string) (gaia.ServicesList, error) {

	services := gaia.ServicesList{}

	mctx := manipulate.NewContext().WithContext(ctx)
	mctx.Namespace = namespace
	mctx.Recursive = true

	if err := manipulate.Retry(ctx, func() error { return m.RetrieveMany(mctx, &services) }, nil); err != nil {
		return nil, err
	}

	return services, nil
}

// RetrieveAPISpecInService retrieves the API specification for a service.
func RetrieveAPISpecInService(ctx context.Context, m manipulate.Manipulator, service *gaia.Service) error {

	apis := gaia.RESTAPISpecsList{}

	mctx := manipulate.NewContext().WithContext(ctx)
	mctx.Namespace = service.Namespace
	mctx.Parent = service

	if err := manipulate.Retry(ctx, func() error { return m.RetrieveMany(mctx, &apis) }, nil); err != nil {
		return err
	}

	if len(apis) == 0 {
		return fmt.Errorf("no associated APIs")
	}

	service.Endpoints = apis[0].Endpoints

	return nil
}

// RetrieveServices retrieves all the services in the namespace
func RetrieveServices(ctx context.Context, m manipulate.Manipulator, namespace string) (map[string]*urisearch.APICache, error) {

	serviceMap := map[string]*urisearch.APICache{}

	services, err := RetrieveAllServiceObjects(ctx, m, namespace)
	if err != nil {
		return nil, err
	}

	for _, service := range services {
		if err := RetrieveAPISpecInService(ctx, m, service); err != nil {
			return nil, err
		}
		serviceMap[service.Name] = urisearch.NewAPICache(convertPolicyToRules(service.Endpoints), service.Name, false)
	}

	return serviceMap, nil
}

// convertPolicyToRules convers a policy to a list of rules
func convertPolicyToRules(endpoints types.ExposedAPIList) []*policy.HTTPRule {
	rules := []*policy.HTTPRule{}

	for _, e := range endpoints {
		rules = append(rules, &policy.HTTPRule{
			Scopes:  e.Scopes,
			Methods: e.Methods,
			URIs:    []string{e.URI},
			Public:  e.Public,
		})
	}
	return rules
}

func mapLabels(insts *authorization.Instance) []string {
	labels := []string{}
	if l, ok := insts.Subject.Properties["labels"]; ok && l != nil {
		labelMap := l.(map[string]string)
		for k, v := range labelMap {
			labels = append(labels, k+"="+v)
		}
	}
	labels = append(labels, "@user="+insts.Subject.User)
	labels = append(labels, "@groups="+insts.Subject.Groups)
	if l, ok := insts.Subject.Properties["k8s:namespace"]; ok {
		labels = append(labels, "@k8s:namespace="+l.(string))
	}
	if l, ok := insts.Subject.Properties["k8s:service"]; ok {
		labels = append(labels, "@k8s:service="+l.(string))
	}
	if l, ok := insts.Subject.Properties["k8s:serviceAccount"]; ok {
		labels = append(labels, "@k8s:serviceAccount="+l.(string))
	}
	return labels
}
