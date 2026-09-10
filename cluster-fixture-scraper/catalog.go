/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clock "k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	awsv1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	awscache "github.com/aws/karpenter-provider-aws/pkg/cache"
	"github.com/aws/karpenter-provider-aws/pkg/fake"
	awsoptions "github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancetype"
	"github.com/aws/karpenter-provider-aws/pkg/providers/pricing"
	"github.com/aws/karpenter-provider-aws/pkg/providers/subnet"
	awstest "github.com/aws/karpenter-provider-aws/pkg/test"

	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	_ "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	_ "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// catalogExporter is a seam for command tests. Production uses
// exportFromCloudProvider, while tests can provide a deterministic catalog
// without making AWS API calls.
var catalogExporter = exportFromCloudProvider

func exportFromCloudProvider(ctx context.Context, fixture *clusterfixture.Fixture, region string) (*clusterfixture.Catalog, error) {
	ctx = coreoptions.ToContext(ctx, coretest.Options())
	ctx = awsoptions.ToContext(ctx, awstest.Options())

	nodeClasses, err := clusterfixture.DecodeYAMLFile[*awsv1.EC2NodeClass](filepath.Join(fixture.Dir, "nodeclasses.yaml"))
	if err != nil {
		return nil, fmt.Errorf("decoding nodeclasses: %w", err)
	}
	if len(nodeClasses) == 0 {
		return nil, fmt.Errorf("no EC2NodeClasses found in fixture %q", fixture.Dir)
	}

	objects := make([]client.Object, 0, len(fixture.NodePools)+len(nodeClasses))
	for _, np := range fixture.NodePools {
		objects = append(objects, np)
	}
	for _, nc := range nodeClasses {
		objects = append(objects, nc)
	}
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		Build()

	cfg, err := loadAWSConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	ec2Client := ec2.NewFromConfig(cfg)

	instanceTypeCache := cache.New(awscache.DefaultTTL, awscache.DefaultCleanupInterval)
	offeringCache := cache.New(awscache.DefaultTTL, awscache.DefaultCleanupInterval)
	discoveredCapacityCache := cache.New(awscache.DiscoveredCapacityCacheTTL, awscache.DefaultCleanupInterval)
	unavailableOfferingsCache := awscache.NewUnavailableOfferings()
	pricingAPI := &fake.PricingAPI{}
	pricingProvider := pricing.NewDefaultProvider(pricingAPI, ec2Client, cfg.Region, false)
	subnetProvider := subnet.NewDefaultProvider(ec2Client, cache.New(awscache.DefaultTTL, awscache.DefaultCleanupInterval), cache.New(awscache.AvailableIPAddressTTL, awscache.DefaultCleanupInterval), cache.New(awscache.AssociatePublicIPAddressTTL, awscache.DefaultCleanupInterval))
	capacityReservationProvider := capacityreservation.NewProvider(ec2Client, &clock.RealClock{}, cache.New(awscache.DefaultTTL, awscache.DefaultCleanupInterval), cache.New(24*time.Hour, awscache.DefaultCleanupInterval))
	instanceTypesResolver := instancetype.NewDefaultResolver(cfg.Region)
	itProvider := instancetype.NewDefaultProvider(
		instanceTypeCache,
		offeringCache,
		discoveredCapacityCache,
		ec2Client,
		subnetProvider,
		pricingProvider,
		capacityReservationProvider,
		unavailableOfferingsCache,
		instanceTypesResolver,
	)
	if err := itProvider.UpdateInstanceTypes(ctx); err != nil {
		return nil, fmt.Errorf("updating instance types: %w", err)
	}
	if err := itProvider.UpdateInstanceTypeOfferings(ctx); err != nil {
		return nil, fmt.Errorf("updating instance type offerings: %w", err)
	}

	perPool := map[string][]*cloudprovider.InstanceType{}
	for _, np := range fixture.NodePools {
		if np.Spec.Template.Spec.NodeClassRef == nil {
			continue
		}
		nodeClass := &awsv1.EC2NodeClass{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: np.Spec.Template.Spec.NodeClassRef.Name}, nodeClass); err != nil {
			return nil, fmt.Errorf("getting nodeclass %q for pool %q: %w", np.Spec.Template.Spec.NodeClassRef.Name, np.Name, err)
		}
		its, err := itProvider.List(ctx, nodeClass)
		if err != nil {
			return nil, fmt.Errorf("listing instance types for pool %q: %w", np.Name, err)
		}
		perPool[np.Name] = its
	}
	if len(perPool) == 0 {
		return nil, fmt.Errorf("no instance types resolved for fixture node pools")
	}
	return clusterfixture.BuildCatalogFromInstanceTypes(perPool), nil
}

func loadAWSConfig(ctx context.Context, region string) (aws.Config, error) {
	if region == "" {
		return aws.Config{}, fmt.Errorf("AWS region is required")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return aws.Config{}, fmt.Errorf("loading AWS config: %w", err)
	}
	if cfg.Region == "" {
		return aws.Config{}, fmt.Errorf("AWS region is required")
	}
	return cfg, nil
}
