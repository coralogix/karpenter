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

// catalogtool exports a production-scale instance type catalog for cluster fixture benchmarks.
//
// It resolves instance types the same way Karpenter does in production: EC2 DescribeInstanceTypes
// plus offerings for each EC2NodeClass subnet zone. Node overlays are not applied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
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
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancetype"
	"github.com/aws/karpenter-provider-aws/pkg/providers/pricing"
	"github.com/aws/karpenter-provider-aws/pkg/providers/subnet"
	awstest "github.com/aws/karpenter-provider-aws/pkg/test"
	awsoptions "github.com/aws/karpenter-provider-aws/pkg/operator/options"

	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	_ "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	_ "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func main() {
	fixtureDir := flag.String("fixture", "", "path to cluster fixture directory")
	output := flag.String("output", "", "path to write instance-types.json")
	region := flag.String("region", "", "AWS region (default: from AWS config)")
	fromNodes := flag.Bool("from-nodes", false, "build catalog from fixture nodes only (legacy)")
	flag.Parse()

	if *fixtureDir == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: catalogtool --fixture <dir> --output <file>")
		os.Exit(2)
	}

	fixture, err := clusterfixture.Load(*fixtureDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading fixture: %v\n", err)
		os.Exit(1)
	}

	var catalog *clusterfixture.Catalog
	if *fromNodes {
		catalog = clusterfixture.BuildCatalog(fixture)
	} else {
		catalog, err = exportFromCloudProvider(context.Background(), fixture, *region)
		if err != nil {
			fmt.Fprintf(os.Stderr, "exporting instance catalog: %v\n", err)
			os.Exit(1)
		}
	}

	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshaling catalog: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "writing catalog: %v\n", err)
		os.Exit(1)
	}

	total := 0
	for _, names := range catalog.NodePoolInstanceTypes {
		total += len(names)
	}
	fmt.Printf("wrote %s (%d instance type specs, %d node pool assignments)\n", *output, len(catalog.InstanceTypeSpecs), total)
}

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

	cfg, err := loadAWSConfig(ctx, fixture, region)
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

func loadAWSConfig(ctx context.Context, fixture *clusterfixture.Fixture, regionFlag string) (aws.Config, error) {
	region := regionFlag
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = clusterfixture.RegionFromFixture(fixture)
	}
	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("loading AWS config: %w", err)
	}
	if cfg.Region == "" {
		return aws.Config{}, fmt.Errorf("AWS region is required (set --region, AWS_REGION, metadata.json region, or node region label)")
	}
	return cfg, nil
}
