package azuresf

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"

	"github.com/prometheus/prometheus/config"
)

const (
	azureSFLabel            = model.MetaLabelPrefix + "azure_sf_"
	azureSFLabelApplication = azureSFLabel + "application"
	azureSFLabelService     = azureSFLabel + "service"
	azureSFLabelPartition   = azureSFLabel + "partition"
	azureSFLabelNode        = azureSFLabel + "node"
	azureSFLabelEndpoint    = azureSFLabel + "endpoint_"
)

var (
	azureSFSDRefreshFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_sd_azure__sf_refresh_failures_total",
			Help: "Number of Azure Service Fabric SD refresh failures.",
		})
	azureSFSDRefreshDuration = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name: "prometheus_sd_azure_sf_refresh_duration_seconds",
			Help: "The duration of a Azure Service Fabric SD refresh in seconds.",
		})
)

func init() {
	prometheus.MustRegister(azureSFSDRefreshFailuresCount)
	prometheus.MustRegister(azureSFSDRefreshDuration)
}

// Discovery periodically performs Azure Service Fabric SD requests.
// It implements the TargetProvider interface.
type Discovery struct {
	cfg      *config.AzureServiceFabricSDConfig
	interval time.Duration
}

func NewDiscovery(cfg *config.AzureServiceFabricSDConfig) *Discovery {
	return &Discovery{
		cfg:      cfg,
		interval: time.Duration(cfg.RefreshInterval),
	}
}

// Run implements the TargetProvider interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tg, err := d.refresh()
		if err != nil {
			log.Errorf("unable to refresh during Azure Sevice Fabric discovery: %s", err)
		} else {
			select {
			case <-ctx.Done():
			case ch <- []*config.TargetGroup{tg}:
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (d *Discovery) refresh() (tg *config.TargetGroup, err error) {
	t0 := time.Now()
	defer func() {
		azureSFSDRefreshDuration.Observe(time.Since(t0).Seconds())
		if err != nil {
			azureSFSDRefreshFailuresCount.Inc()
		}
	}()

	client := &sfClient{
		clusterURL: d.cfg.ClusterURL,
	}

	applicationEntries, err := getApplicationEntries(client)
	if err != nil {
		return nil, err
	}

	targets := makeTargets(applicationEntries)

	log.Debugf("Azure Service Fabric discovery completed.")

	return &config.TargetGroup{
		Targets: targets,
	}, nil
}

type applicationEntry struct {
	application    string
	serviceEntries []serviceEntry
	err            error
}

func getApplicationEntries(client *sfClient) ([]applicationEntry, error) {
	applications, err := client.getApplications()
	if err != nil {
		return nil, fmt.Errorf("unable to get applications: %s", err)
	}

	entriesChan := make(chan applicationEntry, len(applications))
	for _, application := range applications {
		go func(application string) {
			serviceEntries, err := getServiceEntries(client, application)
			if err != nil {
				entriesChan <- applicationEntry{err: err}
			} else {
				entriesChan <- applicationEntry{
					application:    application,
					serviceEntries: serviceEntries,
				}
			}
		}(application)
	}

	applicationEntries := make([]applicationEntry, 0, len(applications))
	for range applications {
		entry := <-entriesChan
		if entry.err != nil {
			return nil, entry.err
		}
		applicationEntries = append(applicationEntries, entry)
	}

	return applicationEntries, nil
}

type serviceEntry struct {
	service           string
	partitionsEntries []partitionEntry
	err               error
}

func getServiceEntries(client *sfClient, application string) ([]serviceEntry, error) {
	services, err := client.getServices(application)
	if err != nil {
		return nil, fmt.Errorf("unable to get services: %s", err)
	}

	entriesChan := make(chan serviceEntry, len(services))
	for _, service := range services {
		go func(service string) {
			partitionEntries, err := getPartitionsEntries(client, application, service)
			if err != nil {
				entriesChan <- serviceEntry{err: err}
			} else {
				entriesChan <- serviceEntry{
					service:           service,
					partitionsEntries: partitionEntries,
				}
			}
		}(service)
	}

	serviceEntries := make([]serviceEntry, 0, len(services))
	for range services {
		entry := <-entriesChan
		if entry.err != nil {
			return nil, entry.err
		}
		serviceEntries = append(serviceEntries, entry)
	}

	return serviceEntries, nil
}

type partitionEntry struct {
	partition     string
	nodeEndpoints []nodeAndEndpoints
	err           error
}

func getPartitionsEntries(client *sfClient, application, service string) ([]partitionEntry, error) {
	partitions, err := client.getPartitions(application, service)
	if err != nil {
		return nil, fmt.Errorf("unable to get partitions: %s", err)
	}

	entriesChan := make(chan partitionEntry, len(partitions))
	for _, partition := range partitions {
		go func(partition string) {
			nodeEndpoints, err := client.getReplicaEndpoints(application, service, partition)
			if err != nil {
				entriesChan <- partitionEntry{err: err}
			} else {
				entriesChan <- partitionEntry{
					partition:     partition,
					nodeEndpoints: nodeEndpoints,
				}
			}
		}(partition)
	}

	partitionEntries := make([]partitionEntry, 0, len(partitions))
	for range partitions {
		entry := <-entriesChan
		if entry.err != nil {
			return nil, entry.err
		}
		partitionEntries = append(partitionEntries, entry)
	}

	return partitionEntries, nil
}

func makeTargets(applicationEntries []applicationEntry) []model.LabelSet {
	// TODO - any clever way to pre-calculate the size of this slice?
	targets := make([]model.LabelSet, 0)

	for _, applicationEntry := range applicationEntries {
		for _, serviceEntry := range applicationEntry.serviceEntries {
			for _, partitionEntry := range serviceEntry.partitionsEntries {
				for _, nodeEndpoints := range partitionEntry.nodeEndpoints {
					labels := model.LabelSet{
						azureSFLabelApplication: model.LabelValue(applicationEntry.application),
						azureSFLabelService:     model.LabelValue(serviceEntry.service),
						azureSFLabelPartition:   model.LabelValue(partitionEntry.partition),
						azureSFLabelNode:        model.LabelValue(nodeEndpoints.node),
					}

					for endpointName, endpointValue := range nodeEndpoints.endpoints {
						labels[azureSFLabelEndpoint+model.LabelName(endpointName)] = model.LabelValue(endpointValue)
					}

					targets = append(targets, labels)
				}
			}
		}
	}

	return targets
}
