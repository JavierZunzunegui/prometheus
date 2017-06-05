package azuresf

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type sfClient struct {
	protocol   string // http / https
	clusterURL string
	client     *http.Client
}

type getApplicationsResponse struct {
	Items []struct {
		ID string `json:"Id"`
	} `json:"Items"`
}

func (c *sfClient) getApplications() ([]string, error) {
	resp, err := c.client.Get(fmt.Sprintf("%s://%s/Applications/?api-version=2.0", c.protocol, c.clusterURL))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expecting 200, got %d", resp.StatusCode)
	}

	var appResp getApplicationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&appResp); err != nil {
		return nil, err
	}

	apps := make([]string, 0, len(appResp.Items))
	for _, item := range appResp.Items {
		apps = append(apps, item.ID)
	}

	return apps, nil
}

type getServicesResponse struct {
	Items []struct {
		ID string `json:"Id"`
	} `json:"Items"`
}

func (c *sfClient) getServices(application string) ([]string, error) {
	resp, err := c.client.Get(fmt.Sprintf("%s://%s/Applications/%s/$/GetServices?api-version=2.0", c.protocol, c.clusterURL, application))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expecting 200, got %d", resp.StatusCode)
	}

	var servicesResp getServicesResponse
	if err := json.NewDecoder(resp.Body).Decode(&servicesResp); err != nil {
		return nil, err
	}

	services := make([]string, 0, len(servicesResp.Items))
	for _, item := range servicesResp.Items {
		services = append(services, item.ID)
	}

	return services, nil
}

type getPartitionsResponse struct {
	Items []struct {
		Info struct {
			ID string `json:"Id"`
		} `json:"PartitionInformation"`
	} `json:"Items"`
}

func (c *sfClient) getPartitions(application, service string) ([]string, error) {
	resp, err := c.client.Get(fmt.Sprintf("%s://%s/Applications/%s/$/GetServices/%s/$/GetPartitions?api-version=2.0", c.protocol, c.clusterURL, application, url.QueryEscape(service)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expecting 200, got %d", resp.StatusCode)
	}

	var partitionsResp getPartitionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&partitionsResp); err != nil {
		return nil, err
	}

	partitions := make([]string, 0, len(partitionsResp.Items))
	for _, item := range partitionsResp.Items {
		partitions = append(partitions, item.Info.ID)
	}

	return partitions, nil
}

type getReplicaResponse struct {
	Items []struct {
		NodeName string `json:"NodeName"`
		Address  string `json:"Address"`
	} `json:"Items"`
}

type getReplicaResponseEndpoints struct {
	Endpoints map[string]string `json:"Endpoints"`
}

type nodeAndEndpoints struct {
	node      string
	endpoints map[string]string
}

func (c *sfClient) getReplicaEndpoints(application, service, partition string) ([]nodeAndEndpoints, error) {
	resp, err := c.client.Get(fmt.Sprintf("%s://%s/Applications/%s/$/GetServices/%s/$/GetPartitions/%s/$/GetReplicas?api-version=2.0", c.protocol, c.clusterURL, application, url.QueryEscape(service), partition))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("expecting 200, got %d", resp.StatusCode)
	}

	var replicaResp getReplicaResponse
	if err := json.NewDecoder(resp.Body).Decode(&replicaResp); err != nil {
		return nil, err
	}

	nodeEndpoints := make([]nodeAndEndpoints, 0, len(replicaResp.Items))
	for _, item := range replicaResp.Items {
		var endpoints getReplicaResponseEndpoints
		if err := json.Unmarshal([]byte(item.Address), &endpoints); err != nil {
			continue // there are other valid formats for endpoints. Ignore this endpoint and move on
		}

		nodeEndpoints = append(nodeEndpoints, nodeAndEndpoints{node: item.NodeName, endpoints: endpoints.Endpoints})
	}

	return nodeEndpoints, nil
}
