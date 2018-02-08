// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package environment

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/coreos/etcd/embed"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/kvconfig"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
)

var (
	errNilRetention  = errors.New("namespace retention options cannot be empty")
	errMissingConfig = errors.New("must supply service or static config")
)

// Configuration is a configuration that can be used to create namespaces, a topology, and kv store
type Configuration struct {
	// StaticConfiguration is used for running M3DB with a static config
	Static *StaticConfiguration `yaml:"static"`

	// KV sets the kv configuration for either a clustered or embedded etcd
	KV *KVConfig `yaml:"kv"`
}

// KVConfig sets the configs for the KV client and server
type KVConfig struct {
	Mode   string                    `yaml:"mode"`
	Client *etcdclient.Configuration `yaml:"client"`
	Server *EmbeddedKV               `yaml:"server"`
}

// EmbeddedKV defines specific fields for the embedded kv server
type EmbeddedKV struct {
	Dir              string        `yaml:"dir"`
	APUrls           []string      `yaml:"initial-advertise-peer-urls"`
	ACUrls           []string      `yaml:"advertise-client-urls"`
	LPUrls           []string      `yaml:"listen-peer-urls"`
	LCUrls           []string      `yaml:"listen-client-urls"`
	InitialCluster   string        `yaml:"initial-cluster"`
	Name             string        `yaml:"name"`
	NamespaceTimeout time.Duration `yaml:"namespace-timeout"`
}

// StaticConfiguration is used for running M3DB with a static config
type StaticConfiguration struct {
	Namespaces     []StaticNamespaceConfiguration `yaml:"namespaces"`
	TopologyConfig *topology.StaticConfiguration  `yaml:"topology"`
	ListenAddress  string                         `yaml:"listenAddress"`
}

// StaticNamespaceConfiguration sets the static namespace
type StaticNamespaceConfiguration struct {
	Name      string                    `yaml:"name"`
	Options   *StaticNamespaceOptions   `yaml:"options"`
	Retention *StaticNamespaceRetention `yaml:"retention"`
}

// StaticNamespaceOptions sets namespace options- if nil, default is used
type StaticNamespaceOptions struct {
	NeedsBootstrap      bool `yaml:"needsBootstrap"`
	NeedsFlush          bool `yaml:"needsFlush"`
	WritesToCommitLog   bool `yaml:"writesToCommitLog"`
	NeedsFilesetCleanup bool `yaml:"needsFilesetCleanup"`
	NeedsRepair         bool `yaml:"needsRepair"`
}

// StaticNamespaceRetention sets the retention per namespace (required)
type StaticNamespaceRetention struct {
	RetentionPeriod                     time.Duration `yaml:"retentionPeriod"`
	BlockSize                           time.Duration `yaml:"blockSize"`
	BufferFuture                        time.Duration `yaml:"bufferFuture"`
	BufferPast                          time.Duration `yaml:"bufferPast"`
	BlockDataExpiry                     bool          `yaml:"blockDataExpiry"`
	BlockDataExpiryAfterNotAccessPeriod time.Duration `yaml:"blockDataExpiryAfterNotAccessPeriod"`
}

// ConfigureResults stores initializers and kv store for dynamic and static configs
type ConfigureResults struct {
	NamespaceInitializer namespace.Initializer
	TopologyInitializer  topology.Initializer
	KVStore              kv.Store
}

// ConfigurationParameters are options used to create new ConfigureResults
type ConfigurationParameters struct {
	InstrumentOpts   instrument.Options
	HashingSeed      uint32
	HostID           string
	NamespaceTimeout time.Duration
}

// Configure creates a new ConfigureResults
func (c Configuration) Configure(cfgParams ConfigurationParameters) (ConfigureResults, error) {

	var emptyConfig ConfigureResults

	configSvcClientOpts := c.KV.Client.NewOptions().
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetServiceDiscoveryConfig(c.KV.Client.SDConfig)
	configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
	if err != nil {
		err = fmt.Errorf("could not create m3cluster client: %v", err)
		return emptyConfig, err
	}

	switch {
	case c.Static == nil:
		dynamicOpts := namespace.NewDynamicOptions().
			SetInstrumentOptions(cfgParams.InstrumentOpts).
			SetConfigServiceClient(configSvcClient).
			SetNamespaceRegistryKey(kvconfig.NamespacesKey).
			SetInitTimeout(cfgParams.NamespaceTimeout)
		nsInit := namespace.NewDynamicInitializer(dynamicOpts)

		serviceID := services.NewServiceID().
			SetName(c.KV.Client.Service).
			SetEnvironment(c.KV.Client.Env).
			SetZone(c.KV.Client.Zone)

		topoOpts := topology.NewDynamicOptions().
			SetConfigServiceClient(configSvcClient).
			SetServiceID(serviceID).
			SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
			SetInstrumentOptions(cfgParams.InstrumentOpts).
			SetHashGen(sharding.NewHashGenWithSeed(cfgParams.HashingSeed))
		topoInit := topology.NewDynamicInitializer(topoOpts)

		kv, err := configSvcClient.KV()
		if err != nil {
			err = fmt.Errorf("could not create KV client, %v", err)
			return emptyConfig, err
		}

		configureResults := ConfigureResults{
			NamespaceInitializer: nsInit,
			TopologyInitializer:  topoInit,
			KVStore:              kv,
		}
		return configureResults, nil

	case c.Static != nil:
		nsList := []namespace.Metadata{}
		for _, ns := range c.Static.Namespaces {
			md, err := newNamespaceMetadata(ns)
			if err != nil {
				err = fmt.Errorf("unable to create metadata for static config: %v", err)
				return emptyConfig, err
			}
			nsList = append(nsList, md)
		}

		nsInitStatic := namespace.NewStaticInitializer(nsList)

		shardSet, hostShardSets, err := newStaticShardSet(c.Static.TopologyConfig.Shards, c.Static.ListenAddress, cfgParams.HostID)
		if err != nil {
			err = fmt.Errorf("unable to create shard set for static config: %v", err)
			return emptyConfig, err
		}
		staticOptions := topology.NewStaticOptions().
			SetReplicas(1).
			SetHostShardSets(hostShardSets).
			SetShardSet(shardSet)

		topoInit := topology.NewStaticInitializer(staticOptions)

		kv, err := configSvcClient.KV()
		if err != nil {
			err = fmt.Errorf("could not create KV client, %v", err)
			return emptyConfig, err
		}

		configureResults := ConfigureResults{
			NamespaceInitializer: nsInitStatic,
			TopologyInitializer:  topoInit,
			KVStore:              kv,
		}
		return configureResults, nil

	default:
		return emptyConfig, errMissingConfig
	}
}

func newStaticShardSet(numShards int, listenAddress, hostID string) (sharding.ShardSet, []topology.HostShardSet, error) {
	var (
		shardSet      sharding.ShardSet
		hostShardSets []topology.HostShardSet
		shardIDs      []uint32
		err           error
	)

	for i := uint32(0); i < uint32(numShards); i++ {
		shardIDs = append(shardIDs, i)
	}

	shards := sharding.NewShards(shardIDs, shard.Available)
	shardSet, err = sharding.NewShardSet(shards, sharding.DefaultHashFn(1))
	if err != nil {
		return nil, nil, err
	}

	host := topology.NewHost(hostID, listenAddress)
	hostShardSet := topology.NewHostShardSet(host, shardSet)
	hostShardSets = append(hostShardSets, hostShardSet)

	return shardSet, hostShardSets, nil
}

func newNamespaceMetadata(cfg StaticNamespaceConfiguration) (namespace.Metadata, error) {
	if cfg.Retention == nil {
		return nil, errNilRetention
	}
	if cfg.Options == nil {
		cfg.Options = &StaticNamespaceOptions{
			NeedsBootstrap:      true,
			NeedsFilesetCleanup: true,
			NeedsFlush:          true,
			NeedsRepair:         true,
			WritesToCommitLog:   true,
		}
	}
	md, err := namespace.NewMetadata(
		ident.StringID(cfg.Name),
		namespace.NewOptions().
			SetNeedsBootstrap(cfg.Options.NeedsBootstrap).
			SetNeedsFilesetCleanup(cfg.Options.NeedsFilesetCleanup).
			SetNeedsFlush(cfg.Options.NeedsFlush).
			SetNeedsRepair(cfg.Options.NeedsRepair).
			SetWritesToCommitLog(cfg.Options.WritesToCommitLog).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(cfg.Retention.BlockSize).
					SetRetentionPeriod(cfg.Retention.RetentionPeriod).
					SetBufferFuture(cfg.Retention.BufferFuture).
					SetBufferPast(cfg.Retention.BufferPast).
					SetBlockDataExpiry(cfg.Retention.BlockDataExpiry).
					SetBlockDataExpiryAfterNotAccessedPeriod(cfg.Retention.BlockDataExpiryAfterNotAccessPeriod)))
	if err != nil {
		return nil, err
	}

	return md, nil
}

// GetETCDConfig creates a new embedded etcd config from kv config
func GetETCDConfig(kvCfg *EmbeddedKV) (*embed.Config, error) {
	newKVCfg := embed.NewConfig()
	newKVCfg.Dir = kvCfg.Dir
	APUrls, err := convertToURLs(kvCfg.APUrls)
	if err != nil {
		return nil, errors.New("unable to convert APUrls")
	}
	ACUrls, err := convertToURLs(kvCfg.ACUrls)
	if err != nil {
		return nil, errors.New("unable to convert ACUrls")
	}
	LPUrls, err := convertToURLs(kvCfg.LPUrls)
	if err != nil {
		return nil, errors.New("unable to convert LPUrls")
	}
	LCUrls, err := convertToURLs(kvCfg.LCUrls)
	if err != nil {
		return nil, errors.New("unable to convert LCUrls")
	}
	newKVCfg.APUrls = APUrls
	newKVCfg.ACUrls = ACUrls
	newKVCfg.LPUrls = LPUrls
	newKVCfg.LCUrls = LCUrls
	newKVCfg.InitialCluster = kvCfg.InitialCluster
	newKVCfg.Name = kvCfg.Name

	return newKVCfg, nil
}

func convertToURLs(rawURLs []string) ([]url.URL, error) {
	var urls []url.URL
	for _, u := range rawURLs {
		parsed, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		urls = append(urls, *parsed)
	}
	return urls, nil
}
