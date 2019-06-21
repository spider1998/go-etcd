package server

import (
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"time"
)

type DataDiscoverer interface {
	Discover(func())
	FindData() map[string]TestData
	Close()
}

func NewETCDRuleDiscoverer(prefix string, endpoints []string, logger Logger) *ETCDRuleDiscoverer {
	d := new(ETCDRuleDiscoverer)
	d.prefix = prefix
	d.endpoints = endpoints
	d.logger = logger
	d.closed = make(chan struct{})
	return d
}

type ETCDRuleDiscoverer struct {
	prefix    string
	endpoints []string
	logger    Logger
	closed    chan struct{}
}

func (d *ETCDRuleDiscoverer) Discover(fn func()) {
	for {
		select {
		case <-d.closed:
			return
		default:
			d.run(fn)
		}
		time.Sleep(time.Second)
	}
}

func (d *ETCDRuleDiscoverer) run(fn func()) {
	d.logger.Info("create rule watcher.")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   d.endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		d.logger.Error("fail to create client.", "error", err)
		return
	}
	defer cli.Close()
	ch := cli.Watch(context.Background(), d.prefix, clientv3.WithPrefix())
	d.logger.Info("begin watching data changing.")
	select {
	case <-ch:
		d.logger.Info("data changed.")
		if fn != nil {
			d.logger.Info("send notify to functions.")
			fn()
		}
	case <-d.closed:
		d.logger.Info("closed watcher.")
		return
	}
}

func (d *ETCDRuleDiscoverer) FindData() (dataMap map[string]TestData) {
	d.logger.Info("try to find data.")
	URLMap, err := d.findData()
	if err != nil {
		d.logger.Error("fail to find Data.", "error", err)
		return
	}
	dataMap = make(map[string]TestData)
	d.logger.Info("found data urls.", "urls", URLMap)
	for k, v := range URLMap {
		var data TestData
		data, err = d.fetchData(v)
		if err != nil {
			d.logger.Error("fail to fetch rules.", "key", k, "url", v, "error", err)
			continue
		}
		dataMap[k] = data
	}
	return
}

func (d *ETCDRuleDiscoverer) Close() {
	close(d.closed)
}

func (d *ETCDRuleDiscoverer) findData() (datas map[string]string, err error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   d.endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		d.logger.Error("fail to create etcd client.", "error", err)
		return
	}
	defer cli.Close()

	resp, err := cli.Get(context.Background(), d.prefix, clientv3.WithPrefix(), clientv3.WithLimit(128))
	if err != nil {
		d.logger.Error("fail to get etcd values.", "error", err)
		return
	}
	datas = make(map[string]string)
	for _, kv := range resp.Kvs {
		datas[string(kv.Key)] = string(kv.Value)
	}
	return
}

func (d *ETCDRuleDiscoverer) fetchData(URL string) (data TestData, err error) {
	return TestData(URL), nil
}
