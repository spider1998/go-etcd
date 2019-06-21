package server

import (
	"sync"
)

type TestData string

type DataService struct {
	sync.RWMutex
	watcher        *Watcher
	datas          []TestData
	ruleDiscoverer DataDiscoverer
}

var (
	Log               Logger
	DataServicePrefix = "test/data"
	DataChangeKey     = Key("test_data")
	ETCDADD           = "127.0.0.1:2379"
)

//启动函数
func (s *DataService) Run() (err error) {
	s.watcher = NewWatcher(1024, Log.New("module", "watcher"))
	s.datas = nil
	s.ruleDiscoverer = NewETCDRuleDiscoverer(DataServicePrefix, []string{ETCDADD}, Log)

	s.watcher.Start()

	s.watcher.OnNotify(DataChangeKey, func() {
		s.reloadData()
	})

	go s.ruleDiscoverer.Discover(func() {
		s.watcher.Notify(DataChangeKey)
	})

	err = s.reloadData()
	if err != nil {
		return
	}
	return
}

func (s *DataService) reloadData() error {
	dataMap := s.ruleDiscoverer.FindData()
	Log.Debug("reload rules.", "datamap", dataMap)
	var datas []TestData
	for _, r := range dataMap {
		datas = append(datas, r)
	}
	s.datas = datas
	return nil
}
