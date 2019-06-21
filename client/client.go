package client

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"log"
	"sync"
	"time"
)

var (
	ETCDADDR = "127.0.0.1:2379"
	Rule     = &DataService{
		updateCH: make(chan struct{}, 1),
	}
)

type DataService struct {
	sync.RWMutex
	data     string
	updateCH chan struct{}
}

//初始化启动函数
func (s *DataService) Run() error {

	go func() {
		for {
			s.registerData()
			time.Sleep(time.Second)
		}
	}()

	return nil
}

func (s *DataService) registerData() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDADDR},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.Logger.Fatal("fail to create etcd client.", "error", err)
		return
	}
	defer cli.Close()
	resp, err := cli.Grant(context.TODO(), 10)
	if err != nil {
		log.Logger.Fatal("fail to execute grant.", "error", err)
		return
	}
	_, err = cli.Put(context.TODO(), "test/data/client", s.data, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Logger.Fatal("fail to put key.", "error", err)
		return
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		log.Logger.Fatal("fail to keep alive.", "error", err)
		return
	}

	defer cli.Revoke(context.TODO(), resp.ID)

	for {
		select {
		case <-cli.Ctx().Done():
			log.Logger.Fatal("etcd server closed.", "error", err)
			return
		case ka, ok := <-ch:
			if !ok {
				log.Logger.Println("keep alive channel closed")
				return
			} else {
				log.Logger.Println("Recv reply from service ttl.", "ttl", ka.TTL)
			}
		case <-s.updateCH:
			log.Logger.Println("update data to etcd.")
			_, err = cli.Put(context.TODO(), "test/data/client", s.data, clientv3.WithLease(resp.ID))
			if err != nil {
				log.Logger.Fatal("fail to update key", "error", err)
				return
			}

		}
	}
}

//更新后通知
func (s *DataService) Update() {
	s.data = "test_data_message"
	select {
	case s.updateCH <- struct{}{}:
	default:
		log.Logger.Println("rule update ch is full.")
	}
}
