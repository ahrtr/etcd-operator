/*
Copyright 2024 The etcd Authors.

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
	"sync"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func memberList(eps []string) (*clientv3.MemberListResponse, error) {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		c.Close()
		cancel()
	}()

	return c.MemberList(ctx)
}

type epHealth struct {
	Ep     string `json:"endpoint"`
	Health bool   `json:"health"`
	Took   string `json:"took"`
	Error  string `json:"error,omitempty"`
}

func (eh epHealth) String() string {
	return fmt.Sprintf("endpoint: %s, health: %t, took: %s, error: %s", eh.Ep, eh.Health, eh.Took, eh.Error)
}

func clusterHealth(eps []string) ([]epHealth, error) {
	lg, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
	if err != nil {
		return nil, err
	}

	var cfgs []*clientv3.Config
	for _, ep := range eps {
		cfg := &clientv3.Config{
			Endpoints:            []string{ep},
			DialTimeout:          2 * time.Second,
			DialKeepAliveTime:    2 * time.Second,
			DialKeepAliveTimeout: 6 * time.Second,
		}

		cfgs = append(cfgs, cfg)
	}

	healthCh := make(chan epHealth, len(eps))

	var wg sync.WaitGroup
	for _, cfg := range cfgs {
		wg.Add(1)
		go func(cfg *clientv3.Config) {
			defer wg.Done()

			ep := cfg.Endpoints[0]
			cfg.Logger = lg.Named("client")
			cli, err := clientv3.New(*cfg)
			if err != nil {
				healthCh <- epHealth{Ep: ep, Health: false, Error: err.Error()}
				return
			}
			startTs := time.Now()
			// get a random key. As long as we can get the response
			// without an error, the endpoint is health.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err = cli.Get(ctx, "health")
			eh := epHealth{Ep: ep, Health: false, Took: time.Since(startTs).String()}
			if err == nil || err == rpctypes.ErrPermissionDenied {
				eh.Health = true
			} else {
				eh.Error = err.Error()
			}

			if eh.Health {
				resp, err := cli.AlarmList(ctx)
				if err == nil && len(resp.Alarms) > 0 {
					eh.Health = false
					eh.Error = "Active Alarm(s): "
					for _, v := range resp.Alarms {
						switch v.Alarm {
						case etcdserverpb.AlarmType_NOSPACE:
							// We ignore AlarmType_NOSPACE, and we need to
							// continue to perform defragmentation.
							eh.Health = true
							eh.Error = eh.Error + "NOSPACE "
						case etcdserverpb.AlarmType_CORRUPT:
							eh.Error = eh.Error + "CORRUPT "
						default:
							eh.Error = eh.Error + "UNKNOWN "
						}
					}
				} else if err != nil {
					eh.Health = false
					eh.Error = "Unable to fetch the alarm list"
				}
			}
			cancel()
			healthCh <- eh
		}(cfg)
	}
	wg.Wait()
	close(healthCh)

	var healthList []epHealth
	for h := range healthCh {
		healthList = append(healthList, h)
	}

	return healthList, nil
}

func memberStatus(ep string) (*clientv3.StatusResponse, error) {
	cfg := clientv3.Config{
		Endpoints:            []string{ep},
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		c.Close()
		cancel()
	}()

	return c.Status(ctx, ep)
}

func addMember(eps []string, peerURLs []string, learner bool) (*clientv3.MemberAddResponse, error) {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		c.Close()
		cancel()
	}()

	if learner {
		return c.MemberAddAsLearner(ctx, peerURLs)
	}

	return c.MemberAdd(ctx, peerURLs)
}

func promoteLearner(eps []string, learnerId uint64) error {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		c.Close()
		cancel()
	}()

	_, err = c.MemberPromote(ctx, learnerId)
	return err
}
