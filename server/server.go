// Copyright 2020 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/juju/errors"
	"github.com/zhenghaoz/gorse/base"
	"github.com/zhenghaoz/gorse/base/encoding"
	"github.com/zhenghaoz/gorse/base/log"
	"github.com/zhenghaoz/gorse/base/parallel"
	"github.com/zhenghaoz/gorse/base/progress"
	"github.com/zhenghaoz/gorse/base/search"
	"github.com/zhenghaoz/gorse/cmd/version"
	"github.com/zhenghaoz/gorse/config"
	"github.com/zhenghaoz/gorse/model/click"
	"github.com/zhenghaoz/gorse/model/ranking"
	"github.com/zhenghaoz/gorse/protocol"
	"github.com/zhenghaoz/gorse/storage/cache"
	"github.com/zhenghaoz/gorse/storage/data"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server manages states of a server node.
type Server struct {
	RestServer
	traceConfig  config.TracingConfig
	cachePath    string
	cachePrefix  string
	dataPath     string
	dataPrefix   string
	masterClient protocol.MasterClient
	serverName   string
	masterHost   string
	masterPort   int
	testMode     bool
	cacheFile    string

	latestRankingModelVersion int64
	latestClickModelVersion   int64
	rankingIndex              *search.HNSW
	randGenerator             *rand.Rand

	syncedChan *parallel.ConditionChannel // meta synced events
	pulledChan *parallel.ConditionChannel // model pulled events

	tracer *progress.Tracer
}

// NewServer creates a server node.
func NewServer(masterHost string, masterPort int, serverHost string, serverPort int, cacheFile string) *Server {
	s := &Server{
		masterHost: masterHost,
		masterPort: masterPort,
		cacheFile:  cacheFile,
		RestServer: RestServer{
			Settings:   config.NewSettings(),
			HttpHost:   serverHost,
			HttpPort:   serverPort,
			WebService: new(restful.WebService),
			IsMaster:   false,
		},
		randGenerator: base.NewRand(time.Now().UTC().UnixNano()),

		syncedChan: parallel.NewConditionChannel(),
		pulledChan: parallel.NewConditionChannel(),
	}
	s.RestServer.SetServer(s)
	return s
}

// Serve starts a server node.
func (s *Server) Serve() {
	rand.Seed(time.Now().UTC().UnixNano())
	// open local store
	state, err := LoadLocalCache(s.cacheFile)
	if err != nil {
		if errors.Is(err, errors.NotFound) {
			log.Logger().Info("no cache file found, create a new one", zap.String("path", s.cacheFile))
		} else {
			log.Logger().Error("failed to connect local store", zap.Error(err),
				zap.String("path", s.cacheFile))
		}
	}
	if state.ServerName == "" {
		state.ServerName = base.GetRandomName(0)
		err = state.WriteLocalCache()
		if err != nil {
			log.Logger().Fatal("failed to write meta", zap.Error(err))
		}
	}
	s.serverName = state.ServerName
	log.Logger().Info("start server",
		zap.String("server_name", s.serverName),
		zap.String("server_host", s.HttpHost),
		zap.Int("server_port", s.HttpPort),
		zap.String("master_host", s.masterHost),
		zap.Int("master_port", s.masterPort))

	s.tracer = progress.NewTracer(s.serverName)

	// connect to master
	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", s.masterHost, s.masterPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Logger().Fatal("failed to connect master", zap.Error(err))
	}
	s.masterClient = protocol.NewMasterClient(conn)

	// 先启动同步，等待第一次同步完成
	syncDone := make(chan struct{})
	go func() {
		// 使用 Range 方法来监听信号
		for range s.syncedChan.C {
			close(syncDone)
			break // 只需要第一次同步信号
		}
	}()
	go s.Sync()

	// 等待第一次同步完成，确保数据库连接已建立
	select {
	case <-syncDone:
	case <-time.After(30 * time.Second):
		log.Logger().Fatal("timeout waiting for first sync")
	}

	// 启动其他后台任务
	go s.Pull()
	go s.BuildRankingIndex()
	// 启动http服务，需要等Sync中完成了database的初始化后才能启动
	container := restful.NewContainer()
	s.StartHttpServer(container)
}

func (s *Server) Shutdown() {
	err := s.HttpServer.Shutdown(context.TODO())
	if err != nil {
		log.Logger().Fatal("failed to shutdown http server", zap.Error(err))
	}
}

// Sync this server to the master.
func (s *Server) Sync() {
	defer base.CheckPanic()
	log.Logger().Info("start meta sync", zap.Duration("meta_timeout", s.Config.Master.MetaTimeout))
	firstSync := true
	for {
		var meta *protocol.Meta
		var err error
		if meta, err = s.masterClient.GetMeta(context.Background(),
			&protocol.NodeInfo{
				NodeType:      protocol.NodeType_ServerNode,
				NodeName:      s.serverName,
				HttpPort:      int64(s.HttpPort),
				BinaryVersion: version.Version,
			}); err != nil {
			log.Logger().Error("failed to get meta", zap.Error(err))
			goto sleep
		}

		// load master config
		err = json.Unmarshal([]byte(meta.Config), &s.Config)
		if err != nil {
			log.Logger().Error("failed to parse master config", zap.Error(err))
			goto sleep
		}

		// connect to data store
		if s.dataPath != s.Config.Database.DataStore || s.dataPrefix != s.Config.Database.DataTablePrefix {
			log.Logger().Info("connect data store",
				zap.String("database", log.RedactDBURL(s.Config.Database.DataStore)))
			if s.DataClient, err = data.Open(s.Config.Database.DataStore, s.Config.Database.DataTablePrefix); err != nil {
				log.Logger().Error("failed to connect data store", zap.Error(err))
				goto sleep
			}
			s.dataPath = s.Config.Database.DataStore
			s.dataPrefix = s.Config.Database.DataTablePrefix
		}

		// connect to cache store
		if s.cachePath != s.Config.Database.CacheStore || s.cachePrefix != s.Config.Database.CacheTablePrefix {
			log.Logger().Info("connect cache store",
				zap.String("database", log.RedactDBURL(s.Config.Database.CacheStore)))
			if s.CacheClient, err = cache.Open(s.Config.Database.CacheStore, s.Config.Database.CacheTablePrefix); err != nil {
				log.Logger().Error("failed to connect cache store", zap.Error(err))
				goto sleep
			}
			s.cachePath = s.Config.Database.CacheStore
			s.cachePrefix = s.Config.Database.CacheTablePrefix
		}

		// create trace provider
		if !s.traceConfig.Equal(s.Config.Tracing) {
			log.Logger().Info("create trace provider", zap.Any("tracing_config", s.Config.Tracing))
			tp, err := s.Config.Tracing.NewTracerProvider()
			if err != nil {
				log.Logger().Fatal("failed to create trace provider", zap.Error(err))
			}
			otel.SetTracerProvider(tp)
			otel.SetErrorHandler(log.GetErrorHandler())
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
			s.traceConfig = s.Config.Tracing
		}

		// check ranking model version
		s.latestRankingModelVersion = meta.RankingModelVersion
		if s.latestRankingModelVersion != s.RankingModelVersion {
			log.Logger().Info("new ranking model found",
				zap.String("old_version", encoding.Hex(s.RankingModelVersion)),
				zap.String("new_version", encoding.Hex(s.latestRankingModelVersion)))
			s.syncedChan.Signal()
		}

		// check click model version
		s.latestClickModelVersion = meta.ClickModelVersion
		if s.latestClickModelVersion != s.ClickModelVersion {
			log.Logger().Info("new click model found",
				zap.String("old_version", encoding.Hex(s.ClickModelVersion)),
				zap.String("new_version", encoding.Hex(s.latestClickModelVersion)))
			s.syncedChan.Signal()
		}

		if firstSync {
			firstSync = false
			s.syncedChan.Signal()
		}

	sleep:
		if s.testMode {
			return
		}
		time.Sleep(s.Config.Master.MetaTimeout)
	}
}

// Pull user index and ranking model from master.
func (s *Server) Pull() {
	defer base.CheckPanic()
	for range s.syncedChan.C {
		pulled := false

		// pull ranking model
		if s.latestRankingModelVersion != s.RankingModelVersion {
			log.Logger().Info("start pull ranking model")
			if rankingModelReceiver, err := s.masterClient.GetRankingModel(context.Background(),
				&protocol.VersionInfo{Version: s.latestRankingModelVersion},
				grpc.MaxCallRecvMsgSize(math.MaxInt)); err != nil {
				log.Logger().Error("failed to pull ranking model", zap.Error(err))
			} else {
				var rankingModel ranking.MatrixFactorization
				rankingModel, err = protocol.UnmarshalRankingModel(rankingModelReceiver)
				if err != nil {
					log.Logger().Error("failed to unmarshal ranking model", zap.Error(err))
				} else {
					s.RankingModel = rankingModel
					s.rankingIndex = nil
					s.RankingModelVersion = s.latestRankingModelVersion
					log.Logger().Info("synced ranking model",
						zap.String("version", encoding.Hex(s.RankingModelVersion)))
					pulled = true
				}
			}
		}

		// pull click model
		if s.latestClickModelVersion != s.ClickModelVersion {
			log.Logger().Info("start pull click model")
			if clickModelReceiver, err := s.masterClient.GetClickModel(context.Background(),
				&protocol.VersionInfo{Version: s.latestClickModelVersion},
				grpc.MaxCallRecvMsgSize(math.MaxInt)); err != nil {
				log.Logger().Error("failed to pull click model", zap.Error(err))
			} else {
				var clickModel click.FactorizationMachine
				clickModel, err = protocol.UnmarshalClickModel(clickModelReceiver)
				if err != nil {
					log.Logger().Error("failed to unmarshal click model", zap.Error(err))
				} else {
					s.ClickModel = clickModel
					s.ClickModelVersion = s.latestClickModelVersion
					log.Logger().Info("synced click model",
						zap.String("version", encoding.Hex(s.ClickModelVersion)))

					pulled = true
				}
			}
		}

		// if s.testMode {
		// 	return
		// }
		if pulled {
			s.pulledChan.Signal()
		}
	}
}

// build ranking index
func (s *Server) BuildRankingIndex() {
	defer base.CheckPanic()
	for range s.pulledChan.C {
		ctx := context.Background()
		itemCache, _, err := s.pullItems(ctx)
		if err != nil {
			log.Logger().Error("failed to pull items", zap.Error(err))
			return
		}

		// FIXME 这里需要加上tracer
		if s.RankingModel != nil && !s.RankingModel.Invalid() && s.rankingIndex == nil {
			if s.Config.Recommend.Collaborative.EnableIndex {
				startTime := time.Now()
				log.Logger().Info("start building ranking index")
				itemIndex := s.RankingModel.GetItemIndex()
				vectors := make([]search.Vector, itemIndex.Len())
				for i := int32(0); i < itemIndex.Len(); i++ {
					itemId := itemIndex.ToName(i)
					if itemCache.IsAvailable(itemId) {
						vectors[i] = search.NewDenseVector(s.RankingModel.GetItemFactor(i), itemCache.GetCategory(itemId), false)
					} else {
						vectors[i] = search.NewDenseVector(s.RankingModel.GetItemFactor(i), nil, true)
					}
				}
				builder := search.NewHNSWBuilder(vectors, s.Config.Recommend.CacheSize, s.Config.Server.NumJobs)

				var recall float32
				s.rankingIndex, recall = builder.Build(ctx, s.tracer, s.Config.Recommend.Collaborative.IndexRecall,
					s.Config.Recommend.Collaborative.IndexFitEpoch, false)
				// FIXME 好像是指标接口需要用到的，暂时先注释掉
				// CollaborativeFilteringIndexRecall.Set(float64(recall))
				if err = s.CacheClient.Set(ctx, cache.String(cache.Key(cache.GlobalMeta, cache.MatchingIndexRecall), encoding.FormatFloat32(recall))); err != nil {
					log.Logger().Error("failed to write meta", zap.Error(err))
				}
				log.Logger().Info("complete building ranking index",
					zap.Duration("build_time", time.Since(startTime)))

				// 10秒一次
				go func() {
					for range time.Tick(10 * time.Second) {
						progress := s.tracer.List()
						// 向master报告进度
						if _, err := s.masterClient.PushProgress(context.Background(), protocol.EncodeProgress(progress)); err != nil {
							log.Logger().Error("failed to report update task", zap.Error(err))
						}
					}
				}()
			} else {
				// CollaborativeFilteringIndexRecall.Set(1)
			}
		}
	}
}
