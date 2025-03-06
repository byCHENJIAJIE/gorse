// Copyright 2021 gorse Project Authors
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

package master

import (
	"context"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/samber/lo"
	"github.com/zhenghaoz/gorse/base/log"
	"github.com/zhenghaoz/gorse/config"
	"github.com/zhenghaoz/gorse/storage/cache"
	"github.com/zhenghaoz/gorse/storage/data"
	"go.uber.org/zap"
)

const (
	LabelFeedbackType = "feedback_type"
	LabelStep         = "step"
	LabelData         = "data"
)

var (
	LoadDatasetStepSecondsVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "load_dataset_step_seconds",
	}, []string{LabelStep})
	LoadDatasetTotalSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "load_dataset_total_seconds",
	})
	FindUserNeighborsSecondsVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "find_user_neighbors_seconds",
	}, []string{LabelStep})
	FindUserNeighborsTotalSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "find_user_neighbors_total_seconds",
	})
	FindItemNeighborsSecondsVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "find_item_neighbors_seconds",
	}, []string{"step"})
	FindItemNeighborsTotalSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "find_item_neighbors_total_seconds",
	})
	UpdateUserNeighborsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "update_user_neighbors_total",
	})
	UpdateItemNeighborsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "update_item_neighbors_total",
	})
	CacheScannedTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "cache_scanned_total",
	})
	CacheReclaimedTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "cache_reclaimed_total",
	})
	CacheScannedSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "cache_scanned_seconds",
	})

	CollaborativeFilteringFitSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_fit_seconds",
	})
	CollaborativeFilteringSearchSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_search_seconds",
	})
	CollaborativeFilteringNDCG10 = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_ndcg_10",
	})
	CollaborativeFilteringPrecision10 = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_precision_10",
	})
	CollaborativeFilteringRecall10 = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_recall_10",
	})
	CollaborativeFilteringSearchPrecision10 = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "collaborative_filtering_search_precision_10",
	})
	RankingFitSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_fit_seconds",
	})
	RankingSearchSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_search_seconds",
	})
	RankingPrecision = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_model_precision",
	})
	RankingRecall = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_model_recall",
	})
	RankingAUC = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_model_auc",
	})
	RankingSearchPrecision = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "ranking_search_precision",
	})
	UserNeighborIndexRecall = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "user_neighbor_index_recall",
	})
	ItemNeighborIndexRecall = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "item_neighbor_index_recall",
	})

	UsersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "users_total",
	})
	ActiveUsersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "active_users_total",
	})
	InactiveUsersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "inactive_users_total",
	})
	ItemsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "items_total",
	})
	ActiveItemsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "active_items_total",
	})
	InactiveItemsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "inactive_items_total",
	})
	UserLabelsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "user_labels_total",
	})
	ItemLabelsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "item_labels_total",
	})
	FeedbacksTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "feedbacks_total",
	})
	ImplicitFeedbacksTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "implicit_feedbacks_total",
	})
	PositiveFeedbacksTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "positive_feedbacks_total",
	})
	NegativeFeedbackTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "negative_feedbacks_total",
	})
	MemoryInUseBytesVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gorse",
		Subsystem: "master",
		Name:      "memory_inuse_bytes",
	}, []string{LabelData})
)

type OnlineEvaluator struct {
	ReadFeedbacks      []map[int32]mapset.Set[int32]
	PositiveFeedbacks  map[string][]lo.Tuple3[int32, int32, time.Time]
	ReverseIndex       map[lo.Tuple2[int32, int32]]time.Time
	EvaluateDays       int
	TruncatedDateToday time.Time
}

func NewOnlineEvaluator() *OnlineEvaluator {
	evaluator := new(OnlineEvaluator)
	evaluator.EvaluateDays = 30
	evaluator.TruncatedDateToday = time.Now().Truncate(time.Hour * 24)
	evaluator.ReverseIndex = make(map[lo.Tuple2[int32, int32]]time.Time)
	evaluator.PositiveFeedbacks = make(map[string][]lo.Tuple3[int32, int32, time.Time])
	evaluator.ReadFeedbacks = make([]map[int32]mapset.Set[int32], evaluator.EvaluateDays)
	for i := 0; i < evaluator.EvaluateDays; i++ {
		evaluator.ReadFeedbacks[i] = make(map[int32]mapset.Set[int32])
	}
	return evaluator
}

func (evaluator *OnlineEvaluator) Read(userIndex, itemIndex int32, timestamp time.Time) {
	// truncate timestamp to day
	truncatedTime := timestamp.Truncate(time.Hour * 24)
	index := int(evaluator.TruncatedDateToday.Sub(truncatedTime) / time.Hour / 24)

	if index >= 0 && index < evaluator.EvaluateDays {
		if evaluator.ReadFeedbacks[index][userIndex] == nil {
			evaluator.ReadFeedbacks[index][userIndex] = mapset.NewSet[int32]()
		}
		evaluator.ReadFeedbacks[index][userIndex].Add(itemIndex)
		evaluator.ReverseIndex[lo.Tuple2[int32, int32]{userIndex, itemIndex}] = timestamp
	}
}

func (evaluator *OnlineEvaluator) Positive(feedbackType string, userIndex, itemIndex int32, timestamp time.Time) {
	evaluator.PositiveFeedbacks[feedbackType] = append(evaluator.PositiveFeedbacks[feedbackType], lo.Tuple3[int32, int32, time.Time]{userIndex, itemIndex, timestamp})
}

func (evaluator *OnlineEvaluator) Evaluate() []cache.TimeSeriesPoint {
	var measurements []cache.TimeSeriesPoint
	for feedbackType, positiveFeedbacks := range evaluator.PositiveFeedbacks {
		positiveFeedbackSets := make([]map[int32]mapset.Set[int32], evaluator.EvaluateDays)
		for i := 0; i < evaluator.EvaluateDays; i++ {
			positiveFeedbackSets[i] = make(map[int32]mapset.Set[int32])
		}

		for _, f := range positiveFeedbacks {
			if readTime, exist := evaluator.ReverseIndex[lo.Tuple2[int32, int32]{f.A, f.B}]; exist /* && readTime.Unix() <= f.C.Unix() */ {
				// truncate timestamp to day
				truncatedTime := readTime.Truncate(time.Hour * 24)
				readIndex := int(evaluator.TruncatedDateToday.Sub(truncatedTime) / time.Hour / 24)
				if positiveFeedbackSets[readIndex][f.A] == nil {
					positiveFeedbackSets[readIndex][f.A] = mapset.NewSet[int32]()
				}
				positiveFeedbackSets[readIndex][f.A].Add(f.B)
			}
		}

		for i := 0; i < evaluator.EvaluateDays; i++ {
			var rate float64
			if len(evaluator.ReadFeedbacks[i]) > 0 {
				var sum float64
				for userIndex, readSet := range evaluator.ReadFeedbacks[i] {
					if positiveSet, exist := positiveFeedbackSets[i][userIndex]; exist {
						sum += float64(positiveSet.Cardinality()) / float64(readSet.Cardinality())
					}
				}
				rate = sum / float64(len(evaluator.ReadFeedbacks[i]))
			}
			measurements = append(measurements, cache.TimeSeriesPoint{
				Name:      cache.Key(PositiveFeedbackRate, feedbackType),
				Timestamp: evaluator.TruncatedDateToday.Add(-time.Hour * 24 * time.Duration(i)),
				Value:     rate,
			})
		}
	}
	return measurements
}

// DailyFeedbackRateCollector 收集每日正反馈率
type DailyFeedbackRateCollector struct {
	ctx         context.Context
	dataClient  data.Database
	cacheClient cache.Database
	config      *config.Config
}

func NewDailyFeedbackRateCollector(ctx context.Context, dataClient data.Database,
	cacheClient cache.Database, config *config.Config) *DailyFeedbackRateCollector {
	return &DailyFeedbackRateCollector{
		ctx:         ctx,
		dataClient:  dataClient,
		cacheClient: cacheClient,
		config:      config,
	}
}

func (c *DailyFeedbackRateCollector) Start() {
	go func() {
		// 计算首次运行时间：下一个凌晨1点
		now := time.Now()
		nextRun := time.Date(now.Year(), now.Month(), now.Day()+1, 1, 0, 0, 0, now.Location())
		log.Logger().Info("collect daily rate, first run time", zap.Time("next_run", nextRun))
		// 先等待到首次运行时间
        time.Sleep(nextRun.Sub(now))
        
        // 立即执行一次
        if err := c.CollectDailyRate(); err != nil {
            log.Logger().Error("failed to collect feedback rate", zap.Error(err))
        }

        // 然后开始定时执行

		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := c.CollectDailyRate(); err != nil {
					log.Logger().Error("failed to collect feedback rate", zap.Error(err))
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func (c *DailyFeedbackRateCollector) CollectDailyRate() error {
    yesterday := time.Now().Add(-24 * time.Hour)
    startTime := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
    endTime := startTime.Add(24 * time.Hour)

	log.Logger().Info("collect daily rate", zap.Time("start_time", startTime), zap.Time("end_time", endTime))

    // 先获取所有阅读记录
    readMap := make(map[string]mapset.Set[string])
    totalReads := 0
    
    feedbackChan, errChan := c.dataClient.GetFeedbackStream(c.ctx, batchSize, 
        data.WithBeginTime(startTime), 
        data.WithEndTime(endTime), 
        data.WithFeedbackTypes("read"))
    
    // 处理阅读数据流
    for feedbacks := range feedbackChan {
        for _, read := range feedbacks {
            if readMap[read.UserId] == nil {
                readMap[read.UserId] = mapset.NewSet[string]()
            }
            readMap[read.UserId].Add(read.ItemId)
            totalReads++
        }
    }
    
    // 检查错误
    if err := <-errChan; err != nil {
        return errors.Trace(err)
    }

    // 对每种正反馈类型计算比率
    for _, feedbackType := range c.config.Recommend.DataSource.PositiveFeedbackTypes {
        positives := 0
        
        // 获取正反馈数据流
        feedbackChan, errChan := c.dataClient.GetFeedbackStream(c.ctx, batchSize,
            data.WithBeginTime(startTime),
            data.WithEndTime(endTime),
            data.WithFeedbackTypes(feedbackType))
        
        // 处理正反馈数据流
        for feedbacks := range feedbackChan {
            for _, positive := range feedbacks {
                if readSet := readMap[positive.UserId]; readSet != nil && readSet.Contains(positive.ItemId) {
                    positives++
                }
            }
        }
        
        // 检查错误
        if err := <-errChan; err != nil {
            return errors.Trace(err)
        }

        // 计算正反馈率
        var rate float64
        if totalReads > 0 {
            rate = float64(positives) / float64(totalReads)
        }

		log.Logger().Info("collect daily rate", zap.String("feedback_type", feedbackType), zap.Float64("rate", rate))
        // 更新时间序列
        err := c.cacheClient.AddTimeSeriesPoints(c.ctx, []cache.TimeSeriesPoint{
            {
                Name:      cache.Key(PositiveFeedbackRate, feedbackType),
                Timestamp: startTime.Add(8 * time.Hour),
                Value:     rate,
            },
        })
        if err != nil {
            return errors.Trace(err)
        }

        // 更新 Prometheus 指标
        // PositiveFeedbackRate.WithLabelValues(feedbackType).Set(rate)
    }

    return nil
}
