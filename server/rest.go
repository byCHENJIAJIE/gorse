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
	"net/http"
	"net/http/pprof"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/araddon/dateparse"
	mapset "github.com/deckarep/golang-set/v2"
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/google/uuid"
	"github.com/juju/errors"
	"github.com/maypok86/otter"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samber/lo"
	"github.com/thoas/go-funk"
	"github.com/zhenghaoz/gorse/base/encoding"
	"github.com/zhenghaoz/gorse/base/heap"
	"github.com/zhenghaoz/gorse/base/log"
	"github.com/zhenghaoz/gorse/base/search"
	"github.com/zhenghaoz/gorse/config"
	"github.com/zhenghaoz/gorse/model/click"
	"github.com/zhenghaoz/gorse/protocol"
	"github.com/zhenghaoz/gorse/storage/cache"
	"github.com/zhenghaoz/gorse/storage/data"
	"go.opentelemetry.io/contrib/instrumentation/github.com/emicklei/go-restful/otelrestful"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"modernc.org/mathutil"
)

const (
	HealthAPITag         = "health"
	UsersAPITag          = "users"
	ItemsAPITag          = "items"
	FeedbackAPITag       = "feedback"
	RecommendationAPITag = "recommendation"
	MeasurementsAPITag   = "measurements"
	DetractedAPITag      = "deprecated"
	batchSize            = 10000
)

// RestServer implements a REST-ful API server.
type RestServer struct {
	*config.Settings

	HttpHost string
	HttpPort int

	DisableLog bool
	WebService *restful.WebService
	HttpServer *http.Server

	IsMaster bool

	Server *Server

	// 添加全局 itemCache
	itemCache *ItemCache

	// 添加全局 feedbackCache
	feedbackCache *FeedbackCache

	// 添加内存缓存
	popularItems         *sync.Map // 热门物品缓存,key为分类
	latestItems          *sync.Map // 最新物品缓存,key为分类
	itemNeighbors        *sync.Map // 物品邻居缓存,key为itemId
	userNeighbors        *sync.Map // 用户邻居缓存,key为userId
	itemNeighborsVersion int64
	userNeighborsVersion int64
}

func (s *RestServer) SetServer(server *Server) {
	s.Server = server
}

// StartHttpServer starts the REST-ful API server.
func (s *RestServer) StartHttpServer(container *restful.Container) {
	if !s.IsMaster {
		// 初始化 itemCache 和 feedbackCache
		if err := s.initItemCache(context.Background()); err != nil {
			log.Logger().Fatal("failed to initialize item cache", zap.Error(err))
		}
		var err error
		s.feedbackCache, err = NewFeedbackCache(s, s.Config.Recommend.DataSource.PositiveFeedbackTypes...)
		if err != nil {
			log.Logger().Fatal("failed to initialize feedback cache", zap.Error(err))
		}

		// 初始化内存缓存
		if err := s.initCache(context.Background()); err != nil {
			log.Logger().Fatal("failed to initialize memory cache", zap.Error(err))
		}

		// go s.reportCacheSize(context.Background())
	}

	// register restful APIs
	s.CreateWebService()
	container.Add(s.WebService)
	// register swagger UI
	specConfig := restfulspec.Config{
		WebServices: []*restful.WebService{s.WebService},
		APIPath:     "/apidocs.json",
	}
	container.Add(restfulspec.NewOpenAPIService(specConfig))
	swaggerFile = specConfig.APIPath
	container.Handle(apiDocsPath, http.HandlerFunc(handler))
	// register prometheus
	container.Handle("/metrics", promhttp.Handler())
	// register pprof
	container.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	container.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	container.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	container.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	container.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	container.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	container.Handle("/debug/pprof/block", pprof.Handler("block"))
	container.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	container.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	container.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	container.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	// Add container filter to enable CORS
	cors := restful.CrossOriginResourceSharing{
		AllowedHeaders: []string{"Content-Type", "Accept", "X-API-Key"},
		AllowedDomains: s.Config.Master.HttpCorsDomains,
		AllowedMethods: s.Config.Master.HttpCorsMethods,
		CookiesAllowed: false,
		Container:      container}
	container.Filter(cors.Filter)

	log.Logger().Info("start http server",
		zap.String("url", fmt.Sprintf("http://%s:%d", s.HttpHost, s.HttpPort)),
		zap.Strings("cors_methods", s.Config.Master.HttpCorsMethods),
		zap.Strings("cors_domains", s.Config.Master.HttpCorsDomains),
	)
	s.HttpServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", s.HttpHost, s.HttpPort),
		Handler: container,
	}
	if err := s.HttpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Logger().Fatal("failed to start http server", zap.Error(err))
	}
}

func (s *RestServer) LogFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// generate request id
	requestId := uuid.New().String()
	resp.AddHeader("X-Request-ID", requestId)

	start := time.Now()
	chain.ProcessFilter(req, resp)
	responseTime := time.Since(start)
	if !s.DisableLog && req.Request.URL.Path != "/api/dashboard/cluster" &&
		req.Request.URL.Path != "/api/dashboard/tasks" {
		log.ResponseLogger(resp).Info(fmt.Sprintf("%s %s", req.Request.Method, req.Request.URL),
			zap.Int("status_code", resp.StatusCode()),
			zap.Duration("response_time", responseTime))
	}
}

func (s *RestServer) AuthFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	if strings.HasPrefix(req.SelectedRoute().Path(), "/api/health/") {
		// Health check APIs don't need API key,
		chain.ProcessFilter(req, resp)
		return
	}
	if s.Config.Server.APIKey == "" {
		chain.ProcessFilter(req, resp)
		return
	}
	apikey := req.HeaderParameter("X-API-Key")
	if apikey == s.Config.Server.APIKey {
		chain.ProcessFilter(req, resp)
		return
	}
	log.ResponseLogger(resp).Error("unauthorized",
		zap.String("api_key", s.Config.Server.APIKey),
		zap.String("X-API-Key", apikey))
	if err := resp.WriteError(http.StatusUnauthorized, fmt.Errorf("unauthorized")); err != nil {
		log.ResponseLogger(resp).Error("failed to write error", zap.Error(err))
	}
}

func (s *RestServer) MetricsFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	startTime := time.Now()
	chain.ProcessFilter(req, resp)
	if req.SelectedRoute() != nil && resp.StatusCode() == http.StatusOK {
		routePath := req.SelectedRoutePath()
		if !strings.HasPrefix(routePath, "/api/dashboard") {
			RestAPIRequestSecondsVec.WithLabelValues(fmt.Sprintf("%s %s", req.Request.Method, routePath)).
				Observe(time.Since(startTime).Seconds())
		}
	}
}

// CreateWebService creates web service.
func (s *RestServer) CreateWebService() {
	// Create a server
	ws := s.WebService
	ws.Path("/api/").
		Produces(restful.MIME_JSON).
		Filter(s.LogFilter).
		Filter(s.AuthFilter).
		Filter(s.MetricsFilter).
		Filter(otelrestful.OTelFilter("gorse"))

	/* Health check */
	ws.Route(ws.GET("/health/live").To(s.checkLive).
		Doc("Probe the liveness of this node. Return OK once the server starts.").
		Metadata(restfulspec.KeyOpenAPITags, []string{HealthAPITag}).
		Returns(http.StatusOK, "OK", HealthStatus{}).
		Writes(HealthStatus{}))
	ws.Route(ws.GET("/health/ready").To(s.checkReady).
		Doc("Probe the readiness of this node. Return OK if the server is able to handle requests.").
		Metadata(restfulspec.KeyOpenAPITags, []string{HealthAPITag}).
		Returns(http.StatusOK, "OK", HealthStatus{}).
		Writes(HealthStatus{}))

	// Insert a user
	ws.Route(ws.POST("/user").To(s.insertUser).
		Doc("Insert a user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads(data.User{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Modify a user
	ws.Route(ws.PATCH("/user/{user-id}").To(s.modifyUser).
		Doc("Modify a user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to modify").DataType("string")).
		Reads(data.UserPatch{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Get a user
	ws.Route(ws.GET("/user/{user-id}").To(s.getUser).
		Doc("Get a user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get").DataType("string")).
		Returns(http.StatusOK, "OK", data.User{}).
		Writes(data.User{}))
	// Insert users
	ws.Route(ws.POST("/users").To(s.insertUsers).
		Doc("Insert users.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads([]data.User{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Get users
	ws.Route(ws.GET("/users").To(s.getUsers).
		Doc("Get users.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned users").DataType("integer")).
		Param(ws.QueryParameter("cursor", "Cursor for the next page").DataType("string")).
		Returns(http.StatusOK, "OK", UserIterator{}).
		Writes(UserIterator{}))
	// Delete a user
	ws.Route(ws.DELETE("/user/{user-id}").To(s.deleteUser).
		Doc("Delete a user and his or her feedback.").
		Metadata(restfulspec.KeyOpenAPITags, []string{UsersAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to delete").DataType("string")).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))

	// Insert an item
	ws.Route(ws.POST("/item").To(s.insertItem).
		Doc("Insert an item. Overwrite if the item exists.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads(data.Item{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Modify an item
	ws.Route(ws.PATCH("/item/{item-id}").To(s.modifyItem).
		Doc("Modify an item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to modify").DataType("string")).
		Reads(data.ItemPatch{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Get items
	ws.Route(ws.GET("/items").To(s.getItems).
		Doc("Get items.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("cursor", "Cursor for the next page").DataType("string")).
		Returns(http.StatusOK, "OK", ItemIterator{}).
		Writes(ItemIterator{}))
	// Get item
	ws.Route(ws.GET("/item/{item-id}").To(s.getItem).
		Doc("Get a item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to get.").DataType("string")).
		Returns(http.StatusOK, "OK", data.Item{}).
		Writes(data.Item{}))
	// Insert items
	ws.Route(ws.POST("/items").To(s.insertItems).
		Doc("Insert items. Overwrite if items exist").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads([]data.Item{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Delete item
	ws.Route(ws.DELETE("/item/{item-id}").To(s.deleteItem).
		Doc("Delete an item and its feedback.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to delete").DataType("string")).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Insert category
	ws.Route(ws.PUT("/item/{item-id}/category/{category}").To(s.insertItemCategory).
		Doc("Insert a category for a item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to insert category").DataType("string")).
		Param(ws.PathParameter("category", "Category to insert").DataType("string")).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Delete category
	ws.Route(ws.DELETE("/item/{item-id}/category/{category}").To(s.deleteItemCategory).
		Doc("Delete a category from a item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{ItemsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to delete categoryßßß").DataType("string")).
		Param(ws.PathParameter("category", "Category to delete").DataType("string")).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Insert feedback
	// ws.Route(ws.POST("/feedback").To(s.insertFeedback(false)).
	// Doc("Insert feedbacks. Ignore insertion if feedback exists.").
	ws.Route(ws.POST("/feedback").To(s.insertFeedback(true)).
		Doc("Insert feedbacks. Existed feedback will be overwritten.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads([]data.Feedback{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	ws.Route(ws.PUT("/feedback").To(s.insertFeedback(true)).
		Doc("Insert feedbacks. Existed feedback will be overwritten.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Reads([]data.Feedback{}).
		Returns(http.StatusOK, "OK", Success{}).
		Writes(Success{}))
	// Get feedback
	ws.Route(ws.GET("/feedback").To(s.getFeedback).
		Doc("Get feedbacks.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("cursor", "Cursor for the next page").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned feedback").DataType("integer")).
		Returns(http.StatusOK, "OK", FeedbackIterator{}).
		Writes(FeedbackIterator{}))
	ws.Route(ws.GET("/feedback/{user-id}/{item-id}").To(s.getUserItemFeedback).
		Doc("Get feedbacks between a user and a item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))
	ws.Route(ws.DELETE("/feedback/{user-id}/{item-id}").To(s.deleteUserItemFeedback).
		Doc("Delete feedbacks between a user and a item.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))
	ws.Route(ws.GET("/feedback/{feedback-type}").To(s.getTypedFeedback).
		Doc("Get feedbacks with feedback type.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("feedback-type", "Type of returned feedbacks").DataType("string")).
		Param(ws.QueryParameter("cursor", "Cursor for the next page").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned feedbacks").DataType("integer")).
		Returns(http.StatusOK, "OK", FeedbackIterator{}).
		Writes(FeedbackIterator{}))
	ws.Route(ws.GET("/feedback/{feedback-type}/{user-id}/{item-id}").To(s.getTypedUserItemFeedback).
		Doc("Get feedbacks between a user and a item with feedback type.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("feedback-type", "Type of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", data.Feedback{}).
		Writes(data.Feedback{}))
	ws.Route(ws.DELETE("/feedback/{feedback-type}/{user-id}/{item-id}").To(s.deleteTypedUserItemFeedback).
		Doc("Delete feedbacks between a user and a item with feedback type.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("feedback-type", "Type of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", data.Feedback{}).
		Writes(data.Feedback{}))
	// Get feedback by user id
	ws.Route(ws.GET("/user/{user-id}/feedback/{feedback-type}").To(s.getTypedFeedbackByUser).
		Doc("Get feedbacks by user id with feedback type.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("feedback-type", "Type of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))
	ws.Route(ws.GET("/user/{user-id}/feedback").To(s.getFeedbackByUser).
		Doc("Get feedbacks by user id.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "User ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))
	// Get feedback by item-id
	ws.Route(ws.GET("/item/{item-id}/feedback/{feedback-type}").To(s.getTypedFeedbackByItem).
		Doc("Get feedbacks by item id with feedback type.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Param(ws.PathParameter("feedback-type", "Type of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))
	ws.Route(ws.GET("/item/{item-id}/feedback/").To(s.getFeedbackByItem).
		Doc("Get feedbacks by item id.").
		Metadata(restfulspec.KeyOpenAPITags, []string{FeedbackAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "Item ID of returned feedbacks").DataType("string")).
		Returns(http.StatusOK, "OK", []data.Feedback{}).
		Writes([]data.Feedback{}))

	// Get collaborative filtering recommendation by user id
	ws.Route(ws.GET("/intermediate/recommend/{user-id}").To(s.getCollaborative).
		Doc("Get the collaborative filtering recommendation for a user").
		Metadata(restfulspec.KeyOpenAPITags, []string{DetractedAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get recommendation").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/intermediate/recommend/{user-id}/{category}").To(s.getCollaborative).
		Doc("Get the collaborative filtering recommendation for a user").
		Metadata(restfulspec.KeyOpenAPITags, []string{DetractedAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get recommendation").DataType("string")).
		Param(ws.PathParameter("category", "Category of returned items.").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))

	// Get popular items
	ws.Route(ws.GET("/popular").To(s.getPopular).
		Doc("Get popular items.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned recommendations").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned recommendations").DataType("integer")).
		Param(ws.QueryParameter("user-id", "Remove read items of a user").DataType("string")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/popular/{category}").To(s.getPopular).
		Doc("Get popular items in category.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("category", "Category of returned items.").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Param(ws.QueryParameter("user-id", "Remove read items of a user").DataType("string")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	// Get latest items
	ws.Route(ws.GET("/latest").To(s.getLatest).
		Doc("Get the latest items.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Param(ws.QueryParameter("user-id", "Remove read items of a user").DataType("string")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/latest/{category}").To(s.getLatest).
		Doc("Get the latest items in category.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("category", "Category of returned items.").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Param(ws.QueryParameter("user-id", "Remove read items of a user").DataType("string")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	// Get neighbors
	ws.Route(ws.GET("/item/{item-id}/neighbors/").To(s.getItemNeighbors).
		Doc("Get neighbors of a item").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to get neighbors").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/item/{item-id}/neighbors/{category}").To(s.getItemNeighbors).
		Doc("Get neighbors of a item in category.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("item-id", "ID of the item to get neighbors").DataType("string")).
		Param(ws.PathParameter("category", "Category of returned items").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/user/{user-id}/neighbors/").To(s.getUserNeighbors).
		Doc("Get neighbors of a user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get neighbors").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned users").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned users").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.GET("/recommend/{user-id}").To(s.getRecommend).
		Doc("Get recommendation for user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get recommendation").DataType("string")).
		Param(ws.QueryParameter("category", "Category of the returned items (support multi-categories filtering)").DataType("string")).
		Param(ws.QueryParameter("write-back-type", "Type of write back feedback").DataType("string")).
		Param(ws.QueryParameter("write-back-delay", "Timestamp delay of write back feedback (format 0h0m0s)").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []string{}).
		Writes([]string{}))
	ws.Route(ws.GET("/recommend/{user-id}/{category}").To(s.getRecommend).
		Doc("Get recommendation for user.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("user-id", "ID of the user to get recommendation").DataType("string")).
		Param(ws.PathParameter("category", "Category of the returned items").DataType("string")).
		Param(ws.QueryParameter("write-back-type", "Type of write back feedback").DataType("string")).
		Param(ws.QueryParameter("write-back-delay", "Timestamp delay of write back feedback (format 0h0m0s)").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Returns(http.StatusOK, "OK", []string{}).
		Writes([]string{}))
	ws.Route(ws.POST("/session/recommend").To(s.sessionRecommend).
		Doc("Get recommendation for session.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Reads([]Feedback{}).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))
	ws.Route(ws.POST("/session/recommend/{category}").To(s.sessionRecommend).
		Doc("Get recommendation for session.").
		Metadata(restfulspec.KeyOpenAPITags, []string{RecommendationAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("category", "Category of the returned items").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned items").DataType("integer")).
		Param(ws.QueryParameter("offset", "Offset of returned items").DataType("integer")).
		Reads([]Feedback{}).
		Returns(http.StatusOK, "OK", []cache.Document{}).
		Writes([]cache.Document{}))

	ws.Route(ws.GET("/measurements/{name}").To(s.getMeasurements).
		Doc("Get measurements.").
		Metadata(restfulspec.KeyOpenAPITags, []string{MeasurementsAPITag}).
		Param(ws.HeaderParameter("X-API-Key", "API key").DataType("string")).
		Param(ws.PathParameter("name", "Name of returned measurements").DataType("string")).
		Param(ws.QueryParameter("n", "Number of returned measurements").DataType("integer")).
		Returns(http.StatusOK, "OK", []cache.TimeSeriesPoint{}).
		Writes([]cache.TimeSeriesPoint{}))
}

// ParseInt parses integers from the query parameter.
func ParseInt(request *restful.Request, name string, fallback int) (value int, err error) {
	valueString := request.QueryParameter(name)
	value, err = strconv.Atoi(valueString)
	if err != nil && valueString == "" {
		value = fallback
		err = nil
	}
	return
}

// ParseDuration parses duration from the query parameter.
func ParseDuration(request *restful.Request, name string) (time.Duration, error) {
	valueString := request.QueryParameter(name)
	if valueString == "" {
		return 0, nil
	}
	return time.ParseDuration(valueString)
}

func (s *RestServer) searchDocuments(collection, subset, category string, isItem bool, request *restful.Request, response *restful.Response) {
	var (
		ctx    = request.Request.Context()
		n      int
		offset int
		userId string
		err    error
	)

	// parse arguments
	if offset, err = ParseInt(request, "offset", 0); err != nil {
		BadRequest(response, err)
		return
	}
	if n, err = ParseInt(request, "n", s.Config.Server.DefaultN); err != nil {
		BadRequest(response, err)
		return
	}
	userId = request.QueryParameter("user-id")

	readItems := mapset.NewSet[string]()
	if userId != "" {
		feedback, err := s.DataClient.GetUserFeedback(ctx, userId, s.Config.Now())
		if err != nil {
			InternalServerError(response, err)
			return
		}
		for _, f := range feedback {
			readItems.Add(f.ItemId)
		}
	}

	end := offset + n
	if end > 0 && readItems.Cardinality() > 0 {
		end += readItems.Cardinality()
	}

	// Get the sorted list
	items, err := s.CacheClient.SearchDocuments(ctx, collection, subset, []string{category}, offset, end)
	if err != nil {
		InternalServerError(response, err)
		return
	}

	// Remove read items
	if userId != "" {
		prunedItems := make([]cache.Document, 0, len(items))
		for _, item := range items {
			if !readItems.Contains(item.Id) {
				prunedItems = append(prunedItems, item)
			}
		}
		items = prunedItems
	}

	// Send result
	if n > 0 && len(items) > n {
		items = items[:n]
	}
	Ok(response, items)
}

func (s *RestServer) getPopular(request *restful.Request, response *restful.Response) {
	category := request.PathParameter("category")
	log.ResponseLogger(response).Debug("get category popular items in category", zap.String("category", category))
	s.searchDocuments(cache.PopularItems, "", category, true, request, response)
}

func (s *RestServer) getLatest(request *restful.Request, response *restful.Response) {
	category := request.PathParameter("category")
	log.ResponseLogger(response).Debug("get category latest items in category", zap.String("category", category))
	s.searchDocuments(cache.LatestItems, "", category, true, request, response)
}

// get feedback by item-id with feedback type
func (s *RestServer) getTypedFeedbackByItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	feedbackType := request.PathParameter("feedback-type")
	itemId := request.PathParameter("item-id")
	feedback, err := s.DataClient.GetItemFeedback(ctx, itemId, feedbackType)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, feedback)
}

// get feedback by item-id
func (s *RestServer) getFeedbackByItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	itemId := request.PathParameter("item-id")
	feedback, err := s.DataClient.GetItemFeedback(ctx, itemId)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, feedback)
}

// getItemNeighbors gets neighbors of a item from database.
func (s *RestServer) getItemNeighbors(request *restful.Request, response *restful.Response) {
	// Get item id
	itemId := request.PathParameter("item-id")
	category := request.PathParameter("category")
	s.searchDocuments(cache.ItemNeighbors, itemId, category, true, request, response)
}

// getUserNeighbors gets neighbors of a user from database.
func (s *RestServer) getUserNeighbors(request *restful.Request, response *restful.Response) {
	// Get item id
	userId := request.PathParameter("user-id")
	s.searchDocuments(cache.UserNeighbors, userId, "", false, request, response)
}

// getCollaborative gets cached recommended items from database.
func (s *RestServer) getCollaborative(request *restful.Request, response *restful.Response) {
	// Get user id
	userId := request.PathParameter("user-id")
	category := request.PathParameter("category")
	s.searchDocuments(cache.OfflineRecommend, userId, category, true, request, response)
}

// Recommend items to users.
// 1. If there are recommendations in cache, return cached recommendations.
// 2. If there are historical interactions of the users, return similar items.
// 3. Otherwise, return fallback recommendation (popular/latest).
func (s *RestServer) Recommend(ctx context.Context, response *restful.Response, userId string, categories []string, n int, recommenders ...Recommender) ([]string, error) {
	initStart := time.Now()

	// create context
	recommendCtx, err := s.createRecommendContext(ctx, userId, categories, n)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// execute recommenders
	for _, recommender := range recommenders {
		err = recommender(recommendCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// return recommendations
	if len(recommendCtx.results) > n {
		recommendCtx.results = recommendCtx.results[:n]
	}
	totalTime := time.Since(initStart)
	log.ResponseLogger(response).Info("complete recommendation",
		zap.Int("num_from_online", recommendCtx.numFromOnline),
		zap.Int("num_from_offline", recommendCtx.numFromOffline),
		zap.Int("num_from_collaborative", recommendCtx.numFromCollaborative),
		zap.Int("num_from_item_based", recommendCtx.numFromItemBased),
		zap.Int("num_from_user_based", recommendCtx.numFromUserBased),
		zap.Int("num_from_latest", recommendCtx.numFromLatest),
		zap.Int("num_from_poplar", recommendCtx.numFromPopular),
		zap.Duration("total_time", totalTime),
		zap.Duration("load_online_recommend_time", recommendCtx.loadOnlineRecTime),
		zap.Duration("load_offline_recommend_time", recommendCtx.loadOfflineRecTime),
		zap.Duration("load_col_recommend_time", recommendCtx.loadColRecTime),
		zap.Duration("load_hist_time", recommendCtx.loadLoadHistTime),
		zap.Duration("item_based_recommend_time", recommendCtx.itemBasedTime),
		zap.Duration("user_based_recommend_time", recommendCtx.userBasedTime),
		zap.Duration("load_latest_time", recommendCtx.loadLatestTime),
		zap.Duration("load_popular_time", recommendCtx.loadPopularTime))
	return recommendCtx.results, nil
}

type recommendContext struct {
	context      context.Context
	userId       string
	categories   []string
	userFeedback []data.Feedback
	n            int
	results      []string
	excludeSet   mapset.Set[string]

	numPrevStage         int
	numFromLatest        int
	numFromPopular       int
	numFromUserBased     int
	numFromItemBased     int
	numFromCollaborative int
	numFromOffline       int
	numFromOnline        int

	loadOfflineRecTime time.Duration
	loadOnlineRecTime  time.Duration
	loadColRecTime     time.Duration
	loadLoadHistTime   time.Duration
	itemBasedTime      time.Duration
	userBasedTime      time.Duration
	loadLatestTime     time.Duration
	loadPopularTime    time.Duration
}

func (s *RestServer) createRecommendContext(ctx context.Context, userId string, categories []string, n int) (*recommendContext, error) {
	// pull historical feedback
	startTime := time.Now()
	userFeedback, err := s.feedbackCache.GetUserFeedback(ctx, userId)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Logger().Info("TimeUse: GetUserFeedback", zap.String("user_id", userId), zap.Duration("get_user_feedback_time", time.Since(startTime)))
	excludeSet := mapset.NewSet[string]()
	for _, item := range userFeedback {
		if !s.Config.Recommend.Replacement.EnableReplacement {
			excludeSet.Add(item.ItemId)
		}
	}
	return &recommendContext{
		userId:       userId,
		categories:   categories,
		n:            n,
		excludeSet:   excludeSet,
		userFeedback: userFeedback,
		context:      ctx,
	}, nil
}

type Recommender func(ctx *recommendContext) error

func (s *RestServer) RecommendOnline(ctx *recommendContext) error {
	log.Logger().Debug("start online recommendation")
	start := time.Now()

	// 使用全局 itemCache
	itemCache := s.itemCache

	// 现在user的label是空的，不需要查数据库
	// user, err := s.DataClient.GetUser(ctx.context, ctx.userId)
	user := data.User{
		UserId: ctx.userId,
		Labels: []string{},
	}
	userId := ctx.userId

	var (
		collaborativeRecommendSeconds atomic.Float64
		userBasedRecommendSeconds     atomic.Float64
		itemBasedRecommendSeconds     atomic.Float64
		latestRecommendSeconds        atomic.Float64
		popularRecommendSeconds       atomic.Float64
		historyItems                  []string
		positiveItems                 []string
		excludeSet                    mapset.Set[string]
	)

	// 并发处理，创建错误通道和结果通道
	errChan := make(chan error, 5)
	candidatesChan := make(chan map[string][][]string, 5)
	var wg sync.WaitGroup

	// load historical and positive items
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	loadUserHistoricalItemsStart := time.Now()
	for _, feedback := range ctx.userFeedback {
		historyItems = append(historyItems, feedback.ItemId)
	}
	excludeSet = mapset.NewSet(historyItems...)
	log.Logger().Debug("TimeUse: load user historical items", zap.String("user_id", userId), zap.Duration("load_user_historical_items_time", time.Since(loadUserHistoricalItemsStart)))

	loadPositiveItemsStart := time.Now()
	if s.Config.Recommend.Offline.EnableItemBasedRecommend {
		for _, feedback := range ctx.userFeedback {
			if funk.ContainsString(s.Config.Recommend.DataSource.PositiveFeedbackTypes, feedback.FeedbackType) {
				positiveItems = append(positiveItems, feedback.ItemId)
			}
		}
	}
	log.Logger().Debug("TimeUse: load positive items", zap.String("user_id", userId), zap.Duration("load_positive_items_time", time.Since(loadPositiveItemsStart)))
	// candidatesChan <- nil
	// }()

	// Recommender #1: collaborative filtering.
	wg.Add(1)
	go func() {
		defer wg.Done()
		collaborativeRecommendStart := time.Now()
		defer func() {
			log.Logger().Debug("TimeUse: collaborative filtering", zap.String("user_id", userId), zap.Duration("collaborative_filtering_time", time.Since(collaborativeRecommendStart)))
		}()
		if s.Config.Recommend.Offline.EnableColRecommend && s.RankingModel != nil && !s.RankingModel.Invalid() {
			if userIndex := s.RankingModel.GetUserIndex().ToNumber(userId); s.RankingModel.IsUserPredictable(userIndex) {
				var recommend map[string][]string
				var usedTime time.Duration
				var err error
				if s.Config.Recommend.Collaborative.EnableIndex && s.Server.rankingIndex != nil {
					log.Logger().Debug("use hnsw", zap.String("user_id", userId))
					recommend, usedTime, err = s.collaborativeRecommendHNSW(s.Server.rankingIndex, userId, ctx.categories, excludeSet, itemCache)
				} else {
					log.Logger().Debug("use brute force", zap.String("user_id", userId))
					recommend, usedTime, err = s.collaborativeRecommendBruteForce(userId, ctx.categories, excludeSet, itemCache)
				}
				if err != nil {
					log.Logger().Error("failed to recommend by collaborative filtering",
						zap.String("user_id", userId), zap.Error(err))
					errChan <- err
					return
				}
				candidates := make(map[string][][]string)
				for category, items := range recommend {
					candidates[category] = append(candidates[category], items)
				}
				collaborativeRecommendSeconds.Add(usedTime.Seconds())
				candidatesChan <- candidates
				return
			} else if !s.RankingModel.IsUserPredictable(userIndex) {
				log.Logger().Info("user is unpredictable", zap.String("user_id", userId))
			} else {
				log.Logger().Error("user is not in the model", zap.String("user_id", userId))
			}
		} else if s.RankingModel == nil || s.RankingModel.Invalid() {
			log.Logger().Info("no collaborative filtering model")
		}
		candidatesChan <- nil
	}()

	// Recommender #2: item-based.
	wg.Add(1)
	go func() {
		defer wg.Done()
		itemBasedRecommendStart := time.Now()
		defer func() {
			log.Logger().Debug("TimeUse: item-based", zap.String("user_id", userId), zap.Duration("item_based_recommend_time", time.Since(itemBasedRecommendStart)))
		}()
		if s.Config.Recommend.Offline.EnableItemBasedRecommend {
			candidates := make(map[string][][]string)
			localStartTime := time.Now()
			for _, category := range ctx.categories {
				// collect candidates
				scores := make(map[string]float64)
				for _, itemId := range positiveItems {
					// load similar items
					similarItems, err := s.GetItemNeighbors(itemId, category)
					if err != nil {
						log.Logger().Error("failed to load similar items", zap.Error(err))
						errChan <- err
						return
					}
					// add unseen items
					for _, item := range similarItems {
						if !excludeSet.Contains(item.Id) && itemCache.IsAvailable(item.Id) {
							scores[item.Id] += item.Score
						}
					}
				}
				// collect top k
				filter := heap.NewTopKFilter[string, float64](s.Config.Recommend.CacheSize)
				for id, score := range scores {
					filter.Push(id, score)
				}
				ids, _ := filter.PopAll()
				candidates[category] = append(candidates[category], ids)
			}
			itemBasedRecommendSeconds.Add(time.Since(localStartTime).Seconds())
			candidatesChan <- candidates
			return
		}
		candidatesChan <- nil
	}()

	// Recommender #3: insert user-based items
	wg.Add(1)
	go func() {
		defer wg.Done()
		userBasedRecommendStart := time.Now()
		defer func() {
			log.Logger().Debug("TimeUse: user-based", zap.String("user_id", userId), zap.Duration("user_based_recommend_time", time.Since(userBasedRecommendStart)))
		}()
		if s.Config.Recommend.Offline.EnableUserBasedRecommend {
			candidates := make(map[string][][]string)
			localStartTime := time.Now()
			scores := make(map[string]float64)
			// load similar users
			similarUsers, err := s.GetUserNeighbors(userId)
			if err != nil {
				log.Logger().Error("failed to load similar users", zap.Error(err))
				errChan <- err
				return
			}
			// FIXME 这里很花时间，有上百毫秒，瓶颈在这里
			for _, user := range similarUsers {
				// load similar user's historical feedback
				similarUserFeedbacks, err := s.feedbackCache.GetUserPositiveFeedback(ctx.context, user.Id)
				if err != nil {
					log.Logger().Error("failed to pull user feedback",
						zap.String("user_id", userId), zap.Error(err))
					errChan <- err
					return
				}
				// add unseen items
				for _, f := range similarUserFeedbacks {
					if !excludeSet.Contains(f.ItemId) && itemCache.IsAvailable(f.ItemId) {
						scores[f.ItemId] += user.Score
					}
				}
			}
			// collect top k
			filters := make(map[string]*heap.TopKFilter[string, float64])
			for _, category := range ctx.categories {
				filters[category] = heap.NewTopKFilter[string, float64](s.Config.Recommend.CacheSize)
			}
			for id, score := range scores {
				for _, category := range itemCache.GetCategory(id) {
					if filter, ok := filters[category]; ok {
						filter.Push(id, score)
					}
				}
			}
			for category, filter := range filters {
				ids, _ := filter.PopAll()
				candidates[category] = append(candidates[category], ids)
			}
			userBasedRecommendSeconds.Add(time.Since(localStartTime).Seconds())
			candidatesChan <- candidates
			return
		}
		candidatesChan <- nil
	}()

	// Recommender #4: latest items.
	wg.Add(1)
	go func() {
		defer wg.Done()
		latestRecommendStart := time.Now()
		defer func() {
			log.Logger().Debug("TimeUse: latest", zap.String("user_id", userId), zap.Duration("latest_recommend_time", time.Since(latestRecommendStart)))
		}()
		if s.Config.Recommend.Offline.EnableLatestRecommend {
			candidates := make(map[string][][]string)
			localStartTime := time.Now()
			for _, category := range ctx.categories {
				latestItems := s.GetLatestItems(category)
				var recommend []string
				for _, latestItem := range latestItems {
					if !excludeSet.Contains(latestItem.Id) && itemCache.IsAvailable(latestItem.Id) {
						recommend = append(recommend, latestItem.Id)
					}
				}
				candidates[category] = append(candidates[category], recommend)
			}
			latestRecommendSeconds.Add(time.Since(localStartTime).Seconds())
			candidatesChan <- candidates
			return
		}
		candidatesChan <- nil
	}()

	// Recommender #5: popular items.
	wg.Add(1)
	go func() {
		defer wg.Done()
		popularRecommendStart := time.Now()
		defer func() {
			log.Logger().Debug("TimeUse: popular", zap.String("user_id", userId), zap.Duration("popular_recommend_time", time.Since(popularRecommendStart)))
		}()
		if s.Config.Recommend.Offline.EnablePopularRecommend {
			candidates := make(map[string][][]string)
			localStartTime := time.Now()
			for _, category := range ctx.categories {
				popularItems := s.GetPopularItems(category)
				var recommend []string
				for _, popularItem := range popularItems {
					if !excludeSet.Contains(popularItem.Id) && itemCache.IsAvailable(popularItem.Id) {
						recommend = append(recommend, popularItem.Id)
					}
				}
				candidates[category] = append(candidates[category], recommend)
			}
			popularRecommendSeconds.Add(time.Since(localStartTime).Seconds())
			candidatesChan <- candidates
			return
		}
		candidatesChan <- nil
	}()

	// 等待所有goroutine完成并收集结果
	go func() {
		wg.Wait()
		close(errChan)
		close(candidatesChan)
	}()

	// 检查错误
	for err := range errChan {
		if err != nil {
			return errors.Trace(err)
		}
	}

	// 合并所有候选结果
	allCandidates := make(map[string][][]string)
	for candidates := range candidatesChan {
		for category, items := range candidates {
			allCandidates[category] = append(allCandidates[category], items...)
		}
	}

	// rank items from different recommenders
	// 1. If click-through rate prediction model is available, use it to rank items.
	// 2. If collaborative filtering model is available, use it to rank items.
	// 3. Otherwise, merge all recommenders' results randomly.
	// ctrUsed := false
	rankItemsStart := time.Now()
	results := make(map[string][]cache.Document)
	for category, catCandidates := range allCandidates {
		ranker := click.Spawn(s.ClickModel)
		var err error
		if s.Config.Recommend.Offline.EnableClickThroughPrediction && ranker != nil && !ranker.Invalid() {
			results[category], err = s.rankByClickTroughRate(&user, catCandidates, itemCache, ranker)
		} else if s.RankingModel != nil && !s.RankingModel.Invalid() &&
			s.RankingModel.IsUserPredictable(s.RankingModel.GetUserIndex().ToNumber(userId)) {
			results[category], err = s.rankByCollaborativeFiltering(userId, catCandidates)
		} else {
			results[category] = s.mergeAndShuffle(catCandidates)
		}
		if err != nil {
			log.Logger().Error("failed to rank items", zap.Error(err))
			return errors.Trace(err)
		}
	}
	log.Logger().Debug("TimeUse: rank items", zap.String("user_id", userId), zap.Duration("rank_items_time", time.Since(rankItemsStart)))

	// replacement
	replacementStart := time.Now()
	if s.Config.Recommend.Replacement.EnableReplacement {
		var err error
		if results, err = s.replacement(results, &user, historyItems, positiveItems, itemCache); err != nil {
			log.Logger().Error("failed to replace items", zap.Error(err))
			return errors.Trace(err)
		}
	}
	log.Logger().Debug("TimeUse: replacement", zap.String("user_id", userId), zap.Duration("replacement_time", time.Since(replacementStart)))

	// explore latest and popular
	exploreStart := time.Now()
	for category, result := range results {
		// 每个category只需要w.Config.Recommend.CacheSize个items
		if len(result) > s.Config.Recommend.CacheSize {
			result = result[:s.Config.Recommend.CacheSize]
		}
		scores, err := s.exploreRecommend(result, excludeSet, category)
		if err != nil {
			log.Logger().Error("failed to explore latest and popular items", zap.Error(err))
			return errors.Trace(err)
		}

		for _, item := range scores {
			if !ctx.excludeSet.Contains(item.Id) {
				ctx.results = append(ctx.results, item.Id)
				ctx.excludeSet.Add(item.Id)
			}
		}
		ctx.loadOnlineRecTime = time.Since(start)
		ctx.numFromOnline = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	log.Logger().Debug("TimeUse: explore", zap.String("user_id", userId), zap.Duration("explore_time", time.Since(exploreStart)))

	return nil
}

func (s *RestServer) pullItems(ctx context.Context) (*ItemCache, []string, error) {
	// pull items from database
	itemCache := NewItemCache()
	itemCategories := mapset.NewSet[string]()
	itemChan, errChan := s.DataClient.GetItemStream(ctx, batchSize, nil)
	for batchItems := range itemChan {
		for _, item := range batchItems {
			itemCache.Set(item.ItemId, item)
			itemCategories.Append(item.Categories...)
		}
	}
	if err := <-errChan; err != nil {
		return nil, nil, errors.Trace(err)
	}
	return itemCache, itemCategories.ToSlice(), nil
}

// ItemCache is alias of map[string]data.Item.
type ItemCache struct {
	Data      *sync.Map
	ByteCount uintptr
}

func NewItemCache() *ItemCache {
	return &ItemCache{Data: &sync.Map{}}
}

func (c *ItemCache) Set(itemId string, item data.Item) {
	if _, exist := c.Data.Load(itemId); !exist {
		c.Data.Store(itemId, &data.Item{
			ItemId:     item.ItemId,
			IsHidden:   item.IsHidden,
			Categories: item.Categories,
			Timestamp:  item.Timestamp,
			Labels:     item.Labels,
		})
	}
}

func (c *ItemCache) Get(itemId string) (*data.Item, bool) {
	item, exist := c.Data.Load(itemId)
	if !exist {
		return nil, false
	}
	return item.(*data.Item), true
}

func (c *ItemCache) GetCategory(itemId string) []string {
	if item, exist := c.Data.Load(itemId); exist {
		return item.(*data.Item).Categories
	} else {
		return nil
	}
}

// IsAvailable means the item exists in database and is not hidden.
func (c *ItemCache) IsAvailable(itemId string) bool {
	if item, exist := c.Data.Load(itemId); exist {
		return !item.(*data.Item).IsHidden
	} else {
		return false
	}
}

func (c *ItemCache) Bytes() int {
	return int(c.ByteCount)
}

// FeedbackCache is the cache for user feedbacks.
type FeedbackCache struct {
	*config.Config
	Client        data.Database
	PositiveTypes []string
	Cache         *otter.Cache[string, []data.Feedback]
	PositiveCache *otter.Cache[string, []data.Feedback]
	// ByteCount     uintptr
}

// NewFeedbackCache creates a new FeedbackCache.
func NewFeedbackCache(s *RestServer, positiveFeedbackTypes ...string) (*FeedbackCache, error) {
	cacheMaxCost := s.Config.Server.FeedbackMaxCost
	positiveCacheMaxCost := s.Config.Server.PositiveFeedbackMaxCost
	cache, err := otter.MustBuilder[string, []data.Feedback](cacheMaxCost).
		CollectStats().
		Cost(func(key string, value []data.Feedback) uint32 {
			return 1
		}).
		WithTTL(48 * time.Hour).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	positiveCache, err := otter.MustBuilder[string, []data.Feedback](positiveCacheMaxCost).
		CollectStats().
		Cost(func(key string, value []data.Feedback) uint32 {
			return 1
		}).
		WithTTL(48 * time.Hour).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &FeedbackCache{
		Config:        s.Config,
		Client:        s.DataClient,
		PositiveTypes: positiveFeedbackTypes,
		Cache:         &cache,
		PositiveCache: &positiveCache,
	}, nil
}

func (c *FeedbackCache) Set(userId string, feedbacks []data.Feedback, isPositive bool) (bool, []data.Feedback) {
	// 对 feedbacks 进行处理, 只保留 FeedbackKey, 降低cache的内存占用
	items := make([]data.Feedback, 0, len(feedbacks))
	for _, feedback := range feedbacks {
		items = append(items, data.Feedback{FeedbackKey: data.FeedbackKey{ItemId: feedback.ItemId, FeedbackType: feedback.FeedbackType}})
	}

	cache := c.Cache
	if isPositive {
		cache = c.PositiveCache
	}
	ok := cache.Set(userId, items)
	if !ok {
		log.Logger().Error("failed to set feedback to cache",
			zap.String("user_id", userId), zap.Any("items", items))
	}
	return ok, items
}

// GetUserFeedback gets user feedback from cache or database.
func (c *FeedbackCache) GetUserFeedback(ctx context.Context, userId string) ([]data.Feedback, error) {
	cache := c.Cache
	if tmp, ok := cache.Get(userId); ok {
		// log.Logger().Info("TimeUse: GetUserFeedback from cache", zap.String("user_id", userId))
		return tmp, nil
	} else {
		startTime := time.Now()
		feedbacks, err := c.Client.GetUserFeedback(ctx, userId, c.Config.Now())
		if err != nil {
			return nil, err
		}
		log.Logger().Info("TimeUse: GetUserFeedback from database", zap.String("user_id", userId), zap.Duration("database_time", time.Since(startTime)))
		_, items := c.Set(userId, feedbacks, false)
		return items, nil
	}
}

// GetUserPositiveFeedback gets user positive feedback from cache or database.
func (c *FeedbackCache) GetUserPositiveFeedback(ctx context.Context, userId string) ([]data.Feedback, error) {
	cache := c.PositiveCache
	if tmp, ok := cache.Get(userId); ok {
		// log.Logger().Info("TimeUse: GetUserFeedback from cache", zap.String("user_id", userId))
		return tmp, nil
	} else {
		startTime := time.Now()
		feedbacks, err := c.Client.GetUserFeedback(ctx, userId, c.Config.Now(), c.PositiveTypes...)
		if err != nil {
			return nil, err
		}
		log.Logger().Info("TimeUse: GetUserPositiveFeedback from database", zap.String("user_id", userId), zap.Duration("database_time", time.Since(startTime)))
		_, items := c.Set(userId, feedbacks, true)
		return items, nil
	}
}

// InsertFeedback inserts feedback to cache and database.
func (c *FeedbackCache) InsertFeedback(ctx context.Context, feedback []data.Feedback, overwrite bool) error {
	// insert feedback to database
	log.Logger().Debug("InsertFeedback", zap.Any("feedback", feedback))
	err := c.Client.BatchInsertFeedback(ctx, feedback,
		c.Config.Server.AutoInsertUser,
		c.Config.Server.AutoInsertItem, overwrite)
	if err != nil {
		return errors.Trace(err)
	}
	// append feedback to cache
	for _, f := range feedback {
		nf := data.Feedback{FeedbackKey: data.FeedbackKey{ItemId: f.ItemId, FeedbackType: f.FeedbackType}}
		// 缓存中没有这个用户的feedback，则无需处理
		if existingFeedback, exists := c.Cache.Get(f.UserId); exists {
			feedbacks := existingFeedback
			// 检查feedback是否已存在
			found := false
			for i, ef := range feedbacks {
				if ef.ItemId == f.ItemId && ef.FeedbackType == f.FeedbackType {
					found = true
					if overwrite {
						// 如果需要覆盖,则更新现有的feedback
						feedbacks[i] = nf
						c.Set(f.UserId, feedbacks, false)
					}
					break
				}
			}
			// 如果feedback不存在,则追加
			if !found {
				feedbacks = append(feedbacks, nf)
				c.Set(f.UserId, feedbacks, false)
			}
		}
		// 正反馈处理
		if funk.ContainsString(c.Config.Recommend.DataSource.PositiveFeedbackTypes, f.FeedbackType) {
			if existingFeedback, exists := c.PositiveCache.Get(f.UserId); exists {
				feedbacks := existingFeedback
				// 检查feedback是否已存在
				found := false
				for i, ef := range feedbacks {
					if ef.ItemId == f.ItemId && ef.FeedbackType == f.FeedbackType {
						found = true
						if overwrite {
							// 如果需要覆盖,则更新现有的feedback
							feedbacks[i] = nf
							c.Set(f.UserId, feedbacks, true)
						}
						break
					}
				}
				// 如果feedback不存在,则追加
				if !found {
					feedbacks = append(feedbacks, nf)
					c.Set(f.UserId, feedbacks, true)
				}
			}
		}
	}
	return nil
}

func (s *RestServer) collaborativeRecommendHNSW(rankingIndex *search.HNSW, userId string, itemCategories []string, excludeSet mapset.Set[string], itemCache *ItemCache) (map[string][]string, time.Duration, error) {
	userIndex := s.RankingModel.GetUserIndex().ToNumber(userId)
	localStartTime := time.Now()
	values, _ := rankingIndex.MultiSearch(search.NewDenseVector(s.RankingModel.GetUserFactor(userIndex), nil, false),
		itemCategories, s.Config.Recommend.CacheSize+excludeSet.Cardinality(), false)
	// save result
	recommend := make(map[string][]string)
	for category, catValues := range values {
		recommendItems := make([]string, 0, len(catValues))
		for i := range catValues {
			itemId := s.RankingModel.GetItemIndex().ToName(catValues[i])
			if !excludeSet.Contains(itemId) && itemCache.IsAvailable(itemId) {
				recommendItems = append(recommendItems, itemId)
			}
		}
		recommend[category] = recommendItems
	}
	return recommend, time.Since(localStartTime), nil
}

func (s *RestServer) collaborativeRecommendBruteForce(userId string, itemCategories []string, excludeSet mapset.Set[string], itemCache *ItemCache) (map[string][]string, time.Duration, error) {
	userIndex := s.RankingModel.GetUserIndex().ToNumber(userId)
	itemIds := s.RankingModel.GetItemIndex().GetNames()
	localStartTime := time.Now()
	recItemsFilters := make(map[string]*heap.TopKFilter[string, float64])
	for _, category := range itemCategories {
		recItemsFilters[category] = heap.NewTopKFilter[string, float64](s.Config.Recommend.CacheSize)
	}
	for itemIndex, itemId := range itemIds {
		if !excludeSet.Contains(itemId) && itemCache.IsAvailable(itemId) && s.RankingModel.IsItemPredictable(int32(itemIndex)) {
			prediction := s.RankingModel.InternalPredict(userIndex, int32(itemIndex))
			for _, category := range itemCategories {
				recItemsFilters[category].Push(itemId, float64(prediction))
			}
		}
	}
	// save result
	recommend := make(map[string][]string)
	for category, recItemsFilter := range recItemsFilters {
		recommendItems, _ := recItemsFilter.PopAll()
		recommend[category] = recommendItems
	}
	return recommend, time.Since(localStartTime), nil
}

func (s *RestServer) rankByCollaborativeFiltering(userId string, candidates [][]string) ([]cache.Document, error) {
	// concat candidates
	memo := mapset.NewSet[string]()
	var itemIds []string
	for _, v := range candidates {
		for _, itemId := range v {
			if !memo.Contains(itemId) {
				memo.Add(itemId)
				itemIds = append(itemIds, itemId)
			}
		}
	}
	// rank by collaborative filtering
	topItems := make([]cache.Document, 0, len(candidates))
	for _, itemId := range itemIds {
		topItems = append(topItems, cache.Document{
			Id:    itemId,
			Score: float64(s.RankingModel.Predict(userId, itemId)),
		})
	}
	cache.SortDocuments(topItems)
	return topItems, nil
}

// rankByClickTroughRate ranks items by predicted click-through-rate.
func (s *RestServer) rankByClickTroughRate(user *data.User, candidates [][]string, itemCache *ItemCache, predictor click.FactorizationMachine) ([]cache.Document, error) {
	// concat candidates
	memo := mapset.NewSet[string]()
	var itemIds []string
	for _, v := range candidates {
		for _, itemId := range v {
			if !memo.Contains(itemId) {
				memo.Add(itemId)
				itemIds = append(itemIds, itemId)
			}
		}
	}
	// download items
	items := make([]*data.Item, 0, len(itemIds))
	for _, itemId := range itemIds {
		if item, exist := itemCache.Get(itemId); exist {
			items = append(items, item)
		} else {
			log.Logger().Warn("item doesn't exists in database", zap.String("item_id", itemId))
		}
	}
	// rank by CTR
	topItems := make([]cache.Document, 0, len(items))
	if batchPredictor, ok := predictor.(click.BatchInference); ok {
		inputs := make([]lo.Tuple4[string, string, []click.Feature, []click.Feature], len(items))
		for i, item := range items {
			inputs[i].A = user.UserId
			inputs[i].B = item.ItemId
			inputs[i].C = click.ConvertLabelsToFeatures(user.Labels)
			inputs[i].D = click.ConvertLabelsToFeatures(item.Labels)
		}
		output := batchPredictor.BatchPredict(inputs)
		for i, score := range output {
			topItems = append(topItems, cache.Document{
				Id:    items[i].ItemId,
				Score: float64(score),
			})
		}
	} else {
		for _, item := range items {
			topItems = append(topItems, cache.Document{
				Id:    item.ItemId,
				Score: float64(predictor.Predict(user.UserId, item.ItemId, click.ConvertLabelsToFeatures(user.Labels), click.ConvertLabelsToFeatures(item.Labels))),
			})
		}
	}
	cache.SortDocuments(topItems)
	return topItems, nil
}

func (s *RestServer) mergeAndShuffle(candidates [][]string) []cache.Document {
	memo := mapset.NewSet[string]()
	pos := make([]int, len(candidates))
	var recommend []cache.Document
	for {
		// filter out ended slice
		var src []int
		for i := range candidates {
			if pos[i] < len(candidates[i]) {
				src = append(src, i)
			}
		}
		if len(src) == 0 {
			break
		}
		// select a slice randomly
		j := src[s.Server.randGenerator.Intn(len(src))]
		candidateId := candidates[j][pos[j]]
		pos[j]++
		if !memo.Contains(candidateId) {
			memo.Add(candidateId)
			recommend = append(recommend, cache.Document{Score: math.Exp(float64(-len(recommend))), Id: candidateId})
		}
	}
	return recommend
}

// replacement inserts historical items back to recommendation.
func (s *RestServer) replacement(recommend map[string][]cache.Document, user *data.User, historyItems []string, oriPositiveItems []string, itemCache *ItemCache) (map[string][]cache.Document, error) {
	upperBounds := make(map[string]float64)
	lowerBounds := make(map[string]float64)
	newRecommend := make(map[string][]cache.Document)
	for category, scores := range recommend {
		// find minimal score
		if len(scores) > 0 {
			s := lo.Map(scores, func(score cache.Document, _ int) float64 {
				return score.Score
			})
			upperBounds[category] = funk.MaxFloat64(s)
			lowerBounds[category] = funk.MinFloat64(s)
		} else {
			upperBounds[category] = math.Inf(1)
			lowerBounds[category] = math.Inf(-1)
		}
		// add scores to filters
		newRecommend[category] = append(newRecommend[category], scores...)
	}

	// remove duplicates
	positiveItems := mapset.NewSet[string]()
	distinctItems := mapset.NewSet[string]()
	for _, id := range historyItems {
		if funk.ContainsString(oriPositiveItems, id) {
			positiveItems.Add(id)
			distinctItems.Add(id)
		} else {
			distinctItems.Add(id)
		}
	}

	for _, itemId := range distinctItems.ToSlice() {
		if item, exist := itemCache.Get(itemId); exist {
			// scoring item
			// 1. If click-through rate prediction model is available, use it.
			// 2. If collaborative filtering model is available, use it.
			// 3. Otherwise, give a random score.
			var score float64
			if s.Config.Recommend.Offline.EnableClickThroughPrediction && s.ClickModel != nil {
				score = float64(s.ClickModel.Predict(user.UserId, itemId, click.ConvertLabelsToFeatures(user.Labels), click.ConvertLabelsToFeatures(item.Labels)))
			} else if s.RankingModel != nil && !s.RankingModel.Invalid() && s.RankingModel.IsUserPredictable(s.RankingModel.GetUserIndex().ToNumber(user.UserId)) {
				score = float64(s.RankingModel.Predict(user.UserId, itemId))
			} else {
				upper := upperBounds[""]
				lower := lowerBounds[""]
				if !math.IsInf(upper, 1) && !math.IsInf(lower, -1) {
					score = lower + s.Server.randGenerator.Float64()*(upper-lower)
				} else {
					score = s.Server.randGenerator.Float64()
				}
			}
			// replace item
			for _, category := range append([]string{""}, item.Categories...) {
				upperBound := upperBounds[category]
				lowerBound := lowerBounds[category]
				if !math.IsInf(upperBound, 1) && !math.IsInf(lowerBound, -1) {
					// decay item
					score -= lowerBound
					if score < 0 {
						continue
					} else if positiveItems.Contains(itemId) {
						score *= s.Config.Recommend.Replacement.PositiveReplacementDecay
					} else {
						score *= s.Config.Recommend.Replacement.ReadReplacementDecay
					}
					score += lowerBound
				}
				newRecommend[category] = append(newRecommend[category], cache.Document{Id: itemId, Score: score})
			}
		} else {
			log.Logger().Warn("item doesn't exists in database", zap.String("item_id", itemId))
		}
	}

	// rank items
	for _, r := range newRecommend {
		cache.SortDocuments(r)
	}
	return newRecommend, nil
}

func (s *RestServer) exploreRecommend(exploitRecommend []cache.Document, excludeSet mapset.Set[string], category string) ([]cache.Document, error) {
	var localExcludeSet mapset.Set[string]
	if s.Config.Recommend.Replacement.EnableReplacement {
		localExcludeSet = mapset.NewSet[string]()
	} else {
		localExcludeSet = excludeSet.Clone()
	}
	// create thresholds
	explorePopularThreshold := 0.0
	if threshold, exist := s.Config.Recommend.Offline.GetExploreRecommend("popular"); exist {
		explorePopularThreshold = threshold
	}
	exploreLatestThreshold := explorePopularThreshold
	if threshold, exist := s.Config.Recommend.Offline.GetExploreRecommend("latest"); exist {
		exploreLatestThreshold += threshold
	}
	// load popular items
	popularItems := s.GetPopularItems(category)
	// load the latest items
	latestItems := s.GetLatestItems(category)
	// explore recommendation
	var exploreRecommend []cache.Document
	score := 1.0
	if len(exploitRecommend) > 0 {
		score += exploitRecommend[0].Score
	}
	for range exploitRecommend {
		dice := s.Server.randGenerator.Float64()
		var recommendItem cache.Document
		if dice < explorePopularThreshold && len(popularItems) > 0 {
			score -= 1e-5
			recommendItem.Id = popularItems[0].Id
			recommendItem.Score = score
			popularItems = popularItems[1:]
		} else if dice < exploreLatestThreshold && len(latestItems) > 0 {
			score -= 1e-5
			recommendItem.Id = latestItems[0].Id
			recommendItem.Score = score
			latestItems = latestItems[1:]
		} else if len(exploitRecommend) > 0 {
			recommendItem = exploitRecommend[0]
			exploitRecommend = exploitRecommend[1:]
			score = recommendItem.Score
		} else {
			break
		}
		if !localExcludeSet.Contains(recommendItem.Id) {
			localExcludeSet.Add(recommendItem.Id)
			exploreRecommend = append(exploreRecommend, recommendItem)
		}
	}
	return exploreRecommend, nil
}

func (s *RestServer) RecommendOffline(ctx *recommendContext) error {
	log.Logger().Debug("start offline recommendation")
	if len(ctx.results) < ctx.n {
		start := time.Now()
		recommendation, err := s.CacheClient.SearchDocuments(ctx.context, cache.OfflineRecommend, ctx.userId, ctx.categories, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range recommendation {
			if !ctx.excludeSet.Contains(item.Id) {
				ctx.results = append(ctx.results, item.Id)
				ctx.excludeSet.Add(item.Id)
			}
		}
		ctx.loadOfflineRecTime = time.Since(start)
		ctx.numFromOffline = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) RecommendCollaborative(ctx *recommendContext) error {
	if len(ctx.results) < ctx.n {
		start := time.Now()
		collaborativeRecommendation, err := s.CacheClient.SearchDocuments(ctx.context, cache.CollaborativeRecommend, ctx.userId, ctx.categories, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range collaborativeRecommendation {
			if !ctx.excludeSet.Contains(item.Id) {
				ctx.results = append(ctx.results, item.Id)
				ctx.excludeSet.Add(item.Id)
			}
		}
		ctx.loadColRecTime = time.Since(start)
		ctx.numFromCollaborative = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) RecommendUserBased(ctx *recommendContext) error {
	if len(ctx.results) < ctx.n {
		start := time.Now()
		candidates := make(map[string]float64)
		// load similar users
		similarUsers, err := s.CacheClient.SearchDocuments(ctx.context, cache.UserNeighbors, ctx.userId, []string{""}, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return errors.Trace(err)
		}
		for _, user := range similarUsers {
			// load historical feedback
			feedbacks, err := s.DataClient.GetUserFeedback(ctx.context, user.Id, s.Config.Now(), s.Config.Recommend.DataSource.PositiveFeedbackTypes...)
			if err != nil {
				return errors.Trace(err)
			}
			// add unseen items
			for _, feedback := range feedbacks {
				if !ctx.excludeSet.Contains(feedback.ItemId) {
					item, err := s.DataClient.GetItem(ctx.context, feedback.ItemId)
					if err != nil {
						return errors.Trace(err)
					}
					if funk.Equal(ctx.categories, []string{""}) || funk.Subset(ctx.categories, item.Categories) {
						candidates[feedback.ItemId] += user.Score
					}
				}
			}
		}
		// collect top k
		k := ctx.n - len(ctx.results)
		filter := heap.NewTopKFilter[string, float64](k)
		for id, score := range candidates {
			filter.Push(id, score)
		}
		ids, _ := filter.PopAll()
		ctx.results = append(ctx.results, ids...)
		ctx.excludeSet.Append(ids...)
		ctx.userBasedTime = time.Since(start)
		ctx.numFromUserBased = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) RecommendItemBased(ctx *recommendContext) error {
	if len(ctx.results) < ctx.n {
		start := time.Now()
		// truncate user feedback
		data.SortFeedbacks(ctx.userFeedback)
		userFeedback := make([]data.Feedback, 0, s.Config.Recommend.Online.NumFeedbackFallbackItemBased)
		for _, feedback := range ctx.userFeedback {
			if s.Config.Recommend.Online.NumFeedbackFallbackItemBased <= len(userFeedback) {
				break
			}
			if funk.ContainsString(s.Config.Recommend.DataSource.PositiveFeedbackTypes, feedback.FeedbackType) {
				userFeedback = append(userFeedback, feedback)
			}
		}
		// collect candidates
		candidates := make(map[string]float64)
		for _, feedback := range userFeedback {
			// load similar items
			similarItems, err := s.CacheClient.SearchDocuments(ctx.context, cache.ItemNeighbors, feedback.ItemId, ctx.categories, 0, s.Config.Recommend.CacheSize)
			if err != nil {
				return errors.Trace(err)
			}
			// add unseen items
			for _, item := range similarItems {
				if !ctx.excludeSet.Contains(item.Id) {
					candidates[item.Id] += item.Score
				}
			}
		}
		// collect top k
		k := ctx.n - len(ctx.results)
		filter := heap.NewTopKFilter[string, float64](k)
		for id, score := range candidates {
			filter.Push(id, score)
		}
		ids, _ := filter.PopAll()
		ctx.results = append(ctx.results, ids...)
		ctx.excludeSet.Append(ids...)
		ctx.itemBasedTime = time.Since(start)
		ctx.numFromItemBased = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) RecommendLatest(ctx *recommendContext) error {
	if len(ctx.results) < ctx.n {
		start := time.Now()
		items, err := s.CacheClient.SearchDocuments(ctx.context, cache.LatestItems, "", ctx.categories, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range items {
			if !ctx.excludeSet.Contains(item.Id) {
				ctx.results = append(ctx.results, item.Id)
				ctx.excludeSet.Add(item.Id)
			}
		}
		ctx.loadLatestTime = time.Since(start)
		ctx.numFromLatest = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) RecommendPopular(ctx *recommendContext) error {
	if len(ctx.results) < ctx.n {
		start := time.Now()
		items, err := s.CacheClient.SearchDocuments(ctx.context, cache.PopularItems, "", ctx.categories, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range items {
			if !ctx.excludeSet.Contains(item.Id) {
				ctx.results = append(ctx.results, item.Id)
				ctx.excludeSet.Add(item.Id)
			}
		}
		ctx.loadPopularTime = time.Since(start)
		ctx.numFromPopular = len(ctx.results) - ctx.numPrevStage
		ctx.numPrevStage = len(ctx.results)
	}
	return nil
}

func (s *RestServer) getRecommend(request *restful.Request, response *restful.Response) {
	log.Logger().Debug("server start get recommendation")
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// parse arguments
	userId := request.PathParameter("user-id")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	categories := request.QueryParameters("category")
	if len(categories) == 0 {
		categories = []string{request.PathParameter("category")}
	}
	offset, err := ParseInt(request, "offset", 0)
	if err != nil {
		BadRequest(response, err)
		return
	}
	writeBackFeedback := request.QueryParameter("write-back-type")
	writeBackDelay, err := ParseDuration(request, "write-back-delay")
	if err != nil {
		BadRequest(response, err)
		return
	}
	// online recommendation
	recommenders := []Recommender{s.RecommendOnline}
	for _, recommender := range s.Config.Recommend.Online.FallbackRecommend {
		switch recommender {
		case "collaborative":
			recommenders = append(recommenders, s.RecommendCollaborative)
		case "item_based":
			recommenders = append(recommenders, s.RecommendItemBased)
		case "user_based":
			recommenders = append(recommenders, s.RecommendUserBased)
		case "latest":
			recommenders = append(recommenders, s.RecommendLatest)
		case "popular":
			recommenders = append(recommenders, s.RecommendPopular)
		default:
			InternalServerError(response, fmt.Errorf("unknown fallback recommendation method `%s`", recommender))
			return
		}
	}
	results, err := s.Recommend(ctx, response, userId, categories, offset+n, recommenders...)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	results = results[mathutil.Min(offset, len(results)):]
	// write back
	if writeBackFeedback != "" {
		startTime := time.Now()
		for _, itemId := range results {
			// insert to data store
			feedback := data.Feedback{
				FeedbackKey: data.FeedbackKey{
					UserId:       userId,
					ItemId:       itemId,
					FeedbackType: writeBackFeedback,
				},
				Timestamp: startTime.Add(writeBackDelay),
			}
			err = s.DataClient.BatchInsertFeedback(ctx, []data.Feedback{feedback}, false, false, false)
			if err != nil {
				InternalServerError(response, err)
				return
			}
		}
	}
	// Send result
	Ok(response, results)
}

func (s *RestServer) sessionRecommend(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// parse arguments
	var feedbacks []Feedback
	if err := request.ReadEntity(&feedbacks); err != nil {
		BadRequest(response, err)
		return
	}
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	category := request.PathParameter("category")
	offset, err := ParseInt(request, "offset", 0)
	if err != nil {
		BadRequest(response, err)
		return
	}

	// pre-process feedback
	dataFeedback := make([]data.Feedback, len(feedbacks))
	for i := range dataFeedback {
		var err error
		dataFeedback[i], err = feedbacks[i].ToDataFeedback()
		if err != nil {
			BadRequest(response, err)
			return
		}
	}
	data.SortFeedbacks(dataFeedback)

	// item-based recommendation
	var excludeSet = mapset.NewSet[string]()
	var userFeedback []data.Feedback
	for _, feedback := range dataFeedback {
		excludeSet.Add(feedback.ItemId)
		if funk.ContainsString(s.Config.Recommend.DataSource.PositiveFeedbackTypes, feedback.FeedbackType) {
			userFeedback = append(userFeedback, feedback)
		}
	}
	// collect candidates
	candidates := make(map[string]float64)
	usedFeedbackCount := 0
	for _, feedback := range userFeedback {
		// load similar items
		similarItems, err := s.CacheClient.SearchDocuments(ctx, cache.ItemNeighbors, feedback.ItemId, []string{category}, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			BadRequest(response, err)
			return
		}
		// add unseen items
		// similarItems = s.FilterOutHiddenScores(response, similarItems, "")
		for _, item := range similarItems {
			if !excludeSet.Contains(item.Id) {
				candidates[item.Id] += item.Score
			}
		}
		// finish recommendation if the number of used feedbacks is enough
		if len(similarItems) > 0 {
			usedFeedbackCount++
			if usedFeedbackCount >= s.Config.Recommend.Online.NumFeedbackFallbackItemBased {
				break
			}
		}
	}
	// collect top k
	filter := heap.NewTopKFilter[string, float64](n + offset)
	for id, score := range candidates {
		filter.Push(id, score)
	}
	names, scores := filter.PopAll()
	result := lo.Map(names, func(_ string, i int) cache.Document {
		return cache.Document{
			Id:    names[i],
			Score: scores[i],
		}
	})
	if len(result) > offset {
		result = result[offset:]
	} else {
		result = nil
	}
	result = result[:lo.Min([]int{len(result), n})]
	// Send result
	Ok(response, result)
}

// Success is the returned data structure for data insert operations.
type Success struct {
	RowAffected int
}

func (s *RestServer) insertUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	temp := data.User{}
	// get userInfo from request and put into temp
	if err := request.ReadEntity(&temp); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	if err := data.ValidateLabels(temp.Labels); err != nil {
		BadRequest(response, err)
		return
	}
	if err := s.DataClient.BatchInsertUsers(ctx, []data.User{temp}); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert modify timestamp
	if err := s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyUserTime, temp.UserId), time.Now())); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: 1})
}

func (s *RestServer) modifyUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// get user id
	userId := request.PathParameter("user-id")
	// modify user
	var patch data.UserPatch
	if err := request.ReadEntity(&patch); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	if err := data.ValidateLabels(patch.Labels); err != nil {
		BadRequest(response, err)
		return
	}
	if err := s.DataClient.ModifyUser(ctx, userId, patch); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert modify timestamp
	if err := s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyUserTime, userId), time.Now())); err != nil {
		return
	}
	Ok(response, Success{RowAffected: 1})
}

func (s *RestServer) getUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// get user id
	userId := request.PathParameter("user-id")
	// get user
	user, err := s.DataClient.GetUser(ctx, userId)
	if err != nil {
		if errors.Is(err, errors.NotFound) {
			PageNotFound(response, err)
		} else {
			InternalServerError(response, err)
		}
		return
	}
	Ok(response, user)
}

func (s *RestServer) insertUsers(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	var temp []data.User
	// get param from request and put into temp
	if err := request.ReadEntity(&temp); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	for _, user := range temp {
		if err := data.ValidateLabels(user.Labels); err != nil {
			BadRequest(response, err)
			return
		}
	}
	// range temp and achieve user
	if err := s.DataClient.BatchInsertUsers(ctx, temp); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert modify timestamp
	values := make([]cache.Value, len(temp))
	for i, user := range temp {
		values[i] = cache.Time(cache.Key(cache.LastModifyUserTime, user.UserId), time.Now())
	}
	if err := s.CacheClient.Set(ctx, values...); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: len(temp)})
}

type UserIterator struct {
	Cursor string
	Users  []data.User
}

func (s *RestServer) getUsers(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	cursor := request.QueryParameter("cursor")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	// get all users
	cursor, users, err := s.DataClient.GetUsers(ctx, cursor, n)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, UserIterator{Cursor: cursor, Users: users})
}

// delete a user by user-id
func (s *RestServer) deleteUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// get user-id and put into temp
	userId := request.PathParameter("user-id")
	if err := s.DataClient.DeleteUser(ctx, userId); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: 1})
}

// get feedback by user-id with feedback type
func (s *RestServer) getTypedFeedbackByUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	feedbackType := request.PathParameter("feedback-type")
	userId := request.PathParameter("user-id")
	feedback, err := s.DataClient.GetUserFeedback(ctx, userId, s.Config.Now(), feedbackType)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, feedback)
}

// get feedback by user-id
func (s *RestServer) getFeedbackByUser(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	userId := request.PathParameter("user-id")
	feedback, err := s.DataClient.GetUserFeedback(ctx, userId, s.Config.Now())
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, feedback)
}

// Item is the data structure for the item but stores the timestamp using string.
type Item struct {
	ItemId     string
	IsHidden   bool
	Categories []string
	Timestamp  string
	Labels     any
	Comment    string
}

func (s *RestServer) batchInsertItems(ctx context.Context, response *restful.Response, temp []Item) {
	var (
		count int
		items = make([]data.Item, 0, len(temp))
		// popularScore = lo.Map(temp, func(item Item, i int) float64 {
		// 	return s.PopularItemsCache.GetSortedScore(item.ItemId)
		// })

		loadExistedItemsTime time.Duration
		parseTimesatmpTime   time.Duration
		insertItemsTime      time.Duration
		insertCacheTime      time.Duration
	)
	// load existed items
	start := time.Now()
	existedItems, err := s.DataClient.BatchGetItems(ctx, lo.Map(temp, func(t Item, i int) string {
		return t.ItemId
	}))
	if err != nil {
		InternalServerError(response, err)
		return
	}
	existedItemsSet := make(map[string]data.Item)
	for _, item := range existedItems {
		existedItemsSet[item.ItemId] = item
	}
	loadExistedItemsTime = time.Since(start)

	start = time.Now()
	for _, item := range temp {
		// parse datetime
		var timestamp time.Time
		var err error
		if item.Timestamp != "" {
			if timestamp, err = dateparse.ParseAny(item.Timestamp); err != nil {
				BadRequest(response, err)
				return
			}
		}
		items = append(items, data.Item{
			ItemId:     item.ItemId,
			IsHidden:   item.IsHidden,
			Categories: item.Categories,
			Timestamp:  timestamp,
			Labels:     item.Labels,
			Comment:    item.Comment,
		})
		// insert to latest items cache
		if err = s.CacheClient.AddDocuments(ctx, cache.LatestItems, "", []cache.Document{{
			Id:         item.ItemId,
			Score:      float64(timestamp.Unix()),
			Categories: withWildCard(item.Categories),
			Timestamp:  time.Now(),
		}}); err != nil {
			InternalServerError(response, err)
			return
		}
		// update items cache
		if err = s.CacheClient.UpdateDocuments(ctx, cache.ItemCache, item.ItemId, cache.DocumentPatch{
			Categories: withWildCard(item.Categories),
			IsHidden:   &item.IsHidden,
		}); err != nil {
			InternalServerError(response, err)
			return
		}
		count++
	}
	parseTimesatmpTime = time.Since(start)

	// insert items
	start = time.Now()
	if err = s.DataClient.BatchInsertItems(ctx, items); err != nil {
		InternalServerError(response, err)
		return
	}
	insertItemsTime = time.Since(start)

	// insert modify timestamp
	start = time.Now()
	categories := mapset.NewSet[string]()
	values := make([]cache.Value, len(items))
	for i, item := range items {
		values[i] = cache.Time(cache.Key(cache.LastModifyItemTime, item.ItemId), time.Now())
		categories.Append(item.Categories...)
	}
	if err = s.CacheClient.Set(ctx, values...); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert categories
	if err = s.CacheClient.AddSet(ctx, cache.ItemCategories, categories.ToSlice()...); err != nil {
		InternalServerError(response, err)
		return
	}

	insertCacheTime = time.Since(start)
	log.ResponseLogger(response).Info("batch insert items",
		zap.Duration("load_existed_items_time", loadExistedItemsTime),
		zap.Duration("parse_timestamp_time", parseTimesatmpTime),
		zap.Duration("insert_items_time", insertItemsTime),
		zap.Duration("insert_cache_time", insertCacheTime))
	Ok(response, Success{RowAffected: count})
}

func (s *RestServer) insertItems(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	var items []Item
	if err := request.ReadEntity(&items); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	for _, user := range items {
		if err := data.ValidateLabels(user.Labels); err != nil {
			BadRequest(response, err)
			return
		}
	}
	// Insert items
	s.batchInsertItems(ctx, response, items)
}

func (s *RestServer) insertItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	var item Item
	var err error
	if err = request.ReadEntity(&item); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	if err := data.ValidateLabels(item.Labels); err != nil {
		BadRequest(response, err)
		return
	}
	s.batchInsertItems(ctx, response, []Item{item})
}

func (s *RestServer) modifyItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	itemId := request.PathParameter("item-id")
	var patch data.ItemPatch
	if err := request.ReadEntity(&patch); err != nil {
		BadRequest(response, err)
		return
	}
	// validate labels
	if err := data.ValidateLabels(patch.Labels); err != nil {
		BadRequest(response, err)
		return
	}
	// remove hidden item from cache
	if patch.IsHidden != nil {
		if err := s.CacheClient.UpdateDocuments(ctx, cache.ItemCache, itemId, cache.DocumentPatch{IsHidden: patch.IsHidden}); err != nil {
			InternalServerError(response, err)
			return
		}
	}
	// add item to latest items cache
	if patch.Timestamp != nil {
		if err := s.CacheClient.UpdateDocuments(ctx, []string{cache.LatestItems}, itemId, cache.DocumentPatch{Score: proto.Float64(float64(patch.Timestamp.Unix()))}); err != nil {
			InternalServerError(response, err)
			return
		}
	}
	// update categories in cache
	if patch.Categories != nil {
		if err := s.CacheClient.UpdateDocuments(ctx, cache.ItemCache, itemId, cache.DocumentPatch{Categories: withWildCard(patch.Categories)}); err != nil {
			InternalServerError(response, err)
			return
		}
	}
	// modify item
	if err := s.DataClient.ModifyItem(ctx, itemId, patch); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert modify timestamp
	if err := s.CacheClient.Set(ctx, cache.Time(cache.Key(cache.LastModifyItemTime, itemId), time.Now())); err != nil {
		return
	}
	Ok(response, Success{RowAffected: 1})
}

// ItemIterator is the iterator for items.
type ItemIterator struct {
	Cursor string
	Items  []data.Item
}

func (s *RestServer) getItems(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	cursor := request.QueryParameter("cursor")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	cursor, items, err := s.DataClient.GetItems(ctx, cursor, n, nil)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, ItemIterator{Cursor: cursor, Items: items})
}

func (s *RestServer) getItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Get item id
	itemId := request.PathParameter("item-id")
	// Get item
	item, err := s.DataClient.GetItem(ctx, itemId)
	if err != nil {
		if errors.Is(err, errors.NotFound) {
			PageNotFound(response, err)
		} else {
			InternalServerError(response, err)
		}
		return
	}
	Ok(response, item)
}

func (s *RestServer) deleteItem(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	itemId := request.PathParameter("item-id")
	// delete item from database
	if err := s.DataClient.DeleteItem(ctx, itemId); err != nil {
		InternalServerError(response, err)
		return
	}
	// delete item from cache
	if err := s.CacheClient.DeleteDocuments(ctx, cache.ItemCache, cache.DocumentCondition{Id: &itemId}); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: 1})
}

func (s *RestServer) insertItemCategory(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// fetch item id and category
	itemId := request.PathParameter("item-id")
	category := request.PathParameter("category")
	// fetch item
	item, err := s.DataClient.GetItem(ctx, itemId)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	if !funk.ContainsString(item.Categories, category) {
		item.Categories = append(item.Categories, category)
	}
	// insert category to database
	if err = s.DataClient.BatchInsertItems(ctx, []data.Item{item}); err != nil {
		InternalServerError(response, err)
		return
	}
	// insert category to cache
	if err = s.CacheClient.UpdateDocuments(ctx, cache.ItemCache, itemId, cache.DocumentPatch{Categories: withWildCard(item.Categories)}); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: 1})
}

func (s *RestServer) deleteItemCategory(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// fetch item id and category
	itemId := request.PathParameter("item-id")
	category := request.PathParameter("category")
	// fetch item
	item, err := s.DataClient.GetItem(ctx, itemId)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	categories := make([]string, 0, len(item.Categories))
	for _, cat := range item.Categories {
		if cat != category {
			categories = append(categories, cat)
		}
	}
	item.Categories = categories
	// delete category from cache
	if err = s.CacheClient.UpdateDocuments(ctx, cache.ItemCache, itemId, cache.DocumentPatch{Categories: withWildCard(categories)}); err != nil {
		InternalServerError(response, err)
		return
	}
	// delete category from database
	if err = s.DataClient.BatchInsertItems(ctx, []data.Item{item}); err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, Success{RowAffected: 1})
}

// Feedback is the data structure for the feedback but stores the timestamp using string.
type Feedback struct {
	data.FeedbackKey
	Timestamp string
	Comment   string
}

func (f Feedback) ToDataFeedback() (data.Feedback, error) {
	var feedback data.Feedback
	feedback.FeedbackKey = f.FeedbackKey
	feedback.Comment = f.Comment
	if f.Timestamp != "" {
		var err error
		feedback.Timestamp, err = dateparse.ParseAny(f.Timestamp)
		if err != nil {
			return data.Feedback{}, err
		}
	}
	return feedback, nil
}

func (s *RestServer) insertFeedback(overwrite bool) func(request *restful.Request, response *restful.Response) {
	return func(request *restful.Request, response *restful.Response) {
		ctx := context.Background()
		if request != nil && request.Request != nil {
			ctx = request.Request.Context()
		}
		// add ratings
		var feedbackLiterTime []Feedback
		if err := request.ReadEntity(&feedbackLiterTime); err != nil {
			BadRequest(response, err)
			return
		}
		// parse datetime
		var err error
		feedback := make([]data.Feedback, len(feedbackLiterTime))
		users := mapset.NewSet[string]()
		items := mapset.NewSet[string]()
		for i := range feedback {
			users.Add(feedbackLiterTime[i].UserId)
			items.Add(feedbackLiterTime[i].ItemId)
			feedback[i], err = feedbackLiterTime[i].ToDataFeedback()
			if err != nil {
				BadRequest(response, err)
				return
			}
		}
		// insert feedback to data store
		// err = s.DataClient.BatchInsertFeedback(ctx, feedback,
		// 	s.Config.Server.AutoInsertUser,
		// 	s.Config.Server.AutoInsertItem, overwrite)

		// insert feedback to cache and data store
		err = s.feedbackCache.InsertFeedback(ctx, feedback, overwrite)
		if err != nil {
			InternalServerError(response, err)
			return
		}
		values := make([]cache.Value, 0, users.Cardinality()+items.Cardinality())
		for _, userId := range users.ToSlice() {
			values = append(values, cache.Time(cache.Key(cache.LastModifyUserTime, userId), time.Now()))
		}
		for _, itemId := range items.ToSlice() {
			values = append(values, cache.Time(cache.Key(cache.LastModifyItemTime, itemId), time.Now()))
		}
		if err = s.CacheClient.Set(ctx, values...); err != nil {
			InternalServerError(response, err)
			return
		}
		log.ResponseLogger(response).Info("Insert feedback successfully", zap.Int("num_feedback", len(feedback)))
		Ok(response, Success{RowAffected: len(feedback)})
	}
}

// FeedbackIterator is the iterator for feedback.
type FeedbackIterator struct {
	Cursor   string
	Feedback []data.Feedback
}

func (s *RestServer) getFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	cursor := request.QueryParameter("cursor")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	cursor, feedback, err := s.DataClient.GetFeedback(ctx, cursor, n, nil, s.Config.Now())
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, FeedbackIterator{Cursor: cursor, Feedback: feedback})
}

func (s *RestServer) getTypedFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	feedbackType := request.PathParameter("feedback-type")
	cursor := request.QueryParameter("cursor")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	cursor, feedback, err := s.DataClient.GetFeedback(ctx, cursor, n, nil, s.Config.Now(), feedbackType)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, FeedbackIterator{Cursor: cursor, Feedback: feedback})
}

func (s *RestServer) getUserItemFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	userId := request.PathParameter("user-id")
	itemId := request.PathParameter("item-id")
	if feedback, err := s.DataClient.GetUserItemFeedback(ctx, userId, itemId); err != nil {
		InternalServerError(response, err)
	} else {
		Ok(response, feedback)
	}
}

func (s *RestServer) deleteUserItemFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	userId := request.PathParameter("user-id")
	itemId := request.PathParameter("item-id")
	if deleteCount, err := s.DataClient.DeleteUserItemFeedback(ctx, userId, itemId); err != nil {
		InternalServerError(response, err)
	} else {
		Ok(response, Success{RowAffected: deleteCount})
	}
}

func (s *RestServer) getTypedUserItemFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	feedbackType := request.PathParameter("feedback-type")
	userId := request.PathParameter("user-id")
	itemId := request.PathParameter("item-id")
	if feedback, err := s.DataClient.GetUserItemFeedback(ctx, userId, itemId, feedbackType); err != nil {
		InternalServerError(response, err)
	} else if feedbackType == "" {
		Text(response, "{}")
	} else {
		Ok(response, feedback[0])
	}
}

func (s *RestServer) deleteTypedUserItemFeedback(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	feedbackType := request.PathParameter("feedback-type")
	userId := request.PathParameter("user-id")
	itemId := request.PathParameter("item-id")
	if deleteCount, err := s.DataClient.DeleteUserItemFeedback(ctx, userId, itemId, feedbackType); err != nil {
		InternalServerError(response, err)
	} else {
		Ok(response, Success{deleteCount})
	}
}

type HealthStatus struct {
	Ready               bool
	DataStoreError      error
	CacheStoreError     error
	DataStoreConnected  bool
	CacheStoreConnected bool
}

func (s *RestServer) checkHealth() HealthStatus {
	healthStatus := HealthStatus{}
	healthStatus.DataStoreError = s.DataClient.Ping()
	healthStatus.CacheStoreError = s.CacheClient.Ping()
	healthStatus.DataStoreConnected = healthStatus.DataStoreError == nil
	healthStatus.CacheStoreConnected = healthStatus.CacheStoreError == nil
	healthStatus.Ready = healthStatus.DataStoreConnected && healthStatus.CacheStoreConnected
	return healthStatus
}

func (s *RestServer) checkReady(_ *restful.Request, response *restful.Response) {
	healthStatus := s.checkHealth()
	if healthStatus.Ready {
		Ok(response, healthStatus)
	} else {
		errReason, err := json.Marshal(healthStatus)
		if err != nil {
			Error(response, http.StatusInternalServerError, err)
		} else {
			Error(response, http.StatusServiceUnavailable, errors.New(string(errReason)))
		}
	}
}

func (s *RestServer) checkLive(_ *restful.Request, response *restful.Response) {
	healthStatus := s.checkHealth()
	Ok(response, healthStatus)
}

func (s *RestServer) getMeasurements(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	// Parse parameters
	name := request.PathParameter("name")
	n, err := ParseInt(request, "n", 100)
	if err != nil {
		BadRequest(response, err)
		return
	}
	measurements, err := s.CacheClient.GetTimeSeriesPoints(ctx, name, time.Now().Add(-24*time.Hour*time.Duration(n)), time.Now())
	if err != nil {
		InternalServerError(response, err)
		return
	}
	Ok(response, measurements)
}

// BadRequest returns a bad request error.
func BadRequest(response *restful.Response, err error) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	log.ResponseLogger(response).Error("bad request", zap.Error(err))
	if err = response.WriteError(http.StatusBadRequest, err); err != nil {
		log.ResponseLogger(response).Error("failed to write error", zap.Error(err))
	}
}

// InternalServerError returns a internal server error.
func InternalServerError(response *restful.Response, err error) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	log.ResponseLogger(response).Error("internal server error", zap.Error(err))
	if err = response.WriteError(http.StatusInternalServerError, err); err != nil {
		log.ResponseLogger(response).Error("failed to write error", zap.Error(err))
	}
}

// PageNotFound returns a not found error.
func PageNotFound(response *restful.Response, err error) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	if err := response.WriteError(http.StatusNotFound, err); err != nil {
		log.ResponseLogger(response).Error("failed to write error", zap.Error(err))
	}
}

// Ok sends the content as JSON to the client.
func Ok(response *restful.Response, content interface{}) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	if err := response.WriteAsJson(content); err != nil {
		log.ResponseLogger(response).Error("failed to write json", zap.Error(err))
	}
}

func Error(response *restful.Response, httpStatus int, responseError error) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	if err := response.WriteError(httpStatus, responseError); err != nil {
		log.ResponseLogger(response).Error("failed to write error", zap.Error(err))
	}
}

// Text returns a plain text.
func Text(response *restful.Response, content string) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	if _, err := response.Write([]byte(content)); err != nil {
		log.ResponseLogger(response).Error("failed to write text", zap.Error(err))
	}
}

func withWildCard(categories []string) []string {
	result := make([]string, len(categories), len(categories)+1)
	copy(result, categories)
	result = append(result, "")
	return result
}

// 添加初始化和更新 itemCache 的方法
func (s *RestServer) initItemCache(ctx context.Context) error {
	itemCache, _, err := s.pullItems(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	s.itemCache = itemCache

	// 启动定期更新
	go s.periodicItemCacheUpdate(ctx)

	return nil
}

func (s *RestServer) periodicItemCacheUpdate(ctx context.Context) {
	// FIXME 可以由master节点收到更新数据时通知更新
	// 每5分钟更新一次
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			itemCache, _, err := s.pullItems(ctx)
			if err != nil {
				log.Logger().Error("failed to update item cache", zap.Error(err))
				continue
			}

			s.itemCache = itemCache

			log.Logger().Info("item cache updated")
				// 这样计算cache_size_bytes不准确
				/*zap.Int("cache_size_bytes", itemCache.Bytes())*/
		}
	}
}

// 初始化缓存
func (s *RestServer) initCache(ctx context.Context) error {
	// 初始化map
	s.popularItems = &sync.Map{}
	s.latestItems = &sync.Map{}
	s.itemNeighbors = &sync.Map{}
	s.userNeighbors = &sync.Map{}

	// 从缓存数据库加载数据
	if err := s.loadPopularItems(ctx); err != nil {
		return err
	}
	if err := s.loadLatestItems(ctx); err != nil {
		return err
	}

	// 启动定期更新
	go s.periodicCacheUpdate(ctx)

	return nil
}

// 加载热门物品
func (s *RestServer) loadPopularItems(ctx context.Context) error {
	// 获取所有分类
	categories, err := s.CacheClient.GetSet(ctx, cache.ItemCategories)
	if err != nil {
		return err
	}
	categories = append(categories, "") // 添加空分类

	// 加载每个分类的热门物品
	for _, category := range categories {
		items, err := s.CacheClient.SearchDocuments(ctx, cache.PopularItems, "", []string{category}, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return err
		}
		s.popularItems.Store(category, items)

		log.Logger().Info("popular items cache updated",
			zap.String("category", category),
			zap.Int("item_count", len(items)),
			// 这样计算cache_size_bytes不准确
			/*zap.Int("cache_size_bytes", len(items)*8)*/)
	}
	return nil
}

// 加载最新物品
func (s *RestServer) loadLatestItems(ctx context.Context) error {
	// 获取所有分类
	categories, err := s.CacheClient.GetSet(ctx, cache.ItemCategories)
	if err != nil {
		return err
	}
	categories = append(categories, "") // 添加空分类

	// 加载每个分类的最新物品
	for _, category := range categories {
		items, err := s.CacheClient.SearchDocuments(ctx, cache.LatestItems, "", []string{category}, 0, s.Config.Recommend.CacheSize)
		if err != nil {
			return err
		}
		s.latestItems.Store(category, items)

		log.Logger().Info("latest items cache updated",
			zap.String("category", category),
			zap.Int("item_count", len(items)),
			// 这样计算cache_size_bytes不准确
			/*zap.Int("cache_size_bytes", len(items)*8)*/)
	}
	return nil
}

func (s *RestServer) loadNeighbors(ctx context.Context) error {
	if s.Server.masterClient == nil {
		log.Logger().Warn("master client is not initialized")
		return nil
	}
	neighborsStatus, err := s.Server.masterClient.GetNeighborsStatus(ctx, &protocol.Empty{})
	if err != nil {
		return err
	}

	log.Logger().Debug("neighbors status",
		zap.Bool("item_neighbors_finished", neighborsStatus.ItemNeighborsFinished),
		zap.Bool("user_neighbors_finished", neighborsStatus.UserNeighborsFinished),
		zap.Int64("item_neighbors_version", neighborsStatus.ItemNeighborsVersion),
		zap.Int64("user_neighbors_version", neighborsStatus.UserNeighborsVersion))

	if s.itemNeighborsVersion != neighborsStatus.ItemNeighborsVersion {
		s.itemNeighborsVersion = neighborsStatus.ItemNeighborsVersion
		s.itemNeighbors = &sync.Map{}
		log.Logger().Info("item neighbors cache updated",
			zap.Int64("version", s.itemNeighborsVersion))
	}
	if s.userNeighborsVersion != neighborsStatus.UserNeighborsVersion {
		s.userNeighborsVersion = neighborsStatus.UserNeighborsVersion
		s.userNeighbors = &sync.Map{}
		log.Logger().Info("user neighbors cache updated",
			zap.Int64("version", s.userNeighborsVersion))
	}

	return nil
}

// 定期更新缓存
func (s *RestServer) periodicCacheUpdate(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.loadPopularItems(ctx); err != nil {
				log.Logger().Error("failed to update popular items cache", zap.Error(err))
			}
			if err := s.loadLatestItems(ctx); err != nil {
				log.Logger().Error("failed to update latest items cache", zap.Error(err))
			}
			if err := s.loadNeighbors(ctx); err != nil {
				log.Logger().Error("failed to update neighbors cache", zap.Error(err))
			}
			s.reportStat()
		}
	}
}

// 获取热门物品
func (s *RestServer) GetPopularItems(category string) []cache.Document {
	items, exists := s.popularItems.Load(category)
    if !exists || items == nil {
        log.Logger().Warn("popular items is nil or not exists",
            zap.String("category", category),
            zap.Bool("exists", exists),
            zap.Bool("is_nil", items == nil))
        return []cache.Document{}
    }
	return items.([]cache.Document)
}

// 获取最新物品
func (s *RestServer) GetLatestItems(category string) []cache.Document {
	items, exists := s.latestItems.Load(category)
    if !exists || items == nil {
        log.Logger().Warn("latest items is nil or not exists",
            zap.String("category", category),
            zap.Bool("exists", exists),
            zap.Bool("is_nil", items == nil))
        return []cache.Document{}
    }
	return items.([]cache.Document)
}

// 获取物品邻居
func (s *RestServer) GetItemNeighbors(itemId string, category string) ([]cache.Document, error) {
	items, _ := s.itemNeighbors.Load(itemId)
	if items != nil {
		return items.([]cache.Document), nil
	}

	// 如果缓存中没有,从缓存数据库加载
	items, err := s.CacheClient.SearchDocuments(context.Background(), cache.ItemNeighbors, itemId, []string{category}, 0, s.Config.Recommend.CacheSize)
	if err != nil {
		return nil, err
	}
	var newItems []cache.Document
	for _, item := range items.([]cache.Document) {
		newItems = append(newItems, cache.Document{Id: item.Id, Score: item.Score})
	}

	// 写入缓存
	s.itemNeighbors.Store(itemId, newItems)

	return newItems, nil
}

// 获取用户邻居
func (s *RestServer) GetUserNeighbors(userId string) ([]cache.Document, error) {
	users, _ := s.userNeighbors.Load(userId)
	if users != nil {
		return users.([]cache.Document), nil
	}

	// 如果缓存中没有,从缓存数据库加载
	users, err := s.CacheClient.SearchDocuments(context.Background(), cache.UserNeighbors, userId, []string{""}, 0, s.Config.Recommend.CacheSize)
	if err != nil {
		return nil, err
	}

	var newUsers []cache.Document
	for _, user := range users.([]cache.Document) {
		newUsers = append(newUsers, cache.Document{Id: user.Id, Score: user.Score})
	}

	// 写入缓存
	s.userNeighbors.Store(userId, newUsers)

	return newUsers, nil
}

func (s *RestServer) reportStat() {
	stats := s.feedbackCache.Cache.Stats()
	log.Logger().Debug("feedbackCache Stats",
		zap.Int("Size", s.feedbackCache.Cache.Size()),
		zap.Int64("EvictedCost", stats.EvictedCost()),
		zap.Int64("EvictedCount", stats.EvictedCount()),
		zap.Int64("Hits", stats.Hits()),
		zap.Int64("Misses", stats.Misses()),
		zap.Float64("Ratio", stats.Ratio()),
		zap.Int64("RejectedSets", stats.RejectedSets()),
	)

	positiveStats := s.feedbackCache.PositiveCache.Stats()
	log.Logger().Debug("positive feedbackCache Stats",
		zap.Int("Size", s.feedbackCache.PositiveCache.Size()),
		zap.Int64("EvictedCost", positiveStats.EvictedCost()),
		zap.Int64("EvictedCount", positiveStats.EvictedCount()),
		zap.Int64("Hits", positiveStats.Hits()),
		zap.Int64("Misses", positiveStats.Misses()),
		zap.Float64("Ratio", positiveStats.Ratio()),
		zap.Int64("RejectedSets", positiveStats.RejectedSets()),
	)
}

func (s *RestServer) reportCacheSize(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.handleCacheSize()
		}
	}
}

func (s *RestServer) handleCacheSize() {
	start := time.Now()
	itemCacheSize := calculateSize(s.itemCache.Data)

	feedbackCacheSize := calculateSize(s.feedbackCache.Cache)
	positiveFeedbackCacheSize := calculateSize(s.feedbackCache.PositiveCache)

	// 添加内存缓存
	popularItemsSize := calculateSize(s.popularItems)
	latestItemsSize := calculateSize(s.latestItems)
	itemNeighborsSize := calculateSize(s.itemNeighbors)
	userNeighborsSize := calculateSize(s.userNeighbors)

	const bytesToMB = 1024 * 1024 // 1 MB = 1024 * 1024 bytes
	// const bytesToMB = 1 // 1 MB = 1024 * 1024 bytes
	log.Logger().Info("cache size",
		zap.String("total_size_mb", fmt.Sprintf("%.2f MB", float64(itemCacheSize+feedbackCacheSize+positiveFeedbackCacheSize+popularItemsSize+latestItemsSize+itemNeighborsSize+userNeighborsSize)/bytesToMB)),
		zap.Float64("item_cache_size", float64(itemCacheSize)),
		zap.Float64("feedback_cache_size", float64(feedbackCacheSize)),
		zap.Float64("positive_feedback_cache_size", float64(positiveFeedbackCacheSize)),
		zap.Float64("popular_items_size", float64(popularItemsSize)),
		zap.Float64("latest_items_size", float64(latestItemsSize)),
		zap.Float64("item_neighbors_size", float64(itemNeighborsSize)),
		zap.Float64("user_neighbors_size", float64(userNeighborsSize)),
		zap.Duration("duration", time.Since(start)))
}

func calculateSize(v interface{}) uintptr {
	visited := make(map[uintptr]bool)
	return sizeOf(reflect.ValueOf(v), visited)
}

func sizeOf(value reflect.Value, visited map[uintptr]bool) uintptr {
	if !value.IsValid() {
		return 0
	}

	// 先处理 ConcurrentMap
	if value.CanInterface() {
		if m, ok := value.Interface().(cmap.ConcurrentMap); ok {
			var size uintptr
			size += unsafe.Sizeof(m)

			for item := range m.IterBuffered() {
				size += reflect.TypeOf(rune(0)).Size() * uintptr(len(item.Key))
				if feedbacks, ok := item.Val.([]data.Feedback); ok {
					for _, feedback := range feedbacks {
						size += reflect.TypeOf(rune(0)).Size() * uintptr(len(feedback.UserId))
						size += reflect.TypeOf(rune(0)).Size() * uintptr(len(feedback.ItemId))
						size += reflect.TypeOf(rune(0)).Size() * uintptr(len(feedback.FeedbackType))
						size += unsafe.Sizeof(feedback.Timestamp)
						size += unsafe.Sizeof(feedback)
					}
					size += unsafe.Sizeof(feedbacks)
				}
			}
			return size
		}
	}

	switch value.Kind() {
	case reflect.Map:
		if value.IsNil() {
			return 0
		}

		// 添加安全检查
		if !value.CanInterface() {
			return unsafe.Sizeof(value.Interface())
		}
		var size uintptr
		// 计算 map 结构本身的大小
		size += unsafe.Sizeof(value.Interface())

		// 特殊处理 map[string]*data.Item
		if mapType := value.Type(); mapType.Key().Kind() == reflect.String &&
			mapType.Elem().Kind() == reflect.Ptr &&
			mapType.Elem().Elem().String() == "data.Item" {

			iter := value.MapRange()
			for iter.Next() {
				key := iter.Key().String()
				item := iter.Value().Interface().(*data.Item)

				size += reflect.TypeOf(rune(0)).Size() * uintptr(len(key)) // key string
				if item != nil {
					size += reflect.TypeOf(item.ItemId).Size() * uintptr(len(item.ItemId))
					size += reflect.TypeOf(item.Comment).Size() * uintptr(len(item.Comment))
					size += encoding.StringsBytes(item.Categories)
					size += reflect.TypeOf(*item).Size()
				}

				size += reflect.TypeOf(rune(0)).Size() * uintptr(len(key)) // ItemId 内容
				size += unsafe.Sizeof("")                                  // ItemId 字符串结构体

				size += unsafe.Sizeof(item.IsHidden) // bool 类型

				size += unsafe.Sizeof(item.Categories) // 切片结构体
				for _, category := range item.Categories {
					size += reflect.TypeOf(rune(0)).Size() * uintptr(len(category)) // 字符串内容
					size += unsafe.Sizeof("")                                       // 字符串结构体
				}

				size += unsafe.Sizeof(item.Timestamp) // time.Time 类型

				if labels, ok := item.Labels.(map[string]any); ok {
					size += unsafe.Sizeof(labels) // map 结构体
					for k, v := range labels {
						size += reflect.TypeOf(rune(0)).Size() * uintptr(len(k)) // key 内容
						size += unsafe.Sizeof("")                                // key 字符串结构体
						if str, ok := v.(string); ok {
							size += reflect.TypeOf(rune(0)).Size() * uintptr(len(str)) // value 内容
							size += unsafe.Sizeof("")                                  // value 字符串结构体
						}
					}
				}

				size += unsafe.Sizeof(item) // 指针大小

			}
			return size
		}

		// 计算所有键值对的大小
		for _, key := range value.MapKeys() {
			// 计算 key 的大小
			size += sizeOf(key, visited)
			// 计算 value ([]cache.Document) 的大小
			slice := value.MapIndex(key)
			if slice.Len() > 0 {
				// 计算切片头的大小
				size += unsafe.Sizeof(slice.Interface())
				// 计算切片中每个元素的大小
				for i := 0; i < slice.Len(); i++ {
					size += sizeOf(slice.Index(i), visited)
				}
			}
		}
		return size

	case reflect.Struct:
		var size uintptr
		for i := 0; i < value.NumField(); i++ {
			size += sizeOf(value.Field(i), visited)
		}
		return unsafe.Sizeof(value.Interface()) + size

	default:
		return unsafe.Sizeof(value.Interface())
	}
}
