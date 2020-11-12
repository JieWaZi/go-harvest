package harvest

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gocolly/colly"
	"github.com/sirupsen/logrus"
	"go_harvest/service"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"
)

const (
	reqContextKey = "colly_request"
	JobRunning    = "running"
	JobError      = "error"
	JobCompleted  = "completed"
)

type ExecutorContext struct {
	executor Executor

	collector    *colly.Collector
	collyContext *colly.Context

	// 一个函数对应一个collector
	collectors map[string]*colly.Collector

	ctlCtx    context.Context
	ctlCancel context.CancelFunc

	// output
	outputDB service.DB
	outputMQ service.MQ

	// output event
	eventWriter EventWriter
}

func (ctx *ExecutorContext) cloneWithCollector(collector *colly.Collector) *ExecutorContext {
	return &ExecutorContext{
		collector:    collector,
		executor:     ctx.executor,
		ctlCtx:       ctx.ctlCtx,
		collectors:   ctx.collectors,
		collyContext: ctx.reqContextClone(),
		ctlCancel:    ctx.ctlCancel,
		outputDB:     ctx.outputDB,
		outputMQ:     ctx.outputMQ,
		eventWriter:  ctx.eventWriter,
	}
}

func (ctx *ExecutorContext) reqContextClone() *colly.Context {
	newCtx := colly.NewContext()
	if ctx.collyContext == nil {
		return newCtx
	}

	ctx.collyContext.ForEach(func(k string, v interface{}) interface{} {
		newCtx.Put(k, v)
		return nil
	})

	return newCtx
}

// GetRequest return the request on this context
func (ctx *ExecutorContext) GetRequest() *Request {
	if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
		return newRequest(req, ctx)
	}
	return nil
}

// PutReqContextValue sets the value for a key
func (ctx *ExecutorContext) PutReqContextValue(key string, value interface{}) {
	if ctx.collyContext == nil {
		if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
			ctx.collyContext = req.Ctx
		} else {
			ctx.collyContext = colly.NewContext()
		}
	}
	ctx.collyContext.Put(key, value)
}

// GetReqContextValue return the string value for a key on ctx
func (ctx *ExecutorContext) GetReqContextValue(key string) string {
	if ctx.collyContext == nil {
		if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
			ctx.collyContext = req.Ctx
		} else {
			return ""
		}
	}
	return ctx.collyContext.Get(key)
}

// GetAnyReqContextValue return the interface value for a key on ctx
func (ctx *ExecutorContext) GetAnyReqContextValue(key string) interface{} {
	if ctx.collyContext == nil {
		if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
			ctx.collyContext = req.Ctx
		} else {
			return nil
		}
	}
	return ctx.collyContext.GetAny(key)
}

func (ctx *ExecutorContext) GetCollector() *colly.Collector {
	return ctx.collector
}

// VisitForNextFunction 下一个function使用get请求访问url
func (ctx *ExecutorContext) VisitForNextFunction(url, functionName string) error {
	defer func() {
		if e := recover(); e != nil {
			ctx.eventWriter.sendEvent(JobError, fmt.Sprintf(", err: %+v, stack:\n%s", e, string(debug.Stack())))
			ctx.ctlCancel()
		}
	}()

	if _, ok := ctx.collectors[functionName]; !ok {
		return ErrFunctionNameNotFind
	}
	newCtx := ctx.cloneWithCollector(ctx.collectors[functionName])

	ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s", functionName))
	err := ctx.executor.execFunction(functionName, newCtx)
	if err != nil {
		ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s err:%s", functionName, err.Error()))
		return err
	}

	return newCtx.collector.Request("GET", ctx.AbsoluteURL(url), nil, ctx.reqContextClone(), nil)
}

// PostForNextFunction 下一个function使用post请求访问url
func (ctx *ExecutorContext) PostForNextFunction(url string, requestData map[string]string, functionName string) error {
	defer func() {
		if e := recover(); e != nil {
			ctx.eventWriter.sendEvent(JobError, fmt.Sprintf(", err: %+v, stack:\n%s", e, string(debug.Stack())))
			ctx.ctlCancel()
		}
	}()

	if _, ok := ctx.collectors[functionName]; !ok {
		return ErrFunctionNameNotFind
	}
	newCtx := ctx.cloneWithCollector(ctx.collectors[functionName])

	ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s", functionName))
	err := ctx.executor.execFunction(functionName, newCtx)
	if err != nil {
		ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s err:%s", functionName, err.Error()))
		return err
	}
	return newCtx.collector.Request("POST", ctx.AbsoluteURL(url), createFormReader(requestData), ctx.reqContextClone(), nil)
}

// PostRowForNextFunction 下一个function使用post请求访问url
func (ctx *ExecutorContext) PostRowForNextFunction(url string, requestData []byte, functionName string) error {
	defer func() {
		if e := recover(); e != nil {
			ctx.eventWriter.sendEvent(JobError, fmt.Sprintf(", err: %+v, stack:\n%s", e, string(debug.Stack())))
			ctx.ctlCancel()
		}
	}()

	if _, ok := ctx.collectors[functionName]; !ok {
		return ErrFunctionNameNotFind
	}
	newCtx := ctx.cloneWithCollector(ctx.collectors[functionName])

	ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s", functionName))
	err := ctx.executor.execFunction(functionName, newCtx)
	if err != nil {
		ctx.eventWriter.sendEvent("running", fmt.Sprintf("exec %s err:%s", functionName, err.Error()))
		return err
	}
	return newCtx.collector.Request("POST", ctx.AbsoluteURL(url), bytes.NewReader(requestData), ctx.reqContextClone(), nil)
}

// OtherForNextFunction 下一个function使用用户自定义访问url
func (ctx *ExecutorContext) CustomForNextFunction(url, functionName, method string, requestData io.Reader, hdr http.Header) error {
	defer func() {
		if e := recover(); e != nil {
			ctx.eventWriter.sendEvent(JobError, fmt.Sprintf(", err: %+v, stack:\n%s", e, string(debug.Stack())))
			ctx.ctlCancel()
		}
	}()

	if _, ok := ctx.collectors[functionName]; !ok {
		return ErrFunctionNameNotFind
	}
	newCtx := ctx.cloneWithCollector(ctx.collectors[functionName])
	err := ctx.executor.execFunction(functionName, newCtx)
	if err != nil {
		return err
	}
	return newCtx.collector.Request(method, url, requestData, ctx.reqContextClone(), hdr)
}

// AbsoluteURL return the absolute URL of u
func (ctx *ExecutorContext) AbsoluteURL(u string) string {
	if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
		return req.AbsoluteURL(u)
	}
	return u
}

// Retry retry current request again
func (ctx ExecutorContext) Retry(count int) error {
	req := ctx.GetRequest()
	key := fmt.Sprintf("err_req_%s", req.URL.String())

	var et int
	if errCount := ctx.GetAnyReqContextValue(key); errCount != nil {
		et = errCount.(int)
		if et >= count {
			return fmt.Errorf("exceed %d counts", count)
		}
	}
	logrus.Infof("errCount:%d, we wil retry url:%s, after 1 second", et+1, req.URL.String())
	time.Sleep(time.Second)
	ctx.PutReqContextValue(key, et+1)
	ctx.retry()

	return nil
}

func (ctx *ExecutorContext) retry() error {
	if req, ok := ctx.ctlCtx.Value(reqContextKey).(*colly.Request); ok {
		return req.Retry()
	}

	return nil
}

func createFormReader(data map[string]string) io.Reader {
	form := url.Values{}
	for k, v := range data {
		form.Add(k, v)
	}
	return strings.NewReader(form.Encode())
}

func newExecutorContext(executor Executor, writer EventWriter) (*ExecutorContext, error) {
	c, err := newCollector(executor.ExecutorConfig)
	if err != nil {
		logrus.Errorf("new collector err:%s", err.Error())
		return nil, err
	}
	funcCount := len(executor.FunctionMap)
	collectors := make(map[string]*colly.Collector, funcCount)
	for funcName, _ := range executor.FunctionMap {
		collectors[funcName] = c.Clone()
	}
	ctxCtl, cancel := context.WithCancel(context.Background())

	return newContext(ctxCtl, cancel, executor, c, collectors, writer), nil
}

func (ctx *ExecutorContext) startExecutor() error {
	var err error
	defer func() {
		if err != nil {
			ctx.eventWriter.sendEvent(JobError, err.Error())
		}
	}()

	ctx.eventWriter.sendEvent(JobRunning, "")
	if ctx.executor.PreFunction != nil {
		err = ctx.executor.PreFunction(ctx.cloneWithCollector(ctx.collector))
		if err != nil {
			return err
		}
	}

	for _, c := range ctx.collectors {
		c.Wait()
	}
	ctx.eventWriter.sendEvent(JobCompleted, "")
	return nil
}

func newContext(ctlCtx context.Context, ctlCancel context.CancelFunc, executor Executor, collector *colly.Collector, collectors map[string]*colly.Collector, writer EventWriter) *ExecutorContext {
	return &ExecutorContext{
		eventWriter: writer,
		executor:    executor,
		collectors:  collectors,
		collector:   collector,
		ctlCtx:      ctlCtx,
		ctlCancel:   ctlCancel,
	}
}
