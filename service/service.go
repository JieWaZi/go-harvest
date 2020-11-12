package service

import "database/sql"

type DB interface {
	// DB初始化
	Init() error
	Get() *sql.DB
}

type MQ interface {
	Init() error
	// 将内容存储到消息队列里
	SendDataMessage(key string, data []byte) error
	// 向调度器发送job相关事件
	SendEventMessage(key string, data []byte) error
	Close() error
}
