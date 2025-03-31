package broker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ydf0509/gofunboost/core"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

// SqliteBroker 实现Sqlite消息队列
type SqliteBroker struct {
	*BaseBroker
	DB *sql.DB
	mu sync.Mutex
}

func (b *SqliteBroker) createSqliteDB() (*sql.DB, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 从配置中获取sqlite_dir
	sqliteDir, ok := b.BrokerConfig.BrokerTransportOptions["sqlite_dir"].(string)
	if !ok || sqliteDir == "" {
		sqliteDir = "./sqlite_queues"
	}

	// 确保目录存在并设置正确的权限
	err := os.MkdirAll(sqliteDir, 0755)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create directory: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}

	// 创建数据库文件路径
	dbPath := filepath.Join(sqliteDir, fmt.Sprintf("%s.db", b.QueueName))
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not get absolute path: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}
	b.Logger.Info("show dbPath", zap.String("dbPath", absPath))

	// 检查数据库文件权限
	if _, err := os.Stat(absPath); err == nil {
		if err := os.Chmod(absPath, 0644); err != nil {
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not set database file permissions: %v", err), 0, err, b.Logger)
			err2.Log()
			return nil, err2
		}
	}

	// 创建或打开SQLite数据库，添加内存和性能优化参数
	dbURL := fmt.Sprintf("%s", absPath)
	var db *sql.DB
	var retryCount int
	for retryCount < 3 {
		db, err = sql.Open("sqlite", dbURL)
		if err == nil {
			break
		}
		if retryCount == 2 {
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not open SQLite database after retries: %v", err), 0, err, b.Logger)
			err2.Log()
			return nil, err2
		}
		retryCount++
		time.Sleep(time.Second * time.Duration(retryCount))
	}

	// // 设置连接池参数
	// db.SetMaxOpenConns(1)            // 限制最大连接数
	// db.SetMaxIdleConns(1)            // 限制空闲连接数
	// db.SetConnMaxLifetime(time.Hour) // 设置连接最大生命周期

	// 创建消息队列表，使用队列名作为表名
	tableName := b.QueueName
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message TEXT NOT NULL,
			status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'finish')),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create table %s: %v", tableName, err), 0, err, b.Logger)
		err2.Log()
		return nil, err
	}

	b.Sugar.Infof("Connected to SQLite database, created table %s", tableName)
	return db, nil
}

func (b *SqliteBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newSqliteBrokerCustomInit %v", b)
	var err error
	b.BoostOptions.ConnNum = 1
	b.DB, err = b.createSqliteDB()
	if err != nil {
		panic(err)
	}
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (b *SqliteBroker) impConsumeUsingOneConn() error {
	b.Logger.Info("SQLite connection established", zap.String("path", b.BrokerConfig.BrokerUrl), zap.String("queue", b.QueueName))

	ctx := context.Background()
	// 持续消费消息
	for {
		b.mu.Lock()
		// 开始事务
		tx, err := b.DB.BeginTx(ctx, nil)
		if err != nil {
			b.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 从队列中获取并删除最早的消息
		var msgStr string
		var id int64
		err = tx.QueryRowContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET status = 'running'
			WHERE id = (
				SELECT id FROM %s
				WHERE status = 'pending'
				ORDER BY created_at ASC LIMIT 1
			)
			RETURNING id, message
		`, b.QueueName, b.QueueName)).Scan(&id, &msgStr)

		if err == sql.ErrNoRows {
			// 队列为空，回滚事务并等待
			tx.Rollback()
			b.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			tx.Rollback()
			b.mu.Unlock()
			if err.Error() == "database is locked" {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to get message from SQLite: %v", err), 0, err, b.Logger)
			err2.Log()
			return err2
		}

		// 提交事务
		if err = tx.Commit(); err != nil {
			tx.Rollback()
			b.mu.Unlock()
			if err.Error() == "database is locked" {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to commit transaction: %v", err), 0, err, b.Logger)
			err2.Log()
			return err2
		}
		b.mu.Unlock()

		// 反序列化消息
		msg := b.imp.Json2Message(msgStr)

		// 创建MessageWrapper并存储消息ID
		wrapper := &core.MessageWrapper{
			Msg:          msg,
			ContextExtra: make(map[string]interface{}),
		}
		wrapper.ContextExtra["sqlite_msg_id"] = id

		// 使用协程池处理消息
		b.Pool.Submit(func() { b.execute(wrapper) })
	}
}

func (b *SqliteBroker) impSendMsg(msg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 将消息插入队列
	ctx := context.Background()
	_, err := b.DB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (message) VALUES (?)", b.QueueName), msg)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to push message to SQLite queue %s: %v", b.QueueName, err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	return nil
}

func (b *SqliteBroker) impAckMsg(msg *core.MessageWrapper) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 从ContextExtra获取消息ID
	msgID, ok := msg.ContextExtra["sqlite_msg_id"].(int64)
	if !ok {
		err := core.NewBrokerNetworkError("Failed to get message ID from context", 0, nil, b.Logger)
		err.Log()
		return err
	}

	// 将消息状态更新为finish
	ctx := context.Background()
	_, err := b.DB.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET status = 'finish' WHERE id = ?", b.QueueName), msgID)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to acknowledge message in SQLite queue %s: %v", b.QueueName, err), 0, err, b.Logger)
		err2.Log()
		return err2
	}
	return nil
}
