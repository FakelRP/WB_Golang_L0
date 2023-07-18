package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

// Message представляет структуру сообщения из NATS.
type Message struct {
	OrderUID        string       `json:"order_uid"`
	TrackNumber     string       `json:"track_number"`
	Entry           string       `json:"entry"`
	Delivery        DeliveryInfo `json:"delivery"`
	Payment         PaymentInfo  `json:"payment"`
	Items           []Item       `json:"items"`
	Locale          string       `json:"locale"`
	InternalSig     string       `json:"internal_signature"`
	CustomerID      string       `json:"customer_id"`
	DeliveryService string       `json:"delivery_service"`
	ShardKey        string       `json:"shardkey"`
	SMID            int          `json:"sm_id"`
	DateCreated     time.Time    `json:"date_created"`
	OofShard        string       `json:"oof_shard"`
}

// DeliveryInfo представляет информацию о доставке.
type DeliveryInfo struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

// PaymentInfo представляет информацию о платеже.
type PaymentInfo struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDT    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

// Item представляет информацию о товаре.
type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

// Cache представляет in-memory кеш.
type Cache struct {
	sync.RWMutex
	data map[string]Message
}

// Конфигурация базы данных PostgreSQL.
const (
	dbHost     = "localhost"
	dbPort     = 5432
	dbUser     = "fakel"
	dbPassword = "petre"
	dbName     = "fakel"
)

// Подключение к базе данных PostgreSQL.
func connectToDB() (*sql.DB, error) {
	dbInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", dbInfo)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Подключение к NATS Streaming серверу и подписка на канал.
func subscribeToNATS(channel string, cache *Cache, db *sql.DB) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Восстановление кеша из Postgres при запуске сервиса.
	err = restoreCacheFromDB(cache, db)
	if err != nil {
		log.Println("Failed to restore cache from DB:", err)
	}

	// Обработка полученных сообщений из NATS.
	_, err = nc.Subscribe(channel, func(msg *nats.Msg) {
		var message Message
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			log.Println("Failed to unmarshal message:", err)
			return
		}

		// Сохранение данных в кеше.
		cache.Lock()
		cache.data[message.OrderUID] = message
		cache.Unlock()

		// Сохранение данных в Postgres.
		err = saveMessageToDB(message, db)
		if err != nil {
			log.Println("Failed to save message to DB:", err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Subscribed to channel '%s' in NATS Streaming", channel)

	select {}
}

// Восстановление кеша из базы данных PostgreSQL.
func restoreCacheFromDB(cache *Cache, db *sql.DB) error {
	rows, err := db.Query("SELECT id, data FROM messages")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var data string
		err := rows.Scan(&id, &data)
		if err != nil {
			return err
		}

		var message Message
		err = json.Unmarshal([]byte(data), &message)
		if err != nil {
			return err
		}

		cache.Lock()
		cache.data[strconv.Itoa(id)] = message
		cache.Unlock()
	}

	log.Println("Cache restored from DB")
	return nil
}

// Сохранение сообщения в базе данных PostgreSQL.
func saveMessageToDB(message Message, db *sql.DB) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO messages (id, data) VALUES ($1, $2)", message.OrderUID, string(data))
	if err != nil {
		return err
	}

	return nil
}

// HTTP обработчик для получения данных из кеша по ID.
func getDataFromCacheHandler(cache *Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.URL.Query().Get("id")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			http.Error(w, "Invalid ID", http.StatusBadRequest)
			return
		}

		cache.RLock()
		message, ok := cache.data[strconv.Itoa(id)]
		cache.RUnlock()

		if !ok {
			http.Error(w, "Message not found", http.StatusNotFound)
			return
		}

		jsonData, err := json.Marshal(message)
		if err != nil {
			http.Error(w, "Failed to marshal data", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	}
}

func main() {
	// Создание кеша.
	cache := &Cache{
		data: make(map[string]Message),
	}

	// Подключение к базе данных PostgreSQL.
	db, err := connectToDB()
	if err != nil {
		log.Fatal("Failed to connect to DB:", err)
	}
	defer db.Close()

	// Подключение и подписка на канал в NATS Streaming.
	go subscribeToNATS("NAVNP33RHRZHWLND5OZXIY4ZD7FSBMBP7QROEWFWFT2U3FSQ5XQTJBNS", cache, db)

	// HTTP сервер для получения данных из кеша по ID.
	http.HandleFunc("/data", getDataFromCacheHandler(cache))

	log.Println("Starting HTTP server on port 8080...")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("Failed to start HTTP server:", err)
	}
}
