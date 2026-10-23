package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Helper function untuk mengambil ENV dengan nilai default (fallback)
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// Helper function untuk mengambil ENV bertipe angka dengan nilai default.
func getEnvFloat(key string, fallback float64) float64 {
	value, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback
	}

	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		log.Printf("\u26A0 ENV %s=%q tidak valid, memakai default %v: %v", key, value, fallback, err)
		return fallback
	}

	return parsed
}

// Geofence adalah area melingkar di sekitar satu titik pusat.
// Default: Monas Jakarta dengan radius 50 meter.
type Geofence struct {
	Lat    float64
	Lng    float64
	Radius float64 // meter
}

// loadGeofence membaca konfigurasi geofence dari ENV.
func loadGeofence() Geofence {
	return Geofence{
		Lat:    getEnvFloat("GEOFENCE_LAT", -6.1753924),
		Lng:    getEnvFloat("GEOFENCE_LNG", 106.8271528),
		Radius: getEnvFloat("GEOFENCE_RADIUS", 50.0),
	}
}

// Contains melaporkan apakah sebuah koordinat berada di dalam radius geofence.
func (g Geofence) Contains(lat, lng float64) bool {
	return calculateDistance(lat, lng, g.Lat, g.Lng) <= g.Radius
}

// Struktur data untuk menerima payload JSON dari mobil
type LocationData struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

type LocationPayload struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type GeofenceEvent struct {
	VehicleID string          `json:"vehicle_id"`
	Event     string          `json:"event"`
	Location  LocationPayload `json:"location"`
	Timestamp int64           `json:"timestamp"`
}

// Rumus Haversine untuk menghitung jarak 2 titik koordinat (dalam meter)
func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // Radius bumi dalam meter
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0

	rLat1 := lat1 * math.Pi / 180.0
	rLat2 := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(rLat1)*math.Cos(rLat2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

// rabbitPublisher membungkus koneksi RabbitMQ agar otomatis reconnect ketika
// broker restart / koneksi putus. Dipakai bersama oleh handler MQTT dan
// endpoint /health, sehingga aksesnya dilindungi mutex.
type rabbitPublisher struct {
	url string

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newRabbitPublisher(url string) *rabbitPublisher {
	return &rabbitPublisher{url: url}
}

// declareFleetTopology mendeklarasikan exchange, queue, dan binding.
// Idempotent, jadi aman dipanggil ulang setiap kali reconnect.
func declareFleetTopology(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		"fleet.events", // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return fmt.Errorf("declare exchange fleet.events: %w", err)
	}

	queue, err := ch.QueueDeclare(
		"geofence_alerts", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("declare queue geofence_alerts: %w", err)
	}

	if err := ch.QueueBind(queue.Name, "geofence_alerts", "fleet.events", false, nil); err != nil {
		return fmt.Errorf("bind queue geofence_alerts: %w", err)
	}

	return nil
}

// ensureLocked memastikan koneksi & channel siap dipakai, membuka yang baru
// bila belum ada atau sudah tertutup. Pemanggil wajib memegang p.mu.
func (p *rabbitPublisher) ensureLocked() error {
	if p.conn != nil && !p.conn.IsClosed() && p.ch != nil && !p.ch.IsClosed() {
		return nil
	}
	p.dropLocked()

	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 10 * time.Second, // deteksi koneksi mati tanpa menunggu TCP timeout
		Dial:      amqp.DefaultDial(3 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}

	if err := declareFleetTopology(ch); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	p.conn, p.ch = conn, ch

	// Saat koneksi ditutup (broker restart / network drop), kosongkan state
	// supaya publish atau health check berikutnya memicu reconnect.
	go func(c *amqp.Connection) {
		closeErr := <-c.NotifyClose(make(chan *amqp.Error, 1))

		p.mu.Lock()
		if p.conn == c {
			p.conn, p.ch = nil, nil
		}
		p.mu.Unlock()

		if closeErr != nil {
			fmt.Printf("\u26A0 Koneksi RabbitMQ terputus: %v (reconnect otomatis)\n", closeErr)
		}
	}(conn)

	return nil
}

// dropLocked menutup koneksi aktif tanpa memegang state baru.
func (p *rabbitPublisher) dropLocked() {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.ch, p.conn = nil, nil
}

// Connect membuka koneksi saat startup dengan retry terbatas.
func (p *rabbitPublisher) Connect(attempts int, interval time.Duration) error {
	var lastErr error

	for i := 1; i <= attempts; i++ {
		p.mu.Lock()
		lastErr = p.ensureLocked()
		p.mu.Unlock()

		if lastErr == nil {
			return nil
		}
		if i < attempts {
			fmt.Printf("\u23F3 RabbitMQ belum siap (%v), coba lagi %s [%d/%d]\n", lastErr, interval, i, attempts)
			time.Sleep(interval)
		}
	}

	return lastErr
}

// Publish mengirim body ke RabbitMQ. Kalau channel/koneksi ternyata sudah mati,
// koneksi dibuka ulang lalu publish dicoba sekali lagi.
func (p *rabbitPublisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	send := func() error {
		return p.ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // message bertahan walau broker restart
			Body:         body,
		})
	}

	if err := p.ensureLocked(); err != nil {
		return err
	}

	err := send()
	if err == nil {
		return nil
	}

	// Bisa jadi channel sudah ditutup broker tanpa sempat terdeteksi NotifyClose
	p.dropLocked()
	if rerr := p.ensureLocked(); rerr != nil {
		return fmt.Errorf("publish gagal (%v) dan reconnect gagal: %w", err, rerr)
	}

	return send()
}

// Health memastikan koneksi RabbitMQ siap. Bila sedang putus, health check
// sekaligus memicu reconnect sehingga status pulih sendiri.
func (p *rabbitPublisher) Health() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.ensureLocked()
}

// Close menutup koneksi RabbitMQ.
func (p *rabbitPublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dropLocked()
}

// publishGeofence mengirimkan payload event geofence ke RabbitMQ.
func (p *rabbitPublisher) publishGeofence(data LocationData, distance, radius float64) error {
	eventPayload := GeofenceEvent{
		VehicleID: data.VehicleID,
		Event:     "geofence_entry",
		Location: LocationPayload{
			Latitude:  data.Latitude,
			Longitude: data.Longitude,
		},
		Timestamp: data.Timestamp,
	}

	body, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("marshal geofence event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Publish(ctx, "fleet.events", "geofence_alerts", body); err != nil {
		return err
	}

	fmt.Printf("\U0001F6A8 [GEOFENCE ALERT PUBLISHED] Vehicle ID: %s | Jarak: %.1fm (radius %.0fm)\n",
		data.VehicleID, distance, radius)

	return nil
}

func main() {
	/** connect Database */
	host := getEnv("DB_HOST", "localhost")
	portStr := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "root")
	password := getEnv("DB_PASSWORD", "root123")
	dbname := getEnv("DB_NAME", "test-tj")
	tcpBroker := getEnv("TCP_BROKER", "tcp://localhost:1883")
	rabbitmqURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	appPort := getEnv("APP_PORT", "8080")
	geo := loadGeofence()
	fmt.Printf("\U0001F3AF Geofence aktif: lat=%.7f lng=%.7f radius=%.0fm\n", geo.Lat, geo.Lng, geo.Radius)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("\u26A0 Port database tidak valid: %v", err)
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)

	if err != nil {
		fmt.Printf("Error database %s", err)
		return
	}
	defer db.Close()
	/** end connect Database */

	/** connect RabbitMQ */
	publisher := newRabbitPublisher(rabbitmqURL)
	if err := publisher.Connect(30, 2*time.Second); err != nil {
		log.Fatalf("\u26A0 Gagal terhubung ke RabbitMQ: %v", err)
	}
	defer publisher.Close()

	fmt.Println("\u2705 RabbitMQ Exchange & Queue siap digunakan!")

	// Setup Gin REST API Server (Asynchronous di Goroutine)
	gin.SetMode(gin.ReleaseMode) // Set ke ReleaseMode agar log lebih bersih
	r := gin.Default()

	// Client MQTT dibuat sebelum HTTP server berjalan supaya endpoint /health
	// bisa memeriksa status koneksinya.
	var mqttClient mqtt.Client

	/** Rest API Gin */
	// Endpoint 1: GET /vehicles/:vehicle_id/location
	r.GET("/vehicles/:vehicle_id/location", func(c *gin.Context) {
		vehicleID := c.Param("vehicle_id")

		query := `
			SELECT vehicle_id, latitude, longitude, timestamp 
			FROM vehicles 
			WHERE vehicle_id = $1 
			ORDER BY timestamp DESC 
			LIMIT 1`

		var data LocationData
		err := db.QueryRow(query, vehicleID).Scan(&data.VehicleID, &data.Latitude, &data.Longitude, &data.Timestamp)
		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{
					"message": fmt.Sprintf("Data lokasi untuk vehicle_id '%s' tidak ditemukan", vehicleID),
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Gagal mengambil data dari database: %v", err),
			})
			return
		}

		c.JSON(http.StatusOK, data)
	})

	// Endpoint 2: GET /vehicles/:vehicle_id/history?start=1715000000&end=1715009999 (Riwayat Lokasi)
	r.GET("/vehicles/:vehicle_id/history", func(c *gin.Context) {
		vehicleID := c.Param("vehicle_id")
		startStr := c.Query("start")
		endStr := c.Query("end")

		// Validasi query parameter wajib ada
		if startStr == "" || endStr == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Query parameter 'start' dan 'end' wajib diisi (contoh: /history?start=1715000000&end=1715009999)",
			})
			return
		}

		// Konversi start dan end ke tipe int64
		start, errStart := strconv.ParseInt(startStr, 10, 64)
		end, errEnd := strconv.ParseInt(endStr, 10, 64)

		if errStart != nil || errEnd != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Query parameter 'start' dan 'end' harus berupa format unix timestamp (angka)",
			})
			return
		}

		// Query mengambil histori lokasi berdasarkan rentang waktu timestamp
		query := `
			SELECT vehicle_id, latitude, longitude, timestamp 
			FROM vehicles 
			WHERE vehicle_id = $1 AND timestamp >= $2 AND timestamp <= $3 
			ORDER BY timestamp ASC`

		rows, err := db.Query(query, vehicleID, start, end)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Gagal mengambil histori dari database: %v", err),
			})
			return
		}
		defer rows.Close()

		history := []LocationData{}
		for rows.Next() {
			var loc LocationData
			if err := rows.Scan(&loc.VehicleID, &loc.Latitude, &loc.Longitude, &loc.Timestamp); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": fmt.Sprintf("Gagal membaca row data: %v", err),
				})
				return
			}
			history = append(history, loc)
		}

		c.JSON(http.StatusOK, gin.H{
			"vehicle_id": vehicleID,
			"total":      len(history),
			"data":       history,
		})
	})

	// Endpoint 3: GET /health (dipakai HEALTHCHECK Docker / readiness probe)
	// Cek PostgreSQL, RabbitMQ, dan koneksi MQTT sekaligus.
	r.GET("/health", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
		defer cancel()

		checks := gin.H{}
		healthy := true

		if err := db.PingContext(ctx); err != nil {
			checks["database"] = err.Error()
			healthy = false
		} else {
			checks["database"] = "ok"
		}

		// Bila koneksi RabbitMQ putus, Health() sekaligus memicu reconnect
		if err := publisher.Health(); err != nil {
			checks["rabbitmq"] = err.Error()
			healthy = false
		} else {
			checks["rabbitmq"] = "ok"
		}

		if mqttClient == nil || !mqttClient.IsConnectionOpen() {
			checks["mqtt"] = "not connected"
			healthy = false
		} else {
			checks["mqtt"] = "ok"
		}

		if !healthy {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "unhealthy",
				"checks": checks,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"checks": checks,
		})
	})

	// Topik yang didengarkan. Dua format didukung karena MQTT menganggap
	// "/fleet/..." (leading slash) dan "fleet/..." sebagai topik berbeda.
	topicPatterns := map[string]byte{
		"fleet/vehicle/+/location":  1,
		"/fleet/vehicle/+/location": 1,
	}
	topicList := "fleet/vehicle/+/location, /fleet/vehicle/+/location"

	// Handler untuk setiap pesan lokasi yang masuk dari kendaraan
	handleLocation := func(c mqtt.Client, msg mqtt.Message) {
		// msg.Topic() akan berisi topik asli, misal: "fleet/vehicle/mobil-abc/location"
		incomingTopic := msg.Topic()

		// Ekstraksi ID Mobil: segmen tepat sebelum "location".
		// Berlaku untuk kedua format topik (dengan / tanpa leading slash).
		parts := strings.Split(incomingTopic, "/")
		var topicVehicleID string
		if len(parts) >= 2 {
			topicVehicleID = parts[len(parts)-2] // Mengambil "B1234XYZ"
		}

		// Parsing payload JSON yang dikirim oleh mobil
		var data LocationData
		err := json.Unmarshal(msg.Payload(), &data)
		if err != nil {
			fmt.Printf("\u26A0 Gagal membaca data dari %s: %v\n", data.VehicleID, err)
			return
		}

		if topicVehicleID != data.VehicleID {
			fmt.Printf("\u274C [VALIDASI GAGAL] Data ditolak! ID di topik (%s) tidak sama dengan ID di JSON (%s).\n",
				topicVehicleID, data.VehicleID)
			return // Keluar dari fungsi (proses dihentikan/gagal)
		}

		queryInsert := `INSERT INTO vehicles (vehicle_id, latitude, longitude, timestamp) VALUES ($1, $2, $3, $4)`

		// PostgreSQL menggunakan placeholder $1, $2, $3, $4 (bukan ? seperti MySQL)
		_, err = db.Exec(queryInsert, data.VehicleID, data.Latitude, data.Longitude, data.Timestamp)
		if err != nil {
			fmt.Printf("\u26A0 Gagal insert data: %v\n", err)
			return
		}
		fmt.Println("\u2705 Data vehicle berhasil disimpan!")

		// Tampilkan hasil ke konsol/terminal server
		tm := time.Unix(data.Timestamp, 0)
		fmt.Printf("\U0001F697 [LOG LOKASI] Vehicle ID: %s | Lat: %f, Lng: %f | Timestamp: %s\n",
			data.VehicleID, data.Latitude, data.Longitude, tm.String())

		// Hitung Jarak Geofence
		distance := calculateDistance(data.Latitude, data.Longitude, geo.Lat, geo.Lng)
		if distance <= geo.Radius {
			if err := publisher.publishGeofence(data, distance, geo.Radius); err != nil {
				fmt.Printf("\u26A0 Gagal publish geofence alert: %v\n", err)
			}
		}
	}

	// Konfigurasi MQTT Client
	firstSubscribe := make(chan error, 1)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(tcpBroker)
	opts.SetClientID("monitor_managemen_armada")
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true) // reconnect otomatis bila broker MQTT restart

	// OnConnectHandler dipanggil saat connect awal dan setiap reconnect,
	// sehingga subscription otomatis dipulihkan setelah broker MQTT restart.
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		token := c.SubscribeMultiple(topicPatterns, handleLocation)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("\u274C Gagal subscribe ke topik: %v\n", token.Error())
			select {
			case firstSubscribe <- token.Error():
			default:
			}
			return
		}

		fmt.Printf("\U0001F4E1 Menunggu data masuk dari topik: %s...\n", topicList)
		select {
		case firstSubscribe <- nil:
		default:
		}
	})

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		fmt.Printf("\u26A0 Koneksi MQTT terputus: %v (reconnect otomatis)\n", err)
	})

	mqttClient = mqtt.NewClient(opts)

	go func() {
		fmt.Printf("\U0001F310 HTTP Server (Gin) berjalan di port :%s...\n", appPort)
		if err := r.Run(":" + appPort); err != nil {
			log.Fatalf("\u26A0 Gagal menjalankan Gin Server: %v", err)
		}
	}()
	/** end Rest API Gin */

	// Hubungkan MQTT client ke broker
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("\U0001F680 Server Golang berhasil terhubung ke Mosquitto Broker!")

	// Tunggu subscription awal selesai (dijalankan oleh OnConnectHandler)
	select {
	case err := <-firstSubscribe:
		if err != nil {
			os.Exit(1)
		}
	case <-time.After(10 * time.Second):
		log.Fatalf("\u26A0 Timeout subscribe ke topik MQTT: %s", topicList)
	}

	// Menjaga program tetap berjalan sampai ditekan Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Putuskan koneksi saat program dimatikan
	mqttClient.Disconnect(250)
	fmt.Println("\U0001F44B Server berhenti.")
}
