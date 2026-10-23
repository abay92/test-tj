// Command mock-publisher mengirim data lokasi kendaraan dummy ke broker MQTT
// setiap 2 detik (bisa diubah lewat ENV) untuk keperluan demo / testing.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type locationData struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	value, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback
	}

	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		log.Printf("⚠ ENV %s=%q tidak valid, memakai default %v", key, value, fallback)
		return fallback
	}

	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		log.Printf("⚠ ENV %s=%q tidak valid, memakai default %s", key, value, fallback)
		return fallback
	}

	return parsed
}

// offsetMeters menggeser koordinat sejauh meters ke arah bearing (radian).
func offsetMeters(lat, lng, meters, bearing float64) (float64, float64) {
	const metersPerDegLat = 111320.0

	dLat := meters * math.Cos(bearing) / metersPerDegLat
	dLng := meters * math.Sin(bearing) / (metersPerDegLat * math.Cos(lat*math.Pi/180.0))

	return lat + dLat, lng + dLng
}

func main() {
	broker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	topicTemplate := getEnv("MQTT_TOPIC_TEMPLATE", "/fleet/vehicle/{vehicle_id}/location")
	vehicleIDs := strings.Split(getEnv("VEHICLE_IDS", "B1234XYZ"), ",")
	interval := getEnvDuration("PUBLISH_INTERVAL", 2*time.Second)
	centerLat := getEnvFloat("GEOFENCE_LAT", -6.1753924)
	centerLng := getEnvFloat("GEOFENCE_LNG", 106.8271528)
	radius := getEnvFloat("GEOFENCE_RADIUS", 50.0)

	if !strings.Contains(topicTemplate, "{vehicle_id}") {
		log.Fatalf("⚠ MQTT_TOPIC_TEMPLATE harus mengandung placeholder {vehicle_id}: %q", topicTemplate)
	}

	newClientOpts := mqtt.NewClientOptions()
	newClientOpts.AddBroker(broker)
	newClientOpts.SetClientID("mock-publisher-" + strconv.FormatInt(time.Now().UnixNano(), 36))
	newClientOpts.SetAutoReconnect(true)
	newClientOpts.SetConnectRetry(true)
	newClientOpts.SetConnectRetryInterval(2 * time.Second)
	newClientOpts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("⚠ Koneksi MQTT terputus: %v (reconnect otomatis)", err)
	})

	client := mqtt.NewClient(newClientOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("⚠ Gagal terhubung ke MQTT broker %s: %v", broker, token.Error())
	}
	defer client.Disconnect(250)

	log.Printf("🚚 Mock publisher jalan: broker=%s interval=%s kendaraan=%v", broker, interval, vehicleIDs)
	log.Printf("📍 Pusat geofence: lat=%.7f lng=%.7f radius=%.0fm", centerLat, centerLng, radius)
	log.Printf("📡 Topik: %s", topicTemplate)

	// Jarak (meter) dan arah (derajat) bergiliran supaya kendaraan terlihat
	// bergerak: masuk radius -> keluar radius -> masuk lagi.
	distances := []float64{15, 35, 55, 120}
	bearings := []float64{0, 90, 180, 270}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for tick := 0; ; tick++ {
		<-ticker.C

		for i, vehicleID := range vehicleIDs {
			vehicleID = strings.TrimSpace(vehicleID)
			if vehicleID == "" {
				continue
			}

			distance := distances[(tick+i)%len(distances)]
			bearing := bearings[(tick*2+i)%len(bearings)] * math.Pi / 180.0
			lat, lng := offsetMeters(centerLat, centerLng, distance, bearing)

			payload := locationData{
				VehicleID: vehicleID,
				Latitude:  lat,
				Longitude: lng,
				Timestamp: time.Now().Unix(),
			}

			body, err := json.Marshal(payload)
			if err != nil {
				log.Printf("⚠ Gagal marshal payload: %v", err)
				continue
			}

			topic := strings.ReplaceAll(topicTemplate, "{vehicle_id}", vehicleID)

			token := client.Publish(topic, 0, false, body)
			if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
				log.Printf("⚠ Gagal publish ke %s: %v", topic, token.Error())
				continue
			}

			status := "di luar radius"
			if distance <= radius {
				status = "DI DALAM radius"
			}

			fmt.Printf("📤 [%s] %s | lat=%.7f lng=%.7f | %.0fm dari pusat (%s)\n",
				time.Now().Format("15:04:05"), topic, lat, lng, distance, status)
		}
	}
}
