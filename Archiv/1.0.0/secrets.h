#pragma once

// WLAN-Zugangsdaten
#define WIFI_SSID     "Miau"
#define WIFI_PASSWORD "small3Haradini@#"

// Nuki-Serverpfad für httpNuki()
#define NUKI_SERVERPATH \
  "http://192.168.1.50:8080/lockAction?nukiId=541101474&deviceType=2&action=3&token=wZ80Kp"


// --- MQTT-Zugangsdaten für den lokalen Broker (z.B. HiveMQ) ---
#define MQTT_BROKER "86ee09c6a4f04697be877e3dd1507e0e.s1.eu.hivemq.cloud"
#define MQTT_USER "hiveMQ"
#define MQTT_PASSWORD "8pXDFQ$&n"
#define MQTT_BASE_TOPIC   "3250-Garage"

// --- ThingSpeak MQTT Credentials ---
// HINWEIS: Aktualisiert mit den neu heruntergeladenen Daten.
#define THINGSPEAK_CHANNEL_ID      "636834"
#define THINGSPEAK_CHANNEL_ID_TUER  "670632"
#define THINGSPEAK_MQTT_CLIENT_ID  "JAgMNzUCJz0pOisqPRkzJC0"
#define THINGSPEAK_MQTT_USERNAME   "JAgMNzUCJz0pOisqPRkzJC0"
#define THINGSPEAK_MQTT_PASSWORD   "/pLJD6qwPIa3PmSFHFMByClU"

// ThingSpeak-API-Key
#define THINGSPEAK_API_KEY "R75URRL6UBW2V26W"

// ntfy topic name for notifications
#define NTFY_TOPIC_NAME "Ga4ag3n594s5a57s_3250" // WICHTIG: Wähle deinen eigenen geheimen Namen!