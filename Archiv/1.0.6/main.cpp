// =====================================================================================
// LoRaReceiverMQTT (code3)
// -------------------------------------------------------------------------------------
// Letzte Änderung: 28. Juni 2025
// Hardware:        ESP32 DevKitC (oder ähnlicher ESP32 mit LoRa-Modul)
// Funktion:        Empfängt LoRa-Daten von GarageLuefterregler (code2),
//                  verarbeitet diese und leitet sie an MQTT-Broker (HiveMQ/ThingSpeak)
//                  weiter. Überwacht zudem den Batteriestatus des Senders (code1C)
//                  und sendet Benachrichtigungen bei geringer Spannung oder Torstatus-Änderung.
// Logik:
// - Baut WLAN-Verbindung auf und hält diese stabil.
// - Initialisiert LoRa-Modul für den Empfang.
// - Empfängt LoRaPayload-Strukturen, die Sensordaten, Torstatus und Fingerabdruck-Events enthalten.
// - Priorisiert Nuki-Trigger-Events für sofortige HTTP-Anfragen.
// - Sendet Garagen- und Event-Daten an konfigurierte MQTT-Dienste.
// - Speichert den letzten Torstatus im EEPROM und sendet ntfy-Benachrichtigungen bei Änderungen.
// - Überwacht die Batteriespannung des LoRa-Senders (code1C) und sendet bei Unterschreitung
//   einer Schwelle eine einmalige ntfy-Warnung.
// - Steuert eine Status-LED zur Anzeige des Verbindungsstatus (WLAN, LoRa, MQTT).
// =====================================================================================

#include <Arduino.h>
#include <WiFi.h>
#include "secrets.h"
#include <LoRa_E220.h>
#include <HTTPClient.h>
#include <PubSubClient.h>
#include <WiFiClientSecure.h>
#include <EEPROM.h>
#include <Ticker.h>

// ======================= STEUERUNG =======================
#define USE_HIVEMQ      true
#define USE_THINGSPEAK  true
// =========================================================

#define EEPROM_SIZE 1
#define STATUS_LED_PIN 2

#define NUKI_TRIGGER_ID 1976
#define GARAGE_FINGER_ACTION_ID 3250

// NEU: Batteriespannung-Warnschwelle für den Empfänger (Code1C-Sender)
#define BATTERY_WARNING_VOLTAGE_SENDER 3.4 // Gleiche Schwelle wie im Sender

// ---------------- LoRa-Pins ----------------
#define RXD1      16
#define TXD1      17
#define AUX_PIN   22
#define M0_PIN    18
#define M1_PIN    19

LoRa_E220 e220ttl(&Serial2, AUX_PIN, M0_PIN, M1_PIN);

// ---------------- Payload-Struktur ----------------
struct __attribute__((packed)) LoRaPayload {
  uint16_t messageID;
  float temperatureInnen, humidityInnen, absHumidityInnen;
  float temperatureAussen, humidityAussen, absHumidityAussen;
  bool  fanOn;
  uint8_t torStatus, fingerID, confidence;
  bool  fingerEventValid;
  uint16_t actionID;
  float batteryVoltage;
};

struct LoRaMessage {
  LoRaPayload payload;
  int8_t rssi;
};

// ---------------- Globale Variablen ----------------
Ticker statusLedTicker;

// Zustandsvariablen für nicht-blockierenden WLAN-Verbindungsaufbau
enum WiFiState { WIFI_IDLE, WIFI_CONNECTING, WIFI_CONNECTED, WIFI_FAILED };
WiFiState wifiState = WIFI_IDLE;
unsigned long wifiConnectStart = 0;
const unsigned long WIFI_CONNECT_TIMEOUT = 10000; // 10 Sekunden Timeout
int wifiRetries = 0;
const int WIFI_MAX_RETRIES = 20; // 20 Versuche à 500 ms = 10 Sekunden

bool loraOk = false;

// NEU: RTC_DATA_ATTR Variable für den Batteriewarnstatus
RTC_DATA_ATTR bool batteryWarningSent = false;


#if USE_HIVEMQ
WiFiClientSecure hive_espClientSecure;
PubSubClient hive_mqttClient(hive_espClientSecure);
char hive_mqttTopicGarageData[128];
char hive_mqttTopicDoorEvent[128];
unsigned long lastHiveReconnectAttempt = 0;
#endif

#if USE_THINGSPEAK
WiFiClient ts_espClient;
PubSubClient ts_mqttClient(ts_espClient);
char ts_topicGarage[128];
char ts_topicTuer[128];
unsigned long lastTsReconnectAttempt = 0;
unsigned long tuerEventTimestamp = 0;
bool tuerStatusNeedsReset = false;
LoRaPayload lastTuerPayload;
unsigned long lastThingSpeakPublish = 0;
const unsigned long THINGSPEAK_PUBLISH_INTERVAL = 300000;
const unsigned long MIN_THINGSPEAK_INTERVAL = 30000;
#define TS_FINGER_QUEUE_SIZE 10
LoRaPayload tsFingerQueue[TS_FINGER_QUEUE_SIZE];
int tsFingerQueueHead = 0;
int tsFingerQueueTail = 0;
#endif

#define LORA_QUEUE_SIZE 50
LoRaMessage loraMessageQueue[LORA_QUEUE_SIZE];
volatile int queueHead = 0;
volatile int queueTail = 0;

uint8_t letzterBekannterTorstatus;

// ---------------- Hilfsfunktionen ----------------
void flipStatusLed() {
  digitalWrite(STATUS_LED_PIN, !digitalRead(STATUS_LED_PIN));
}

String getTimeStamp() {
  unsigned long ms = millis();
  unsigned long seconds = ms / 1000;
  unsigned long minutes = seconds / 60;
  unsigned long hours = minutes / 60;
  seconds %= 60;
  minutes %= 60;
  char buffer[20];
  snprintf(buffer, sizeof(buffer), "[%02lu:%02lu:%02lu]", hours, minutes, seconds);
  return String(buffer);
}

// ---------------- WLAN-Funktionen ----------------
bool ensureWiFiConnected() {
  unsigned long now = millis();

  switch (wifiState) {
    case WIFI_IDLE:
      if (WiFi.status() == WL_CONNECTED) {
        wifiState = WIFI_CONNECTED;
        return true;
      }
      // Verbindung verloren, Reconnect starten
      Serial.println("[WiFi] Connection lost. Starting reconnect..." + getTimeStamp());
      WiFi.disconnect();
      WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
      WiFi.setHostname("LoRaReceiver");
      wifiState = WIFI_CONNECTING;
      wifiConnectStart = now;
      wifiRetries = 0;
      return false;

    case WIFI_CONNECTING:
      if (WiFi.status() == WL_CONNECTED) {
        Serial.println("[WiFi] Reconnected! IP: " + WiFi.localIP().toString() + ", RSSI: " + String(WiFi.RSSI()) + " dBm" + getTimeStamp());
        wifiState = WIFI_CONNECTED;
        return true;
      }
      if (now - wifiConnectStart >= 500) { // Prüfen alle 500 ms
        wifiRetries++;
        Serial.print(".");
        if (wifiRetries >= WIFI_MAX_RETRIES || now - wifiConnectStart >= WIFI_CONNECT_TIMEOUT) {
          Serial.println("\n[WiFi] Reconnect failed after " + String(wifiRetries) + " attempts." + getTimeStamp());
          wifiState = WIFI_FAILED;
          return false;
        }
        wifiConnectStart = now; // Timer zurücksetzen
      }
      return false;

    case WIFI_CONNECTED:
      if (WiFi.status() != WL_CONNECTED) {
        Serial.println("[WiFi] Connection lost during operation." + getTimeStamp());
        wifiState = WIFI_IDLE;
      }
      return true;

    case WIFI_FAILED:
      // Bei anhaltendem Fehler nach 30 Sekunden neustarten
      if (now - wifiConnectStart >= 30000) {
        Serial.println("[WiFi] Critical: No WiFi for 30 seconds. Restarting ESP32..." + getTimeStamp());
        ESP.restart();
      }
      return false;
  }
  return false;
}

bool isWiFiConnected() {
  return wifiState == WIFI_CONNECTED;
}

// ---------------- MQTT-Funktionen ----------------
#if USE_HIVEMQ
void hive_mqttReconnect() {
  if (!isWiFiConnected()) {
    Serial.println("[MQTT-HIVE] No WiFi, skipping reconnect." + getTimeStamp());
    return;
  }
  Serial.print("[MQTT-HIVE] Attempting connection to HiveMQ Cloud (MQTTS)..." + getTimeStamp());
  String clientId = "LoRaReceiver-Hive-" + String(random(0xffff), HEX);
  hive_espClientSecure.stop(); // Alte Verbindungen schließen
  hive_espClientSecure.setTimeout(15000); // Erhöhtes Timeout für TLS
  hive_mqttClient.setKeepAlive(60); // Keep-Alive auf 60 Sekunden
  if (hive_mqttClient.connect(clientId.c_str(), MQTT_USER, MQTT_PASSWORD)) {
    Serial.println(" connected!" + getTimeStamp());
  } else {
    Serial.printf(" failed, rc=%d. Check MQTT_BROKER, MQTT_USER, MQTT_PASSWORD in secrets.h. %s\n",
                  hive_mqttClient.state(), getTimeStamp().c_str());
  }
}

void publishToHiveMQ(const LoRaMessage& msg) {
  if (!hive_mqttClient.connected()) {
    Serial.println("[MQTT-HIVE] Not connected, skipping send." + getTimeStamp());
    return;
  }
  char jsonBuffer[512];
  const LoRaPayload& p = msg.payload;

  if (p.actionID == NUKI_TRIGGER_ID) {
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"id\":%d, \"confidence\":%d, \"voltage\":%.2f, \"rssi\":%d}",
             p.fingerID, p.confidence, p.batteryVoltage, msg.rssi);
    if (hive_mqttClient.publish(hive_mqttTopicDoorEvent, jsonBuffer)) {
      Serial.println("[MQTT-HIVE] Door event published." + getTimeStamp());
    } else {
      Serial.println("[MQTT-HIVE] Failed to publish door event." + getTimeStamp());
    }
  } else {
    // GEÄNDERT: Batteriespannung hinzugefügt
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"temp_in\":%.1f, \"hum_in\":%.1f, \"abs_hum_in\":%.1f, \"temp_out\":%.1f, \"hum_out\":%.1f, \"abs_hum_out\":%.1f, \"fan\":%d, \"door\":%d, \"finger_id\":%d, \"voltage_sender\":%.2f, \"rssi\":%d}",
             p.temperatureInnen, p.humidityInnen, p.absHumidityInnen, p.temperatureAussen, p.humidityAussen, p.absHumidityAussen,
             p.fanOn ? 1 : 0, p.torStatus, (p.actionID == GARAGE_FINGER_ACTION_ID) ? p.fingerID : 0, p.batteryVoltage, msg.rssi);
    if (hive_mqttClient.publish(hive_mqttTopicGarageData, jsonBuffer)) {
      Serial.println("[MQTT-HIVE] Garage data published." + getTimeStamp());
    } else {
      Serial.println("[MQTT-HIVE] Failed to send garage data." + getTimeStamp());
    }
  }
}
#endif

#if USE_THINGSPEAK
void ts_mqttReconnect() {
  if (!isWiFiConnected()) {
    Serial.println("[MQTT-TS] No WiFi, skipping reconnect." + getTimeStamp());
    return;
  }
  Serial.print("[MQTT-TS] Connecting to ThingSpeak..." + getTimeStamp());
  ts_espClient.stop(); // Alte Verbindungen schließen
  ts_mqttClient.setKeepAlive(60); // Keep-Alive auf 60 Sekunden
  if (ts_mqttClient.connect(THINGSPEAK_MQTT_CLIENT_ID, THINGSPEAK_MQTT_USERNAME, THINGSPEAK_MQTT_PASSWORD)) {
    Serial.println(" connected!" + getTimeStamp());
  } else {
    Serial.printf(" failed, rc=%d. Check THINGSPEAK_MQTT_CLIENT_ID, USERNAME, PASSWORD. %s\n",
                  ts_mqttClient.state(), getTimeStamp().c_str());
  }
}

void publishToThingspeak(const LoRaPayload& p, int tuerStatus) {
  if (!ts_mqttClient.connected()) {
    Serial.println("[MQTT-TS] Not connected, skipping send." + getTimeStamp());
    return;
  }
  char payload[256];
  if (p.actionID == NUKI_TRIGGER_ID) {
    snprintf(payload, sizeof(payload), "field1=%d&field2=%d&field3=%.2f", tuerStatus, p.fingerID, p.batteryVoltage);
    Serial.printf("[MQTT-TS] Topic: %s, Payload: %s %s\n", ts_topicTuer, payload, getTimeStamp().c_str());
    if (ts_mqttClient.publish(ts_topicTuer, payload)) {
      Serial.println("[MQTT-TS] Door data sent." + getTimeStamp());
    } else {
      Serial.println("[MQTT-TS] Failed to send door data." + getTimeStamp());
    }
  } else {
    // Unverändert: Batteriespannung nicht für Thingspeak-Garage-Daten
    snprintf(payload, sizeof(payload), "field1=%.2f&field2=%.2f&field3=%.2f&field4=%.2f&field5=%.2f&field6=%.2f&field7=%d&field8=%d",
             p.temperatureInnen, p.humidityInnen, p.absHumidityInnen, p.temperatureAussen, p.humidityAussen, p.absHumidityAussen,
             p.fanOn ? 1 : 0, p.torStatus);
    Serial.printf("[MQTT-TS] Topic: %s, Payload: %s %s\n", ts_topicGarage, payload, getTimeStamp().c_str());
    if (ts_mqttClient.publish(ts_topicGarage, payload)) {
      Serial.println("[MQTT-TS] Garage data sent." + getTimeStamp());
    } else {
      Serial.println("[MQTT-TS] Failed to send garage data." + getTimeStamp());
    }
  }
  lastThingSpeakPublish = millis();
}
#endif

// ---------------- HTTP-Funktionen ----------------
void sendNtfyNotification(const String message) {
  if (!isWiFiConnected()) {
    Serial.println("[NTFY] No WiFi, skipping notification." + getTimeStamp());
    return;
  }
  HTTPClient http;
  char ntfyUrl[128];
  snprintf(ntfyUrl, sizeof(ntfyUrl), "https://ntfy.sh/%s", NTFY_TOPIC_NAME);
  http.begin(ntfyUrl);
  http.addHeader("Content-Type", "text/plain");
  http.setTimeout(15000); // Erhöhtes Timeout
  int httpCode = http.POST(message);
  if (httpCode == 200) {
    Serial.println("[NTFY] Notification sent successfully." + getTimeStamp());
  } else {
    Serial.printf("[NTFY] Failed to send notification, HTTP code: %d %s\n", httpCode, getTimeStamp().c_str());
  }
  http.end();
}

bool httpNuki() {
  bool success = false;
  const int maxRetries = 3;
  for (int attempt = 1; attempt <= maxRetries; attempt++) {
    if (!isWiFiConnected()) {
      Serial.println("[NUKI] No WiFi on attempt " + String(attempt) + ", retrying..." + getTimeStamp());
      delay(1000); // Verzögerung bei fehlendem WLAN
      continue;
    }
    Serial.println("[NUKI] Triggering Nuki (Attempt " + String(attempt) + ")..." + getTimeStamp());
    HTTPClient http;
    http.setTimeout(15000); // Erhöhtes Timeout
    http.begin(NUKI_SERVERPATH);
    int httpCode = http.GET();
    Serial.printf("[DEBUG] HTTP Code: %d, Error: %s %s\n", httpCode, http.errorToString(httpCode).c_str(), getTimeStamp().c_str());
    if (httpCode > 0) {
      String response = http.getString();
      Serial.println("[NUKI] Response: " + response + " " + getTimeStamp());
      success = true;
      break;
    } else {
      Serial.println("[NUKI] Request failed, retrying..." + String(attempt) + getTimeStamp());
      delay(500); // Reduzierte Verzögerung
    }
    http.end();
  }
  if (!success) {
    Serial.println("[NUKI] Failed after " + String(maxRetries) + " attempts." + getTimeStamp());
  }
  return success;
}

// ---------------- LoRa Empfang ----------------
void readAvailableLoraMessages() {
  while (e220ttl.available() >= (int)sizeof(LoRaPayload)) {
    ResponseStructContainer rsc = e220ttl.receiveMessageRSSI(sizeof(LoRaPayload));
    if (rsc.status.code == E220_SUCCESS) {
      LoRaPayload payload;
      memcpy(&payload, rsc.data, sizeof(LoRaPayload));
      
      // NEU: Logik für Batteriespannungswarnung (Code 1C Sender)
      if (payload.batteryVoltage < BATTERY_WARNING_VOLTAGE_SENDER) {
        if (!batteryWarningSent) {
          Serial.printf("[BATTERY_WARN] Akku des Senders unter %.2fV (%.2fV)! Sende ntfy-Meldung... %s\n",
                        BATTERY_WARNING_VOLTAGE_SENDER, payload.batteryVoltage, getTimeStamp().c_str());
          sendNtfyNotification("Sender-Batterie niedrig: " + String(payload.batteryVoltage, 2) + "V!");
          batteryWarningSent = true; // Markiere als gesendet
        }
      } else {
        // Wenn die Spannung wieder über der Warnschwelle ist, zurücksetzen
        batteryWarningSent = false;
      }

      if (payload.actionID == NUKI_TRIGGER_ID) {
        LoRaMessage message;
        message.payload = payload;
        message.rssi = rsc.rssi;
        Serial.printf("[LORA] PRIORITY Finger-Event: MessageID=%u, ID=%d, Conf=%d, ActionID=%d, Vcc=%.2fV, RSSI=%d dBm %s\n",
                      payload.messageID, payload.fingerID, payload.confidence,
                      payload.actionID, payload.batteryVoltage, message.rssi, getTimeStamp().c_str());
        httpNuki();
        #if USE_HIVEMQ
        if (isWiFiConnected()) {
          publishToHiveMQ(message);
        }
        #endif
        #if USE_THINGSPEAK
        if (isWiFiConnected() && millis() - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
          publishToThingspeak(payload, 1);
          lastTuerPayload = payload;
          tuerEventTimestamp = millis();
          tuerStatusNeedsReset = true;
        }
        #endif
      } else {
        int nextTail = (queueTail + 1) % LORA_QUEUE_SIZE;
        if (nextTail == queueHead) {
          Serial.println("[WARN] LoRa queue is full!" + getTimeStamp());
          rsc.close();
          return;
        }
        memcpy(&loraMessageQueue[queueTail].payload, rsc.data, sizeof(LoRaPayload));
        loraMessageQueue[queueTail].rssi = rsc.rssi;
        queueTail = nextTail;
      }
    } else {
      Serial.printf("[ERROR] LoRa receive error: %s %s\n", rsc.status.getResponseDescription().c_str(), getTimeStamp().c_str());
    }
    rsc.close();
  }
}

// ---------------- Status LED Funktion ----------------
void updateStatusLed() {
  static bool isBlinking = false;
  static int lastLedState = -1;
  int currentLedState = 0;

  bool wlanConnected = isWiFiConnected();
  bool allMqttConnected = false;
  #if USE_HIVEMQ && USE_THINGSPEAK
    allMqttConnected = (hive_mqttClient.connected() && ts_mqttClient.connected());
  #elif USE_HIVEMQ
    allMqttConnected = hive_mqttClient.connected();
  #elif USE_THINGSPEAK
    allMqttConnected = ts_mqttClient.connected();
  #endif

  if (!wlanConnected) {
    currentLedState = 0;
  } else if (!loraOk) { // LORA Modul nicht erreichbar
    currentLedState = 1; // Schnelles Blinken
  } else if (!allMqttConnected) {
    currentLedState = 2; // Langsames Blinken
  } else {
    currentLedState = 3; // Dauerhaft an
  }

  if (currentLedState != lastLedState) {
    statusLedTicker.detach();
    isBlinking = false; // Reset blink state

    switch (currentLedState) {
      case 0: // WLAN getrennt (LED aus)
        digitalWrite(STATUS_LED_PIN, LOW);
        break;
      case 1: // LoRa nicht OK (schnelles Blinken)
        statusLedTicker.attach_ms(150, flipStatusLed);
        isBlinking = true;
        break;
      case 2: // MQTT nicht verbunden (langsames Blinken)
        statusLedTicker.attach_ms(750, flipStatusLed);
        isBlinking = true;
        break;
      case 3: // Alles OK (LED an)
        digitalWrite(STATUS_LED_PIN, HIGH);
        break;
    }
    lastLedState = currentLedState;
  }
}


// ---------------- Setup & Loop ----------------
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n[SETUP] LoRaReceiverMQTT starting..." + getTimeStamp());

  pinMode(STATUS_LED_PIN, OUTPUT);
  digitalWrite(STATUS_LED_PIN, LOW);

  EEPROM.begin(EEPROM_SIZE);
  letzterBekannterTorstatus = EEPROM.read(0);
  if (letzterBekannterTorstatus > 2) {
    letzterBekannterTorstatus = 99; // Setze auf ungültigen Status, wenn Wert ungültig
  }
  Serial.printf("[SETUP] Last known door status from EEPROM: %d\n", letzterBekannterTorstatus);

  pinMode(AUX_PIN, INPUT);

  Serial2.begin(9600, SERIAL_8N1, RXD1, TXD1);
  e220ttl.begin();

  ResponseStructContainer cfg = e220ttl.getConfiguration();
  if (cfg.status.code == E220_SUCCESS) {
    loraOk = true;
    Serial.println("[LORA] Module check at startup: OK.");
  } else {
    loraOk = false;
    Serial.println("[LORA-ERROR] Module not reachable at startup!");
  }
  cfg.close();

  #if USE_HIVEMQ
  snprintf(hive_mqttTopicGarageData, sizeof(hive_mqttTopicGarageData), "%s/garage/data", MQTT_BASE_TOPIC);
  snprintf(hive_mqttTopicDoorEvent, sizeof(hive_mqttTopicDoorEvent), "%s/door/event", MQTT_BASE_TOPIC);
  #endif

  #if USE_THINGSPEAK
  snprintf(ts_topicGarage, sizeof(ts_topicGarage), "channels/%s/publish", THINGSPEAK_CHANNEL_ID);
  snprintf(ts_topicTuer, sizeof(ts_topicTuer), "channels/%s/publish", THINGSPEAK_CHANNEL_ID_TUER);
  #endif

  // Aktiver WLAN-Verbindungsaufbau in setup()
  Serial.println("[SETUP] Attempting WiFi connection..." + getTimeStamp());
  WiFi.mode(WIFI_STA); // Nur Station-Modus
  WiFi.setHostname("LoRaReceiver");
  WiFi.config(INADDR_NONE, INADDR_NONE, INADDR_NONE, IPAddress(8, 8, 8, 8)); // Google DNS
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  unsigned long setupConnectStart = millis();
  while (millis() - setupConnectStart < 10000) { // 10 Sekunden Timeout
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("[SETUP] WiFi connected. IP: " + WiFi.localIP().toString() + ", RSSI: " + String(WiFi.RSSI()) + " dBm" + getTimeStamp());
      wifiState = WIFI_CONNECTED;
      break;
    }
    delay(500); // Kurzes Delay, aber LoRa-Empfang bleibt möglich
    readAvailableLoraMessages(); // LoRa-Nachrichten während WLAN-Verbindung prüfen
    Serial.print(".");
  }
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[SETUP] WiFi connection failed." + getTimeStamp());
    wifiState = WIFI_IDLE;
  }

  // Kurze Pause vor MQTT-Initialisierung
  delay(1000);
  readAvailableLoraMessages(); // LoRa-Nachrichten prüfen

  #if USE_HIVEMQ
  if (isWiFiConnected()) {
    hive_mqttClient.setServer(MQTT_BROKER, 8883);
    hive_mqttClient.setBufferSize(512);
    hive_espClientSecure.setInsecure(); // Falls kein Zertifikat verfügbar
    hive_mqttReconnect();
  }
  #endif

  #if USE_THINGSPEAK
  if (isWiFiConnected()) {
    ts_mqttClient.setServer("mqtt3.thingspeak.com", 1883);
    ts_mqttClient.setBufferSize(512);
    ts_mqttReconnect();
  }
  #endif

  Serial.println("[SETUP] Initialization completed." + getTimeStamp());
}

void loop() {
  unsigned long now = millis();

  updateStatusLed();

  // LoRa-Empfang immer ausführen, um Pakete nicht zu verlieren
  if (e220ttl.available() > 0) {
    readAvailableLoraMessages();
  }

  // Nicht-blockierender WLAN-Verbindungsaufbau
  ensureWiFiConnected();

  // MQTT-Reconnects mit größerem Abstand (15 Sekunden)
  if (isWiFiConnected()) {
    #if USE_HIVEMQ
    if (!hive_mqttClient.connected() && now - lastHiveReconnectAttempt > 15000) {
      lastHiveReconnectAttempt = now;
      hive_mqttReconnect();
    }
    hive_mqttClient.loop();
    #endif
    #if USE_THINGSPEAK
    if (!ts_mqttClient.connected() && now - lastTsReconnectAttempt > 15000) {
      lastTsReconnectAttempt = now;
      ts_mqttReconnect();
    }
    ts_mqttClient.loop();
    #endif
  }

  // Verarbeitung der Warteschleife für nicht-Nuki-Pakete
  while (queueHead != queueTail) {
    LoRaMessage messageToProcess = loraMessageQueue[queueHead];
    const LoRaPayload& payloadToProcess = messageToProcess.payload;

    if (payloadToProcess.actionID != NUKI_TRIGGER_ID && payloadToProcess.torStatus != letzterBekannterTorstatus) {
      String statusText = "unbekannt (" + String(payloadToProcess.torStatus) + ")";
      if (payloadToProcess.torStatus == 0) statusText = "geschlossen";
      if (payloadToProcess.torStatus == 1) statusText = "halb offen";
      if (payloadToProcess.torStatus == 2) statusText = "offen";

      Serial.printf("[TOR] Status hat sich von %d zu %d geändert! Sende Benachrichtigung... %s\n",
                    letzterBekannterTorstatus, payloadToProcess.torStatus, getTimeStamp().c_str());

      if (isWiFiConnected()) {
        sendNtfyNotification("Garagentor ist jetzt " + statusText);
      }

      letzterBekannterTorstatus = payloadToProcess.torStatus;
      EEPROM.write(0, letzterBekannterTorstatus);
      EEPROM.commit();
      Serial.printf("[EEPROM] Neuen Torstatus (%d) gespeichert.\n", letzterBekannterTorstatus);
    }

    if (payloadToProcess.fingerEventValid && payloadToProcess.actionID == GARAGE_FINGER_ACTION_ID) {
      Serial.printf("[LORA] Priorisierter Finger-Event: MessageID=%u, ID=%d, Conf=%d, ActionID=%d, RSSI=%d dBm %s\n",
                    payloadToProcess.messageID, payloadToProcess.fingerID, payloadToProcess.confidence,
                    payloadToProcess.actionID, messageToProcess.rssi, getTimeStamp().c_str());
    }

    if (payloadToProcess.actionID != NUKI_TRIGGER_ID) {
      if (payloadToProcess.actionID != GARAGE_FINGER_ACTION_ID) {
        // GEÄNDERT: Batteriespannung hinzugefügt in serieller Ausgabe
        Serial.printf("[LORA] Empfangen: MessageID=%u, Tin=%.1f, RHin=%.1f, Ain=%.1f | Tau=%.1f, RHa=%.1f, Aa=%.1f | Fan=%s, Tor=%d, Vbat=%.2fV, RSSI=%d dBm %s\n",
                      payloadToProcess.messageID, payloadToProcess.temperatureInnen, payloadToProcess.humidityInnen,
                      payloadToProcess.absHumidityInnen, payloadToProcess.temperatureAussen, payloadToProcess.humidityAussen,
                      payloadToProcess.absHumidityAussen, payloadToProcess.fanOn ? "true" : "false",
                      payloadToProcess.torStatus, payloadToProcess.batteryVoltage, messageToProcess.rssi, getTimeStamp().c_str());
      }
      #if USE_HIVEMQ
      if (isWiFiConnected()) {
        publishToHiveMQ(messageToProcess);
      }
      #endif
      #if USE_THINGSPEAK
      if (payloadToProcess.fingerEventValid && payloadToProcess.actionID == GARAGE_FINGER_ACTION_ID) {
        if (isWiFiConnected() && now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
          // Die "publishToThingspeak" Funktion wird hier mit dem Tür-Status 0 (für Garage) aufgerufen,
          // die Batteriespannung ist bereits in den Nuki-Feldern enthalten.
          // Für andere Sensordaten soll sie nicht zu ThingSpeak gesendet werden, wie gewünscht.
          publishToThingspeak(payloadToProcess, 0); 
        } else if (isWiFiConnected()) {
          int nextTail = (tsFingerQueueTail + 1) % TS_FINGER_QUEUE_SIZE;
          if (nextTail != tsFingerQueueHead) {
            tsFingerQueue[tsFingerQueueTail] = payloadToProcess;
            tsFingerQueueTail = nextTail;
            Serial.println("[MQTT-TS] Finger-Event in Warteschlange gespeichert." + getTimeStamp());
          } else {
            Serial.println("[MQTT-TS] Finger-Event übersprungen: Warteschlange voll." + getTimeStamp());
          }
        }
      } else if (isWiFiConnected() && now - lastThingSpeakPublish >= THINGSPEAK_PUBLISH_INTERVAL) {
        publishToThingspeak(payloadToProcess, 0);
      }
      #endif
    }

    queueHead = (queueHead + 1) % LORA_QUEUE_SIZE;
  }

  #if USE_THINGSPEAK
  if (tsFingerQueueHead != tsFingerQueueTail && isWiFiConnected()) {
    Serial.printf("[MQTT-TS] Finger-Event aus Warteschlange gesendet: MessageID=%u, Queue: Head=%d, Tail=%d %s\n",
                  tsFingerQueue[tsFingerQueueHead].messageID, tsFingerQueueHead, tsFingerQueueTail, getTimeStamp().c_str());
    publishToThingspeak(tsFingerQueue[tsFingerQueueHead], 0);
    tsFingerQueueHead = (tsFingerQueueHead + 1) % TS_FINGER_QUEUE_SIZE;
  }

  if (tuerStatusNeedsReset && (now - tuerEventTimestamp > 60000)) {
    Serial.println("[TS-TUER] Sende Tuerstatus '0' nach 60 Sekunden." + getTimeStamp());
    if (isWiFiConnected() && now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
      publishToThingspeak(lastTuerPayload, 0);
    }
    tuerStatusNeedsReset = false;
  }
  #endif
}