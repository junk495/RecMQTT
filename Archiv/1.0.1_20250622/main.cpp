// =====================================================================================
// LoRaReceiverMQTT (code3)
// -------------------------------------------------------------------------------------
// Letzte Änderung: 22. Juni 2025, 19:35
// Hardware:        AZ-Delivery D1 Mini ESP32
// Funktion:        Ein LoRa-Empfänger (Gateway), der Sensordaten empfängt und an
//                  verschiedene Cloud-Dienste (MQTT, HTTP) weiterleitet.
// Logik:
// - Verlässt sich auf die am Modul voreingestellte LoRa-Konfiguration.
// - Prüft beim Start aktiv, ob das LoRa-Modul verbunden ist.
// - Empfängt LoRa-Datenpakete und erfasst den RSSI-Wert.
// - Speichert den letzten Torstatus im EEPROM.
// - Leitet Daten an HiveMQ und ThingSpeak weiter.
// - Sendet bei Änderung des Torstatus eine Push-Benachrichtigung via ntfy.sh.
// - Zeigt den Betriebszustand über die blaue Onboard-LED an.
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

// ---------------- LoRa-Pins (ANGEPASST AN NEUE HARDWARE) ----------------
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

// Eine Struktur, die den Payload und die Signalstärke (RSSI) bündelt.
struct LoRaMessage {
  LoRaPayload payload;
  int8_t rssi;
};


// ---------------- Globale Variablen ----------------
Ticker statusLedTicker;

// Globale Variable für den LoRa-Status (wird nur im Setup gesetzt)
bool loraOk = false;

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

#define LORA_QUEUE_SIZE 20
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
  char buffer[12];
  snprintf(buffer, sizeof(buffer), "[%02lu:%02lu:%02lu]", hours, minutes, seconds);
  return String(buffer);
}

// ---------------- WLAN-Funktionen ----------------
bool ensureWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return true;
  }
  Serial.println("[WLAN] Verbindung verloren. Versuche Reconnect..." + getTimeStamp());
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int retries = 0;
  while(WiFi.status() != WL_CONNECTED && retries < 20) {
    delay(500);
    Serial.print(".");
    retries++;
  }
  if(WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WLAN] Wieder verbunden!" + getTimeStamp());
    return true;
  }
  Serial.println("\n[WLAN] Reconnect fehlgeschlagen." + getTimeStamp());
  return false;
}

// ---------------- MQTT-Funktionen ----------------
#if USE_HIVEMQ
void hive_mqttReconnect() {
  Serial.print("[MQTT-HIVE] Verbindungsversuch zu HiveMQ Cloud (MQTTS)..." + getTimeStamp());
  String clientId = "LoRaReceiver-Hive-" + String(random(0xffff), HEX);
  if (hive_mqttClient.connect(clientId.c_str(), MQTT_USER, MQTT_PASSWORD)) {
    Serial.println(" verbunden!" + getTimeStamp());
  } else {
    Serial.printf(" fehlgeschlagen, rc=%d. Prüfe MQTT_BROKER, MQTT_USER, MQTT_PASSWORD in secrets.h. %s\n",
                  hive_mqttClient.state(), getTimeStamp().c_str());
  }
}

void publishToHiveMQ(const LoRaMessage& msg) {
  if (!hive_mqttClient.connected()) {
    Serial.println("[MQTT-HIVE] Nicht verbunden, Senden übersprungen." + getTimeStamp());
    return;
  }
  char jsonBuffer[450];
  const LoRaPayload& p = msg.payload;

  if (p.actionID == NUKI_TRIGGER_ID) {
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"id\":%d, \"confidence\":%d, \"voltage\":%.2f, \"rssi\":%d}",
             p.fingerID, p.confidence, p.batteryVoltage, msg.rssi);
    if (hive_mqttClient.publish(hive_mqttTopicDoorEvent, jsonBuffer)) {
      Serial.println("[MQTT-HIVE] Tür-Event gesendet." + getTimeStamp());
    } else {
      Serial.println("[MQTT-HIVE] Fehler beim Senden des Tür-Events." + getTimeStamp());
    }
  } else {
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"temp_in\":%.1f, \"hum_in\":%.1f, \"abs_hum_in\":%.1f, \"temp_out\":%.1f, \"hum_out\":%.1f, \"abs_hum_out\":%.1f, \"fan\":%d, \"door\":%d, \"finger_id\":%d, \"rssi\":%d}",
             p.temperatureInnen, p.humidityInnen, p.absHumidityInnen, p.temperatureAussen, p.humidityAussen, p.absHumidityAussen,
             p.fanOn ? 1 : 0, p.torStatus, (p.actionID == GARAGE_FINGER_ACTION_ID) ? p.fingerID : 0, msg.rssi);
    if (hive_mqttClient.publish(hive_mqttTopicGarageData, jsonBuffer)) {
      Serial.println("[MQTT-HIVE] Garagen-Daten gesendet." + getTimeStamp());
    } else {
      Serial.println("[MQTT-HIVE] Fehler beim Senden der Garagen-Daten." + getTimeStamp());
    }
  }
}
#endif

#if USE_THINGSPEAK
void ts_mqttReconnect() {
  Serial.print("[MQTT-TS] Verbinde mit ThingSpeak..." + getTimeStamp());
  if (ts_mqttClient.connect(THINGSPEAK_MQTT_CLIENT_ID, THINGSPEAK_MQTT_USERNAME, THINGSPEAK_MQTT_PASSWORD)) {
    Serial.println(" verbunden!" + getTimeStamp());
  } else {
    Serial.printf(" fehlgeschlagen, rc=%d. Prüfe THINGSPEAK_MQTT_CLIENT_ID, USERNAME, PASSWORD. %s\n", ts_mqttClient.state(), getTimeStamp().c_str());
  }
}

void publishToThingspeak(const LoRaPayload& p, int tuerStatus) {
  if (!ts_mqttClient.connected()) {
    Serial.println("[MQTT-TS] Nicht verbunden, Senden übersprungen." + getTimeStamp());
    return;
  }
  char payload[256];
  if (p.actionID == NUKI_TRIGGER_ID) {
    snprintf(payload, sizeof(payload), "field1=%d&field2=%d&field3=%.2f", tuerStatus, p.fingerID, p.batteryVoltage);
    Serial.printf("[MQTT-TS] Topic: %s, Payload: %s %s\n", ts_topicTuer, payload, getTimeStamp().c_str());
    if (ts_mqttClient.publish(ts_topicTuer, payload)) {
      Serial.println("[MQTT-TS] Tür-Daten gesendet." + getTimeStamp());
    } else {
      Serial.println("[MQTT-TS] Fehler beim Senden der Tür-Daten." + getTimeStamp());
    }
  } else {
    snprintf(payload, sizeof(payload), "field1=%.2f&field2=%.2f&field3=%.2f&field4=%.2f&field5=%.2f&field6=%.2f&field7=%d&field8=%d",
             p.temperatureInnen, p.humidityInnen, p.absHumidityInnen, p.temperatureAussen, p.humidityAussen, p.absHumidityAussen,
             p.fanOn ? 1 : 0, p.torStatus);
    Serial.printf("[MQTT-TS] Topic: %s, Payload: %s %s\n", ts_topicGarage, payload, getTimeStamp().c_str());
    if (ts_mqttClient.publish(ts_topicGarage, payload)) {
      Serial.println("[MQTT-TS] Garagen-Daten gesendet." + getTimeStamp());
    } else {
      Serial.println("[MQTT-TS] Fehler beim Senden der Garagen-Daten." + getTimeStamp());
    }
  }
  lastThingSpeakPublish = millis();
}
#endif


// ---------------- HTTP-Funktionen ----------------
void sendNtfyNotification(String message) {
  if (!ensureWiFi()) {
    Serial.println("[NTFY] Kein WLAN, Senden der Benachrichtigung übersprungen." + getTimeStamp());
    return;
  }
  HTTPClient http;
  char ntfyUrl[128];
  snprintf(ntfyUrl, sizeof(ntfyUrl), "https://ntfy.sh/%s", NTFY_TOPIC_NAME);
  http.begin(ntfyUrl);
  http.addHeader("Content-Type", "text/plain");
  int httpCode = http.POST(message);
  if (httpCode == 200) {
    Serial.println("[NTFY] Benachrichtigung erfolgreich gesendet." + getTimeStamp());
  } else {
    Serial.printf("[NTFY] Fehler beim Senden der Benachrichtigung, HTTP-Code: %d %s\n", httpCode, getTimeStamp().c_str());
  }
  http.end();
}

void httpNuki() {
  if (!ensureWiFi()) {
    Serial.println("[NUKI] Kein WLAN, Trigger übersprungen." + getTimeStamp());
    return;
  }
  Serial.println("[NUKI] Nuki-Trigger wird ausgelöst..." + getTimeStamp());
  HTTPClient http;
  http.setTimeout(5000);
  http.begin(NUKI_SERVERPATH);
  int httpCode = http.GET();
  if (httpCode > 0) {
    Serial.printf("[NUKI] Anfrage erfolgreich (Code: %d) %s\n", httpCode, getTimeStamp().c_str());
    http.getString();
  } else if (httpCode == HTTPC_ERROR_READ_TIMEOUT) {
    Serial.println("[NUKI] Anfrage gesendet. Server hat nicht geantwortet (Timeout), Aktion wurde wie erwartet ausgeführt." + getTimeStamp());
  } else {
    Serial.printf("[NUKI] Anfrage fehlgeschlagen: %s %s\n", http.errorToString(httpCode).c_str(), getTimeStamp().c_str());
  }
  http.end();
}

// ---------------- LoRa Empfang ----------------
void readAvailableLoraMessages() {
  while (e220ttl.available() >= (int)sizeof(LoRaPayload)) {
    int nextTail = (queueTail + 1) % LORA_QUEUE_SIZE;
    if (nextTail == queueHead) {
      Serial.println("[WARNUNG] LoRa-Queue ist voll!" + getTimeStamp());
      e220ttl.receiveMessage(sizeof(LoRaPayload)).close();
      return;
    }
    ResponseStructContainer rsc = e220ttl.receiveMessageRSSI(sizeof(LoRaPayload));
    if (rsc.status.code == E220_SUCCESS) {
      memcpy(&loraMessageQueue[queueTail].payload, rsc.data, sizeof(LoRaPayload));
      loraMessageQueue[queueTail].rssi = rsc.rssi;
      queueTail = nextTail;
    } else {
      Serial.printf("[ERROR] LoRa Empfangsfehler: %s %s\n", rsc.status.getResponseDescription().c_str(), getTimeStamp().c_str());
    }
    rsc.close();
  }
}

// Funktion zur Steuerung der Status-LED
void updateStatusLed() {
  static bool isBlinking = false;
  static int lastLedState = -1; 
  int currentLedState = 0;

  bool wlanConnected = (WiFi.status() == WL_CONNECTED);
  
  bool allMqttConnected = false;
  #if USE_HIVEMQ && USE_THINGSPEAK
    allMqttConnected = hive_mqttClient.connected() && ts_mqttClient.connected();
  #elif USE_HIVEMQ
    allMqttConnected = hive_mqttClient.connected();
  #elif USE_THINGSPEAK
    allMqttConnected = ts_mqttClient.connected();
  #endif

  if (!wlanConnected) {
    currentLedState = 0; // Zustand 0: Kein WLAN -> LED AUS
  } else if (!loraOk) {
    currentLedState = 1; // Zustand 1: WLAN OK, aber LoRa-Fehler (beim Start) -> BLINKT SCHNELL
  } else if (!allMqttConnected) {
    currentLedState = 2; // Zustand 2: WLAN/LoRa OK, MQTT nicht -> BLINKT LANGSAM
  } else {
    currentLedState = 3; // Zustand 3: Alles OK -> LED DAUERHAFT AN
  }

  if (currentLedState != lastLedState) {
    statusLedTicker.detach();
    isBlinking = false;

    switch (currentLedState) {
      case 0:
        digitalWrite(STATUS_LED_PIN, LOW);
        break;
      case 1:
        statusLedTicker.attach_ms(150, flipStatusLed);
        isBlinking = true;
        break;
      case 2:
        statusLedTicker.attach_ms(750, flipStatusLed);
        isBlinking = true;
        break;
      case 3:
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
  Serial.println("\n[SETUP] LoRaReceiverMQTT startet..." + getTimeStamp());

  pinMode(STATUS_LED_PIN, OUTPUT);
  digitalWrite(STATUS_LED_PIN, LOW);

  EEPROM.begin(EEPROM_SIZE);
  letzterBekannterTorstatus = EEPROM.read(0);
  if (letzterBekannterTorstatus > 2) {
      letzterBekannterTorstatus = 99;
  }
  Serial.printf("[SETUP] Letzter bekannter Torstatus aus EEPROM: %d\n", letzterBekannterTorstatus);

  pinMode(AUX_PIN, INPUT);

  Serial2.begin(9600, SERIAL_8N1, RXD1, TXD1);
  e220ttl.begin();

  // Aktiver Check des LoRa-Moduls nur beim Start
  ResponseStructContainer cfg = e220ttl.getConfiguration();
  if (cfg.status.code == E220_SUCCESS) {
      loraOk = true;
      Serial.println("[LORA] Modul-Check beim Start: OK.");
  } else {
      loraOk = false;
      Serial.println("[LORA-FEHLER] Modul beim Start nicht erreichbar!");
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

  if (ensureWiFi()) {
      Serial.println("[SETUP] WLAN verbunden.");
  } else {
      Serial.println("[SETUP] WLAN-Verbindung fehlgeschlagen.");
  }

  #if USE_HIVEMQ
  if (ensureWiFi()) {
    hive_mqttClient.setServer(MQTT_BROKER, 8883);
    hive_mqttClient.setBufferSize(512);
    hive_espClientSecure.setInsecure();
    hive_espClientSecure.setTimeout(5000);
    hive_mqttReconnect();
  }
  #endif

  #if USE_THINGSPEAK
  if (ensureWiFi()) {
    ts_mqttClient.setServer("mqtt3.thingspeak.com", 1883);
    ts_mqttClient.setBufferSize(512);
    ts_mqttReconnect();
  }
  #endif

  Serial.println("[SETUP] Initialisierung abgeschlossen." + getTimeStamp());
}

void loop() {
  unsigned long now = millis();
  
  updateStatusLed();

  if (e220ttl.available() > 0) {
    readAvailableLoraMessages();
  }

  if (WiFi.status() == WL_CONNECTED) {
      #if USE_HIVEMQ
      if (!hive_mqttClient.connected() && now - lastHiveReconnectAttempt > 10000) {
        lastHiveReconnectAttempt = now;
        hive_mqttReconnect();
      }
      hive_mqttClient.loop();
      #endif
      #if USE_THINGSPEAK
      if (!ts_mqttClient.connected() && now - lastTsReconnectAttempt > 10000) {
        lastTsReconnectAttempt = now;
        ts_mqttReconnect();
      }
      ts_mqttClient.loop();
      #endif
  } else {
      ensureWiFi(); 
  }

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

      sendNtfyNotification("Garagentor ist jetzt " + statusText);
      
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

    if (payloadToProcess.actionID == NUKI_TRIGGER_ID) {
      Serial.printf("[LORA] Finger-Event: MessageID=%u, ID=%d, Conf=%d, ActionID=%d, Vcc=%.2fV, RSSI=%d dBm %s\n",
                      payloadToProcess.messageID, payloadToProcess.fingerID, payloadToProcess.confidence,
                      payloadToProcess.actionID, payloadToProcess.batteryVoltage, messageToProcess.rssi, getTimeStamp().c_str());
      httpNuki();
      #if USE_HIVEMQ
      publishToHiveMQ(messageToProcess);
      #endif
      #if USE_THINGSPEAK
      if (now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
        publishToThingspeak(payloadToProcess, 1);
        lastTuerPayload = payloadToProcess;
        tuerEventTimestamp = now;
        tuerStatusNeedsReset = true;
      } else {
        Serial.println("[MQTT-TS] Finger-Event übersprungen: Zu kurz nach letzter Sendung." + getTimeStamp());
      }
      #endif
    } else {
      if (payloadToProcess.actionID != GARAGE_FINGER_ACTION_ID) {
        Serial.printf("[LoRa] Empfangen: MessageID=%u, Tin=%.1f, RHin=%.1f, Ain=%.1f | Tau=%.1f, RHa=%.1f, Aa=%.1f | Fan=%s, Tor=%d, RSSI=%d dBm %s\n",
                        payloadToProcess.messageID, payloadToProcess.temperatureInnen, payloadToProcess.humidityInnen,
                        payloadToProcess.absHumidityInnen, payloadToProcess.temperatureAussen, payloadToProcess.humidityAussen,
                        payloadToProcess.absHumidityAussen, payloadToProcess.fanOn ? "true" : "false",
                        payloadToProcess.torStatus, messageToProcess.rssi, getTimeStamp().c_str());
      }
      #if USE_HIVEMQ
      publishToHiveMQ(messageToProcess);
      #endif
      #if USE_THINGSPEAK
      if (payloadToProcess.fingerEventValid && payloadToProcess.actionID == GARAGE_FINGER_ACTION_ID) {
        if (now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
          publishToThingspeak(payloadToProcess, 0);
        } else {
          int nextTail = (tsFingerQueueTail + 1) % TS_FINGER_QUEUE_SIZE;
          if (nextTail != tsFingerQueueHead) {
            tsFingerQueue[tsFingerQueueTail] = payloadToProcess;
            tsFingerQueueTail = nextTail;
            Serial.println("[MQTT-TS] Finger-Event in Warteschlange gespeichert." + getTimeStamp());
          } else {
            Serial.println("[MQTT-TS] Finger-Event übersprungen: Warteschlange voll." + getTimeStamp());
          }
        }
      } else if (now - lastThingSpeakPublish >= THINGSPEAK_PUBLISH_INTERVAL) {
        publishToThingspeak(payloadToProcess, 0);
      }
      #endif
    }

    queueHead = (queueHead + 1) % LORA_QUEUE_SIZE;
  }

  #if USE_THINGSPEAK
  if (tsFingerQueueHead != tsFingerQueueTail) {
    Serial.printf("[MQTT-TS] Finger-Event aus Warteschlange gesendet: MessageID=%u, Queue: Head=%d, Tail=%d %s\n",
                  tsFingerQueue[tsFingerQueueHead].messageID, tsFingerQueueHead, tsFingerQueueTail, getTimeStamp().c_str());
    publishToThingspeak(tsFingerQueue[tsFingerQueueHead], 0);
    tsFingerQueueHead = (tsFingerQueueHead + 1) % TS_FINGER_QUEUE_SIZE;
  }
  #endif

  #if USE_THINGSPEAK
  if (tuerStatusNeedsReset && (now - tuerEventTimestamp > 60000)) {
    Serial.println("[TS-TUER] Sende Tuerstatus '0' nach 60 Sekunden." + getTimeStamp());
    if (now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
      publishToThingspeak(lastTuerPayload, 0);
    }
    tuerStatusNeedsReset = false;
  }
  #endif
}
