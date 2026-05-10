// =====================================================================================
// LoRaReceiverMQTT (code3)
// -------------------------------------------------------------------------------------
// Letzte Änderung: 21. Juni 2025, 10:16
// Hardware:        AZ-Delivery D1 Mini ESP32
// Funktion:        Ein LoRa-Empfänger (Gateway), der Sensordaten empfängt und an
//                  verschiedene Cloud-Dienste (MQTT, HTTP) weiterleitet.
// Logik:
// - Empfängt LoRa-Datenpakete (`LoRaPayload`) von verschiedenen Sensoren.
// - Leitet die Daten parallel an zwei MQTT-Dienste weiter: HiveMQ (MQTTS) und
//   ThingSpeak (MQTT).
// - Sendet Daten an ThingSpeak mit einem Intervall (5 Min.) und einer Warteschlange
//   für priorisierte Ereignisse (z.B. Finger-Events).
// - Löst bei Empfang einer speziellen Nuki-Trigger-ID eine HTTP-Aktion aus.
// - Sendet bei Änderung des Torstatus eine Push-Benachrichtigung via ntfy.sh.
// =====================================================================================

#include <Arduino.h>
#include <WiFi.h>
#include "secrets.h"
#include <LoRa_E220.h>
#include <HTTPClient.h>
#include <PubSubClient.h>
#include <WiFiClientSecure.h>

// ======================= STEUERUNG =======================
#define USE_HIVEMQ      true
#define USE_THINGSPEAK  true
// =========================================================

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

// ---------------- Globale Variablen ----------------
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

// Nachrichten-Warteschlange
#define LORA_QUEUE_SIZE 20
LoRaPayload loraMessageQueue[LORA_QUEUE_SIZE];
volatile int queueHead = 0;
volatile int queueTail = 0;

// Globale Variable, um den letzten Torstatus zu speichern
uint8_t letzterBekannterTorstatus = 99; // Initialisiert mit einem unmöglichen Wert

// ---------------- Hilfsfunktion für Zeitsignatur ----------------
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
  if (WiFi.status() == WL_CONNECTED) return true;
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

void publishToHiveMQ(const LoRaPayload& p) {
  if (!hive_mqttClient.connected()) {
    Serial.println("[MQTT-HIVE] Nicht verbunden, Senden übersprungen." + getTimeStamp());
    return;
  }
  char jsonBuffer[400];
  if (p.actionID == NUKI_TRIGGER_ID) {
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"id\":%d, \"confidence\":%d, \"voltage\":%.2f}", p.fingerID, p.confidence, p.batteryVoltage);
    if (hive_mqttClient.publish(hive_mqttTopicDoorEvent, jsonBuffer)) {
      Serial.println("[MQTT-HIVE] Tür-Event gesendet." + getTimeStamp());
    } else {
      Serial.println("[MQTT-HIVE] Fehler beim Senden des Tür-Events." + getTimeStamp());
    }
  } else {
    snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"temp_in\":%.1f, \"hum_in\":%.1f, \"abs_hum_in\":%.1f, \"temp_out\":%.1f, \"hum_out\":%.1f, \"abs_hum_out\":%.1f, \"fan\":%d, \"door\":%d, \"finger_id\":%d}",
             p.temperatureInnen, p.humidityInnen, p.absHumidityInnen, p.temperatureAussen, p.humidityAussen, p.absHumidityAussen,
             p.fanOn ? 1 : 0, p.torStatus, (p.actionID == GARAGE_FINGER_ACTION_ID) ? p.fingerID : 0);
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
  lastThingSpeakPublish = millis(); // Aktualisiere letzte Sendung
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
    ResponseStructContainer rsc = e220ttl.receiveMessage(sizeof(LoRaPayload));
    if (rsc.status.code == E220_SUCCESS) {
      memcpy(&loraMessageQueue[queueTail], rsc.data, sizeof(LoRaPayload));
      queueTail = nextTail;
    } else {
      Serial.printf("[ERROR] LoRa Empfangsfehler: %s %s\n", rsc.status.getResponseDescription().c_str(), getTimeStamp().c_str());
    }
    rsc.close();
  }
}

// ---------------- Setup & Loop ----------------
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n[SETUP] LoRaReceiverMQTT startet..." + getTimeStamp());
  
  pinMode(AUX_PIN, INPUT);

  Serial2.begin(9600, SERIAL_8N1, RXD1, TXD1);
  e220ttl.begin();

  ResponseStructContainer cfgContainer = e220ttl.getConfiguration();
  if (cfgContainer.status.code == E220_SUCCESS) {
    Configuration cfg = *(Configuration*)cfgContainer.data;
    cfg.ADDH = 0;
    cfg.ADDL = 2;
    cfg.CHAN = 23;
    auto status = e220ttl.setConfiguration(cfg, WRITE_CFG_PWR_DWN_SAVE);
    Serial.printf("[LoRa] Modul konfiguriert, Status: %s %s\n", status.getResponseDescription().c_str(), getTimeStamp().c_str());
    cfgContainer.close();
  } else {
    Serial.printf("[LoRa] Konfig lesen fehlgeschlagen: %s %s\n", cfgContainer.status.getResponseDescription().c_str(), getTimeStamp().c_str());
    cfgContainer.close();
    while (1);
  }

  #if USE_HIVEMQ
  snprintf(hive_mqttTopicGarageData, sizeof(hive_mqttTopicGarageData), "%s/garage/data", MQTT_BASE_TOPIC);
  snprintf(hive_mqttTopicDoorEvent, sizeof(hive_mqttTopicDoorEvent), "%s/door/event", MQTT_BASE_TOPIC);
  Serial.printf("[MQTT-HIVE] Garagen-Topic: %s %s\n", hive_mqttTopicGarageData, getTimeStamp().c_str());
  Serial.printf("[MQTT-HIVE] Tür-Topic: %s %s\n", hive_mqttTopicDoorEvent, getTimeStamp().c_str());
  #endif

  #if USE_THINGSPEAK
  snprintf(ts_topicGarage, sizeof(ts_topicGarage), "channels/%s/publish", THINGSPEAK_CHANNEL_ID);
  snprintf(ts_topicTuer, sizeof(ts_topicTuer), "channels/%s/publish", THINGSPEAK_CHANNEL_ID_TUER);
  Serial.printf("[MQTT-TS] Garagen-Topic: %s %s\n", ts_topicGarage, getTimeStamp().c_str());
  Serial.printf("[MQTT-TS] Tür-Topic: %s %s\n", ts_topicTuer, getTimeStamp().c_str());
  #endif

  #if USE_HIVEMQ || USE_THINGSPEAK
  WiFi.config(INADDR_NONE, INADDR_NONE, INADDR_NONE, IPAddress(8, 8, 8, 8)); // Google DNS
  ensureWiFi();
  #endif
  
  #if USE_HIVEMQ
  hive_mqttClient.setBufferSize(512);
  hive_espClientSecure.setInsecure();
  hive_espClientSecure.setTimeout(5000);
  hive_mqttClient.setServer(MQTT_BROKER, 8883);
  if (ensureWiFi()) {
    hive_mqttReconnect();
  }
  #endif
  
  #if USE_THINGSPEAK
  ts_mqttClient.setBufferSize(512);
  ts_mqttClient.setServer("mqtt3.thingspeak.com", 1883);
  if (ensureWiFi()) {
    ts_mqttReconnect();
  }
  #endif
  
  Serial.println("[SETUP] Initialisierung abgeschlossen." + getTimeStamp());
}

void loop() {
  // Priorisiere LoRa-Verarbeitung
  if (e220ttl.available() > 0) {
    readAvailableLoraMessages();
  }

  unsigned long now = millis();

  // Verarbeite Warteschlange sofort
  ensureWiFi();
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

  // Verarbeite LoRa-Warteschlange
  while (queueHead != queueTail) {
    LoRaPayload payloadToProcess = loraMessageQueue[queueHead];
    
    // Logik zur Erkennung der Torstatus-Änderung
    if (payloadToProcess.actionID != NUKI_TRIGGER_ID && payloadToProcess.torStatus != letzterBekannterTorstatus) {
      // GEÄNDERT: Logik zur Übersetzung des Status in Text
      String statusText = "unbekannt (" + String(payloadToProcess.torStatus) + ")";
      if (payloadToProcess.torStatus == 0) statusText = "geschlossen";
      if (payloadToProcess.torStatus == 1) statusText = "halb offen";
      if (payloadToProcess.torStatus == 2) statusText = "offen";

      Serial.printf("[TOR] Status hat sich von %d zu %d geändert! Sende Benachrichtigung... %s\n",
                    letzterBekannterTorstatus, payloadToProcess.torStatus, getTimeStamp().c_str());
      
      sendNtfyNotification("Garagentor ist jetzt " + statusText);

      // Wichtig: Den neuen Status als den "letzten bekannten" speichern
      letzterBekannterTorstatus = payloadToProcess.torStatus;
    }
    
    if (payloadToProcess.fingerEventValid && payloadToProcess.actionID == GARAGE_FINGER_ACTION_ID) {
      Serial.printf("[LORA] Priorisierter Finger-Event: MessageID=%u, ID=%d, Conf=%d, ActionID=%d %s\n",
                      payloadToProcess.messageID, payloadToProcess.fingerID, payloadToProcess.confidence,
                      payloadToProcess.actionID, getTimeStamp().c_str());
    }
    
    if (payloadToProcess.actionID == NUKI_TRIGGER_ID) {
      Serial.printf("[LORA] Finger-Event: MessageID=%u, ID=%d, Conf=%d, ActionID=%d, Vcc=%.2fV %s\n",
                      payloadToProcess.messageID, payloadToProcess.fingerID, payloadToProcess.confidence,
                      payloadToProcess.actionID, payloadToProcess.batteryVoltage, getTimeStamp().c_str());
      httpNuki();
      #if USE_HIVEMQ
      publishToHiveMQ(payloadToProcess);
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
        Serial.printf("[LoRa] Empfangen: MessageID=%u, Tin=%.1f, RHin=%.1f, Ain=%.1f | Tau=%.1f, RHa=%.1f, Aa=%.1f | Fan=%s, Tor=%d %s\n",
                        payloadToProcess.messageID, payloadToProcess.temperatureInnen, payloadToProcess.humidityInnen,
                        payloadToProcess.absHumidityInnen, payloadToProcess.temperatureAussen, payloadToProcess.humidityAussen,
                        payloadToProcess.absHumidityAussen, payloadToProcess.fanOn ? "true" : "false",
                        payloadToProcess.torStatus, getTimeStamp().c_str());
      }
      #if USE_HIVEMQ
      publishToHiveMQ(payloadToProcess);
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

  // Verarbeite ThingSpeak-Finger-Warteschlange
  #if USE_THINGSPEAK
  if (tsFingerQueueHead != tsFingerQueueTail) {
    Serial.printf("[MQTT-TS] Finger-Event aus Warteschlange gesendet: MessageID=%u, Queue: Head=%d, Tail=%d %s\n",
                  tsFingerQueue[tsFingerQueueHead].messageID, tsFingerQueueHead, tsFingerQueueTail, getTimeStamp().c_str());
    publishToThingspeak(tsFingerQueue[tsFingerQueueHead], 0);
    tsFingerQueueHead = (tsFingerQueueHead + 1) % TS_FINGER_QUEUE_SIZE;
  }
  #endif

  #if USE_THINGSPEAK
  if (tuerStatusNeedsReset && (now - tuerEventTimestamp > 5000)) {
    Serial.println("[TS-TUER] Sende Tuerstatus '0' nach 5 Sekunden." + getTimeStamp());
    if (now - lastThingSpeakPublish >= MIN_THINGSPEAK_INTERVAL) {
      publishToThingspeak(lastTuerPayload, 0);
    }
    tuerStatusNeedsReset = false;
  }
  #endif
}