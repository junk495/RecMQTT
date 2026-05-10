// Configuration.h
// Teil der EByte LoRa E220 Library
// Definition der Konfigurationsstruktur für das E220-Modul

#ifndef CONFIGURATION_H
#define CONFIGURATION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Struktur für die UART- und Funkparameter des E220-Moduls
typedef struct {
    uint8_t ADDH;        // High‐Byte der Ziel‐Adresse
    uint8_t ADDL;        // Low‐Byte der Ziel‐Adresse
    uint8_t SPED;        // UART‐Baudrate & Parität & Stopp‐Bits
    uint8_t CHAN;        // Funkkanal (0–83 entspricht 410–441 MHz)
    uint8_t OPTION;      // Air‐Data‐Rate, TX‐Power, WOR‐Funktion
    // (weitere Felder je nach Library‐Version)
} Configuration;

// Antwortcontainer inklusive Status
typedef struct {
    struct {
        uint8_t code;           // 1 = Erfolg, sonst Fehlercode
        char    description[32];
    } status;
    uint8_t data[sizeof(Configuration)];
} ResponseStructContainer;

#ifdef __cplusplus
}
#endif

#endif // CONFIGURATION_H
