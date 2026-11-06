"""Configuration and constants for the simulator."""

MERCHANT_COUNT = 300
DUPLICATE_RATIO = 0.02
LATE_RATIO = 0.05
MAX_DELAY_SECONDS = 15 * 60
TOPIC = "transactions"
BATCH_DURATION_SECONDS = 3600 * 4

CITIES = {
    "Rabat": (34.0209, -6.8416),
    "Casablanca": (33.5731, -7.5898),
    "Marrakech": (31.6295, -7.9811),
    "Fes": (34.0331, -5.0003),
    "Tanger": (35.7595, -5.8340),
    "Agadir": (30.4278, -9.5981),
    "Meknes": (33.8935, -5.5473),
    "Oujda": (34.6814, -1.9086),
    "Tetouan": (35.5889, -5.3626),
    "Kenitra": (34.2610, -6.5802),
}

CATEGORIES = (
    "epicerie",
    "carburant",
    "telecom",
    "restauration",
    "pharmacie",
    "e-commerce",
)

STATUSES = (
    "INITIATED",
    "AUTHORIZED",
    "COMPLETED",
    "REJECTED",
    "REVERSED",
)

CHANNELS = ("APP", "QR", "USSD")
