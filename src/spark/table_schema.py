LAKEHOUSE = "lakehouse"

TABLE_PATHS = {
    "bronze": "ufo/bronze",
    "silver": "ufo/silver",
    "dim_location": "ufo/gold/dim_location",
    "dim_description": "ufo/gold/dim_description",
    "dim_date": "ufo/gold/dim_date",
    "dim_astro": "ufo/gold/dim_astro",
    "fact": "ufo/gold/fact",
}

# Bronze layer
BRONZE = [
    ("DateTime", "STRING"),
    ("City", "STRING"),
    ("State", "STRING"),
    ("Country", "STRING"),
    ("Shape", "STRING"),
    ("Duration", "STRING"),
    ("Summary", "STRING"),
    ("Posted", "STRING"),
    ("Images", "STRING"),
]

# Silver layer
SILVER = [
    ("id", "LONG"),
    ("id_location", "LONG"),
    ("city", "STRING"),
    ("state", "STRING"),
    ("country", "STRING"),
    ("id_description", "LONG"),
    ("shape", "STRING"),
    ("duration", "STRING"),
    ("summary", "STRING"),
    ("images", "STRING"),
    ("id_date", "LONG"),
    ("date", "DATE"),
    ("year", "INT"),
    ("month", "INT"),
    ("dayofweek", "STRING"),
    ("week", "INT"),
    ("hour", "INT"),
    ("id_astro", "LONG"),
    ("moonPhaseAngle", "DOUBLE"),
]

# Gold layer
DIM_LOCATION = [
    ("id_location", "LONG"),
    ("city", "STRING"),
    ("state", "STRING"),
    ("country", "STRING"),
]

DIM_DESCRIPTION = [
    ("id_description", "LONG"),
    ("shape", "STRING"),
    ("duration", "STRING"),
    ("summary", "STRING"),
    ("images", "STRING"),
]

DIM_DATE = [
    ("id_date", "LONG"),
    ("date", "DATE"),
    ("year", "INT"),
    ("month", "INT"),
    ("dayofweek", "STRING"),
    ("week", "INT"),
    ("hour", "INT"),
]

DIM_ASTRO = [
    ("id_astro", "LONG"),
    ("moonPhaseAngle", "DOUBLE"),
]

FACT = [
    ("id_location", "LONG"),
    ("id_description", "LONG"),
    ("id_date", "LONG"),
    ("id_astro", "LONG"),
]
