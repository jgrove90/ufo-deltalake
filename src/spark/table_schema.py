DELTALAKE = "deltalake"

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
    ("city", "STRING"),
    ("state", "STRING"),
    ("country", "STRING"),
    ("shape", "STRING"),
    ("duration", "STRING"),
    ("summary", "STRING"),
    ("images", "STRING"),
    ("date", "DATE"),
    ("year", "INT"),
    ("month", "INT"),
    ("dayofweek", "STRING"),
    ("week", "INT"),
    ("hour", "INT"),
    ("moonPhaseAngle", "DOUBLE"),
]

# Gold layer
DIM_LOCATION = [
    ("city", "STRING"),
    ("state", "STRING"),
    ("country", "STRING"),
]

DIM_DESCRIPTION = [
    ("shape", "STRING"),
    ("duration", "STRING"),
    ("summary", "STRING"),
    ("images", "STRING"),
]

DIM_DATE = [
    ("date", "DATE"),
    ("year", "INT"),
    ("month", "INT"),
    ("dayofweek", "STRING"),
    ("week", "INT"),
    ("hour", "INT"),
]

DIM_ASTRO = [
    ("moonPhaseAngle", "DOUBLE"),
]

FACT = [
    ("state", "STRING"),
    ("city", "STRING"),
    ("shape", "STRING"),
    ("year", "INT"),
    ("month", "INT"),
    ("week", "INT"),
    ("dayofweek", "STRING"),
    ("hour", "INT"),
    ("moonPhaseAngle", "DOUBLE"),
    ("state_count", "INT"),
    ("city_count", "INT"),
    ("shape_count", "INT"),
    ("year_count", "INT"),
    ("month_count", "INT"),
    ("week_count", "INT"),
    ("day_count", "INT"),
    ("hour_count", "INT"),
    ("phaseangle_count", "INT"),
]
