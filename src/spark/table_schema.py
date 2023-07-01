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

# Gold layer (since there are no PKs not sure if gold layer is required but made it anyway)
DIM_LOCATION = [
    ("location_id", "LONG"),
    ("city", "STRING"),
    ("state", "STRING"),
    ("country", "STRING"),
]

DIM_DESCRIPTION = [
    ("description_id", "LONG"),
    ("shape", "STRING"),
    ("duration", "STRING"),
    ("summary", "STRING"),
    ("images", "STRING"),
]

DIM_DATE = [
    ("date_id", "LONG"),
    ("date", "DATE"),
    ("year", "INT"),
    ("month", "INT"),
    ("dayofweek", "STRING"),
    ("week", "INT"),
    ("hour", "INT"),
]

DIM_ASTRO = [
    ("astro_id", "LONG"),
    ("moonPhaseAngle", "DOUBLE"),
]

FACT = [
    ("location_id", "LONG"),
    ("description_id", "LONG"),
    ("date_id", "LONG"),
    ("astro_id", "LONG"),
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
