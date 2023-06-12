from geopy.geocoders import Nominatim
from ratelimit import limits, sleep_and_retry


# Create a geocoder instance
geolocator = Nominatim(user_agent="address_cleaner")

# Cache dictionary to store geocoded addresses
cache = {}


@sleep_and_retry
@limits(calls=1, period=1)
def clean_address(address) -> tuple:
    """
    Clean and geocode an address.

    Args:
        address (str): The address to clean and geocode.

    Returns:
        tuple: A tuple containing the cleaned address components 
               - city, state, country, latitude, longitude.
               If the address could not be geocoded or an exception occurs, it returns None.
    """
    try:
        if address in cache:
            return cache[address]
        else:
            location = geolocator.geocode(address, addressdetails=True)
            if location:
                raw_address = location.raw.get("address", {})
                city = (
                    raw_address.get("city")
                    or raw_address.get("town")
                    or raw_address.get("village")
                )
                state = raw_address.get("state")
                country = raw_address.get("country")
                latitude = location.latitude
                longitude = location.longitude
                cache_result = f"{city}, {state}, {country}"
                cache[address] = cache_result
                return f"{city}, {state}, {country}, {latitude}, {longitude}"
            else:
                return None
    except Exception as e:
        print(f"{e}")
