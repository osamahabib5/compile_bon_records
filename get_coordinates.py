import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

df = pd.DataFrame({'Place': ["China"]})

geolocator = Nominatim(user_agent="batch_geocoder")

# 1. Update the RateLimiter to include addressdetails=True
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, error_wait_seconds=5)

# 2. Perform the geocoding (passing addressdetails=True)
df['location'] = df['Place'].apply(lambda x: geocode(x, addressdetails=True))

# 3. Extract the country from the 'raw' address dictionary
df['country'] = df['location'].apply(lambda loc: loc.raw.get('address', {}).get('country') if loc else None)

# Extract lat/long as before
df['latitude'] = df['location'].apply(lambda loc: loc.latitude if loc else None)
df['longitude'] = df['location'].apply(lambda loc: loc.longitude if loc else None)

print(df[['Place', 'country', 'latitude', 'longitude']])


