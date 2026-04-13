import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Load your data
df = pd.DataFrame({'Place': ["Long Island"]})

geolocator = Nominatim(user_agent="batch_geocoder")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# Create a 'location' column containing the full geocode object
df['location'] = df['Place'].apply(geocode)

# Extract lat/long from the object
df['latitude'] = df['location'].apply(lambda loc: loc.latitude if loc else None)
df['longitude'] = df['location'].apply(lambda loc: loc.longitude if loc else None)

print(df[['Place', 'latitude', 'longitude']])