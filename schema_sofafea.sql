-- 1. Create a unified locations table to handle all geocoding and prevent duplicates
CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    city TEXT,
    county TEXT,
    state TEXT,
    coordinates TEXT, -- Format: "lat, lon"
    UNIQUE(city, county, state)
);

-- 2. Create the recursive family tree table
CREATE TABLE IF NOT EXISTS family_members (
    member_id SERIAL PRIMARY KEY,
    
    -- Link to your existing directory table
    directory_id INTEGER REFERENCES public.black_loyalist_directory(id),
    
    -- Core Identity
    first_name TEXT,
    last_name TEXT,
    alias TEXT,
    gender TEXT,
    race TEXT,
    ethnicity TEXT,
    generation_number INTEGER,
    
    -- Recursive Parent Links (The Family Tree Core)
    father_id INTEGER REFERENCES family_members(member_id),
    mother_id INTEGER REFERENCES family_members(member_id),
    
    -- Life Events & Locations
    birth_date DATE,
    birth_location_id INTEGER REFERENCES locations(location_id),
    death_date DATE,
    death_location_id INTEGER REFERENCES locations(location_id),
    marriage_date DATE,
    marriage_location_id INTEGER REFERENCES locations(location_id),
    
    -- Military Info
    military_service TEXT,
    branch TEXT,
    war TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create indexes for fast tree traversal and lookups
CREATE INDEX idx_fm_father ON family_members(father_id);
CREATE INDEX idx_fm_mother ON family_members(mother_id);
CREATE INDEX idx_fm_directory ON family_members(directory_id);