-- Table for storing sectoral performance
CREATE TABLE sectoral_performance (
    id SERIAL PRIMARY KEY,
    date_captured DATE NOT NULL,
    duration VARCHAR(10) NOT NULL, -- '1d', '5d', '1m', '3m', '1y'
    sector VARCHAR(100) NOT NULL,
    market_cap_change NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_captured, duration, sector)
);

-- Table for storing BSE 500 distribution
CREATE TABLE bse500_distribution (
    id SERIAL PRIMARY KEY,
    date_captured DATE NOT NULL,
    total_companies INTEGER DEFAULT 500,
    below_minus_15 INTEGER DEFAULT 0,
    minus_15_to_minus_10 INTEGER DEFAULT 0,
    minus_10_to_minus_5 INTEGER DEFAULT 0,
    minus_5_to_minus_2 INTEGER DEFAULT 0,
    minus_2_to_0 INTEGER DEFAULT 0,
    exact_0 INTEGER DEFAULT 0,
    plus_0_to_2 INTEGER DEFAULT 0,
    plus_2_to_5 INTEGER DEFAULT 0,
    plus_5_to_10 INTEGER DEFAULT 0,
    plus_10_to_15 INTEGER DEFAULT 0,
    above_15 INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_captured)
);

CREATE TABLE market_data (
    id SERIAL PRIMARY KEY,
    index_name VARCHAR(50) NOT NULL,    -- e.g., 'NIFTY50', 'BSE500'
    date DATE NOT NULL,
    open DECIMAL(16,2) NOT NULL,
    high DECIMAL(16,2) NOT NULL,
    low DECIMAL(16,2) NOT NULL,
    close DECIMAL(16,2) NOT NULL,
    volume BIGINT NOT NULL,
    change_percent DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better query performance
CREATE INDEX idx_sectoral_date ON sectoral_performance(date_captured);
CREATE INDEX idx_sectoral_duration ON sectoral_performance(duration);
CREATE INDEX idx_bse_date ON bse500_distribution(date_captured);
CREATE INDEX idx_market_data_date ON market_data(date);

ALTER TABLE market_data ADD CONSTRAINT unique_market_data_date 
    UNIQUE (index_name, date);
