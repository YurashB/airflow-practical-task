CREATE TABLE airbnb_listings
(
    id                             SERIAL PRIMARY KEY,
    name                           TEXT,
    host_id                        INTEGER,
    host_name                      TEXT,
    neighbourhood_group            TEXT,
    neighbourhood                  TEXT,
    latitude                       DECIMAL(9, 6),
    longitude                      DECIMAL(9, 6),
    room_type                      TEXT,
    price                          INTEGER,
    minimum_nights                 INTEGER,
    number_of_reviews              INTEGER,
    last_review                    DATE,
    reviews_per_month              DECIMAL(3, 2),
    calculated_host_listings_count INTEGER,
    availability_365               INTEGER
);