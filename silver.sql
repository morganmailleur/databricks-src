CREATE OR REPLACE TABLE workspace.silver.calendar AS
SELECT
  CAST(listing_id AS bigint) AS listing_id,
  CAST(date AS DATE) AS date,
  CAST(available AS BOOLEAN) AS available,
  CAST(price AS INT) AS price,
  CAST(adjusted_price AS INT) AS adjusted_price,
  CAST(minimum_nights AS INT) AS minimum_nights,
  CAST(maximum_nights AS INT) AS maximum_nights,
  city
FROM workspace.bronze.calendar;

CREATE OR REPLACE TABLE workspace.silver.reviews_details AS
SELECT
  CAST(listing_id AS BIGINT) AS listing_id,
  CAST(id AS BIGINT) AS id,
  CAST(date AS DATE) AS date,
  CAST(reviewer_id AS BIGINT) AS reviewer_id,
  CAST(reviewer_name AS STRING) AS reviewer_name,
  CAST(comments AS STRING) AS comments,
  city 
FROM workspace.bronze.reviews_details
WHERE try_cast(id AS BIGINT) IS NOT NULL
AND try_cast(listing_id AS BIGINT) IS NOT NULL;

create or replace table workspace.silver.listings_details as
select 
cast(id AS BIGINT) as id,
cast(listing_url AS STRING) as listing_url,
cast(scrape_id AS STRING) as scrape_id,
cast(last_scraped AS TIMESTAMP) as last_scraped,
cast(source AS STRING) as source,
cast(name AS STRING) as name,
cast(description AS STRING) as description,
cast(neighborhood_overview AS STRING) as neighborhood_overview,
picture_url,
host_id,
host_url,
host_name,
host_since,
host_location,
host_about,
host_response_time,
host_response_rate,
host_acceptance_rate,
host_is_superhost,
host_thumbnail_url,
host_picture_url,
host_neighbourhood,
host_listings_count,
host_total_listings_count,
host_verifications,
host_has_profile_pic,
host_identity_verified,
neighbourhood,
neighbourhood_cleansed,
neighbourhood_group_cleansed,
latitude,
longitude,
property_type,
room_type,
accommodates,
bathrooms,
bathrooms_text,
bedrooms,
beds,
amenities,
price,
minimum_nights,
maximum_nights,
minimum_minimum_nights,
maximum_minimum_nights,
minimum_maximum_nights,
maximum_maximum_nights,
minimum_nights_avg_ntm,
maximum_nights_avg_ntm,
calendar_updated,
has_availability,
availability_30,
availability_60,
availability_90,
availability_365,
calendar_last_scraped,
number_of_reviews,
number_of_reviews_ltm,
first_review,
last_review,
review_scores_rating,
review_scores_accuracy,
review_scores_cleanliness,
review_scores_checkin,
review_scores_communication,
review_scores_location,
review_scores_value,
calculated_host_listings_count,
calculated_host_listings_count_entire_homes,
calculated_host_listings_count_private_rooms,
calculated_host_listings_count_shared_rooms,
reviews_per_month,
city
from workspace.bronze.listings_details 
WHERE try_cast(id AS BIGINT) IS NOT NULL
AND try_cast(host_id AS BIGINT) IS NOT NULL;

CREATE OR REPLACE TABLE workspace.silver.neighbourhoods_geo AS
SELECT
    feature.properties.neighbourhood AS neighbourhood,
    feature.properties.neighbourhood_group AS neighbourhood_group,
    b.city,
    feature.type AS feature_type,
    feature.geometry.type AS geometry_type,
    st_geomfromgeojson(
        to_json(feature.geometry)
    ) AS geom
FROM workspace.bronze.neighbourhoods_geo b
LATERAL VIEW EXPLODE(b.features) AS feature;

Create or replace table workspace.silver.listings as

select 
cast(id as bigint) as id,
name,
cast(host_id as bigint) as host_id,
host_name,
neighbourhood_group,
neighbourhood,
cast(latitude as double) as latitude,
cast(longitude as double) as longitude,
room_type,
cast(price as int) as price,
cast(minimum_nights as int) as minimum_nights,
cast(number_of_reviews as int) as number_of_reviews,
last_review,
cast(reviews_per_month as double) as reviews_per_month,
cast(calculated_host_listings_count as int) as calculated_host_listings_count,
cast(availability_365 as int) as availability_365,
cast(number_of_reviews_ltm as int) as number_of_reviews_ltm,
license,
city
from workspace.bronze.listings
WHERE try_cast(id AS BIGINT) IS NOT NULL
AND try_cast(host_id AS BIGINT) IS NOT NULL;
