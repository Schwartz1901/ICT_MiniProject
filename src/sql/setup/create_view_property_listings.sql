CREATE OR REPLACE VIEW V_PROPERTY_LISTINGS AS
SELECT
    f.id AS property_id,
    f.city,
    f.district,
    f.price,
    f.price_per_sqm,
    f.area,
    f.bedrooms,
    f.bathrooms,
    f.rooms_total,
    f.price_category,
    f.area_category,
    f.is_high_value,
    f.latitude,
    f.longitude
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES f
