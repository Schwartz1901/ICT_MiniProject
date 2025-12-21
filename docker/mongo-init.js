// MongoDB initialization script
// Creates the Bigdata database and details collection with sample data

db = db.getSiblingDB('Bigdata');

// Create details collection
db.createCollection('details');

// Insert sample real estate data
db.details.insertMany([
    {
        property_type: "Apartment",
        price: 2500000000,
        area: 75,
        bedrooms: 2,
        bathrooms: 2,
        location: "123 Main Street",
        city: "Ho Chi Minh",
        district: "District 1",
        latitude: 10.7769,
        longitude: 106.7009,
        description: "Modern apartment in city center",
        posted_date: new Date("2024-01-15")
    },
    {
        property_type: "House",
        price: 5500000000,
        area: 120,
        bedrooms: 3,
        bathrooms: 2,
        location: "456 Oak Avenue",
        city: "Ho Chi Minh",
        district: "District 2",
        latitude: 10.7875,
        longitude: 106.7512,
        description: "Spacious family house with garden",
        posted_date: new Date("2024-01-20")
    },
    {
        property_type: "Apartment",
        price: 1800000000,
        area: 55,
        bedrooms: 1,
        bathrooms: 1,
        location: "789 Park Road",
        city: "Hanoi",
        district: "Hoan Kiem",
        latitude: 21.0285,
        longitude: 105.8542,
        description: "Cozy studio near park",
        posted_date: new Date("2024-02-01")
    },
    {
        property_type: "Villa",
        price: 15000000000,
        area: 300,
        bedrooms: 5,
        bathrooms: 4,
        location: "101 Riverside Drive",
        city: "Ho Chi Minh",
        district: "District 7",
        latitude: 10.7326,
        longitude: 106.7196,
        description: "Luxury villa with pool",
        posted_date: new Date("2024-02-10")
    },
    {
        property_type: "Apartment",
        price: 3200000000,
        area: 85,
        bedrooms: 2,
        bathrooms: 2,
        location: "222 Tower Street",
        city: "Da Nang",
        district: "Hai Chau",
        latitude: 16.0544,
        longitude: 108.2022,
        description: "Sea view apartment",
        posted_date: new Date("2024-02-15")
    }
]);

print('MongoDB initialized with sample data');
print('Database: Bigdata');
print('Collection: details');
print('Documents inserted: 5');
