// Switch to the desired database (will be created if it doesn't exist)
db = db.getSiblingDB('mydb');

// Drop collections if they already exist (for idempotency during testing)
db.installations.drop();
db.products.drop();

// Insert documents into `installations`
db.installations.insertMany([
  {
    name: "Installation Alpha",
    type: "solar",
    location: {
      city: "Berlin",
      country: "Germany"
    },
    status: "active",
    installed_at: new Date("2023-05-10T10:00:00Z"),
    capacity_kw: 150
  },
  {
    name: "Installation Beta",
    type: "wind",
    location: {
      city: "Copenhagen",
      country: "Denmark"
    },
    status: "maintenance",
    installed_at: new Date("2022-09-01T14:30:00Z"),
    capacity_kw: 300
  }
]);

// Insert documents into `products`
db.products.insertMany([
  {
    sku: "P-1001",
    name: "Inverter Pro X",
    category: "electrical",
    price_usd: 1200.50,
    in_stock: true,
    specs: {
      voltage: "220V",
      warranty_years: 5
    },
    added_on: new Date("2024-01-15")
  },
  {
    sku: "P-1002",
    name: "Solar Panel Ultra",
    category: "solar",
    price_usd: 350.00,
    in_stock: false,
    specs: {
      wattage: "350W",
      warranty_years: 10
    },
    added_on: new Date("2024-03-22")
  }
]);
