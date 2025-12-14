/**
 * Generate sample product data for the demo
 */

const categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"];
const brands = ["TechCorp", "StyleCo", "HomeBasics", "SportsPro", "ReadMore"];
const colors = ["Red", "Blue", "Green", "Black", "White", "Silver"];

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateProducts(count) {
  const products = [];

  for (let i = 1; i <= count; i++) {
    const category = randomChoice(categories);
    const brand = randomChoice(brands);
    const basePrice = Math.floor(Math.random() * 500) + 10;

    products.push({
      id: `prod-${String(i).padStart(5, "0")}`,
      name: `${brand} ${category.split(" ")[0]} Item ${i}`,
      category,
      brand,
      price: basePrice + Math.random() * 0.99,
      originalPrice: basePrice * 1.2 + Math.random() * 0.99,
      rating: Math.floor(Math.random() * 5) + 1,
      reviewCount: Math.floor(Math.random() * 500),
      inStock: Math.random() > 0.2,
      color: randomChoice(colors),
      createdAt: new Date(
        Date.now() - Math.floor(Math.random() * 365 * 24 * 60 * 60 * 1000)
      ).toISOString(),
    });
  }

  return products;
}

// Generate 5000 products
const products = generateProducts(5000);

// Write to file
const fs = require("fs");
fs.writeFileSync("products.json", JSON.stringify(products, null, 2));

console.log(`Generated ${products.length} products to products.json`);
