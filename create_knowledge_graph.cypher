
// Step 1: Load Patients, Countries, and Hemispheres from CSV
// Replace the URL below with your actual file URL in Neo4j AuraDB

LOAD CSV WITH HEADERS FROM 'https://your_actual_csv_url.csv' AS row

// Create Patient Node
MERGE (p:Patient {patient_id: row.patient_id})
SET p.age = toInteger(row.age),
    p.cholesterol = toInteger(row.cholesterol),
    p.blood_pressure = row.blood_pressure,
    p.heart_rate = toInteger(row.heart_rate),
    p.diabetes = toInteger(row.diabetes),
    p.exercise_hours_per_week = toFloat(row.exercise_hours_per_week),
    p.stress_level = toInteger(row.stress_level),
    p.sedentary_hours_per_day = toFloat(row.sedentary_hours_per_day),
    p.income = toInteger(row.income)

// Link Patient to Country
WITH row, p
UNWIND [key IN keys(row) WHERE key STARTS WITH "country_" AND row[key] = "TRUE"] AS countryKey
WITH p, replace(countryKey, "country_", "") AS countryName
MERGE (c:Country {name: countryName})
MERGE (p)-[:LIVES_IN]->(c)

// Link Country to Hemisphere
WITH row, c
UNWIND [key IN keys(row) WHERE key STARTS WITH "hemisphere_" AND row[key] = "TRUE"] AS hemiKey
WITH c, replace(hemiKey, "hemisphere_", "") AS hemisphereName
MERGE (h:Hemisphere {name: hemisphereName})
MERGE (c)-[:LOCATED_IN]->(h);
