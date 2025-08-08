-- Use the target database
USE brokers;

-- Drop the table if it already exists
DROP TABLE IF EXISTS brokers;

-- Create 'brokers' table
CREATE TABLE brokers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(150) UNIQUE NOT NULL,
  phone VARCHAR(20),
  country VARCHAR(100),
  joined_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample brokers
INSERT INTO brokers (name, email, phone, country) VALUES
  ('Alice Johnson', 'alice.johnson@example.com', '+49-170-1234567', 'Germany'),
  ('Bob Smith', 'bob.smith@example.com', '+45-20-123456', 'Denmark'),
  ('Carlos Ruiz', 'carlos.ruiz@example.com', '+34-600-123456', 'Spain');
