-- ============================================================
-- Automotive Data Engineering Platform
-- PostgreSQL OLTP Source Setup
-- ============================================================

-- Create project database
CREATE DATABASE automotive_db;

-- Connect to the project database
-- Run this command inside psql:
-- \c automotive_db

-- Create domain-based schemas
CREATE SCHEMA customer;
CREATE SCHEMA dealer;
CREATE SCHEMA product;
CREATE SCHEMA sales;