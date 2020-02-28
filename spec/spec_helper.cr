require "spec"
require "../src/presto"

DB_URL = ENV["DATABASE_URL"]? || "presto://localhost:8080/"
