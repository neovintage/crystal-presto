require "spec"
require "../src/presto"

DB_URL = ENV["DATABASE_URL"]? || "presto://presto:@localhost:8080/tpch/sf1"
