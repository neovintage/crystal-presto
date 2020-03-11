require "spec"
require "../src/presto"

DB_URL = ENV["DATABASE_URL"]? ||
          ENV["PRESTO_HOST"]? && ENV["PRESTO_PORT"]? ? "presto://presto:@#{ENV["PRESTO_HOST"]}:#{ENV["PRESTO_PORT"]}/tpch/sf1" : "presto://presto:@localhost:8080/tpch/sf1"

puts DB_URL
