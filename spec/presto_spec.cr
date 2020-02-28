require "./spec_helper"

describe Presto do
  it "works" do
    false.should eq(true)
  end

  it "should query a cluster" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer limit 1"
      puts result
    end
  end
end
