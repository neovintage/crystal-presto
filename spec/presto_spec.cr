require "./spec_helper"

describe Presto do
  it "should query a cluster" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer limit 1"
      typeof(result).should eq Presto::ResultSet
    end
  end

  it "should return the column size" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer limit 1"
      result.column_count.should eq 8
    end
  end

  it "should return column even if data doesnt exist" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer where name = 'nonsense' limit 1"
      result.column_count.should eq 8
    end
  end

  it "should have an empty data array if nothing is returned" do
    DB.open(DB_URL) do |db|
      result = db.query "select * from tpch.sf1.customer where name = 'nonsense' limit 1"
      result.data.size.should eq 0
    end
  end

  it "should be able to return rows" do
    DB.open(DB_URL) do |db|
      db.query "select * from tpch.sf1.customer where name = 'Customer#000000001' limit 1" do |rs|
        rs.row_count.should eq 1
        rs.column_name(0).should eq "custkey"
        rs.each do
          rs.read.as_i.should eq 1
        end
      end
    end
  end

  it "should show the response headers" do
    DB.open(DB_URL) do |db|
      db.query "select * from tpch.sf1.customer where name = 'Customer#000000001' limit 1" do |rs|
        typeof(rs.headers).should eq HTTP::Headers
        rs.headers.should_not be_nil
      end
    end
  end
end
