require "./spec_helper"

describe "PrestoTypes" do

  # In Presto the queued QueryResult is an intermediate stage and may
  # not have data in it.
  #
  it "should be able to parse queued responses" do
    qr = Presto::QueryResult.from_json(presto_queued_query_json)
    qr.should_not be_nil
    qr.stats.state.should eq("QUEUED")
    qr.data.size.should eq(0)
  end

  #it "should parse a full response" do
  #end

  it "should be able to parse and error" do
    qr = Presto::QueryResult.from_json(malformed_query_presto_response)
    qr.should_not be_nil
    qr.stats.state.should eq("FAILED")
    qr.error.should_not be_nil
    qr.error.not_nil!.error_name.should eq("SYNTAX_ERROR")
  end
end
