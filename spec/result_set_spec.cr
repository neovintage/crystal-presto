require "./spec_helper"

describe Presto::ResultSet do

  # this test isn't valid because not representative of the JSON we get back
  #it "should be able to deserialize a json response" do
    #json = %({"id": "asdf2342adsfs", "infoUri": "https://localhost:8080/something/else"})
    #rs = Presto::QueryResult.from_json(json)
    #typeof(rs).should eq(Presto::QueryResult)
    #rs.info_uri.should eq("https://localhost:8080/something/else")
  #end

end
