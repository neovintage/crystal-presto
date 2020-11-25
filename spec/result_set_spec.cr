require "./spec_helper"

describe Presto::ResultSet do

  # this test isn't valid because not representative of the JSON we get back
  #it "should be able to deserialize a json response" do
    #json = %({"id": "asdf2342adsfs", "infoUri": "https://localhost:8080/something/else"})
    #rs = Presto::QueryResult.from_json(json)
    #typeof(rs).should eq(Presto::QueryResult)
    #rs.info_uri.should eq("https://localhost:8080/something/else")
  #end

  # queued query
  # {"id":"20201106_063434_00035_ftprq","infoUri":"http://localhost:8080/ui/query.html?20201106_063434_00035_ftprq","nextUri":"http://localhost:8080/v1/statement/queued/20201106_063434_00035_ftprq/yc62756235d39feabce72eb6184943bc8263d5975/1","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}

end
