require "./spec_helper"

describe Presto::ServerInfo do

  it "should get the current server info" do
    db = DB.open(DB_URL)
    db.using_connection do |conn|
      server_info = conn.server_info
      typeof(server_info).should eq(Presto::ServerInfo)
      server_info.environment.should eq("docker")
      server_info.coordinator.should eq(true)
      server_info.starting.should eq(false)
      server_info.node_version.version.should eq("330")
    end
  end

end
