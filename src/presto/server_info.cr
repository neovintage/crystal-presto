module Presto
  struct ServerInfo
    include JSON::Serializable

    @[JSON::Field(key: "nodeVersion")]
    property node_version : NodeVersion
    property environment : String
    property coordinator : Bool
    property starting : Bool
    property uptime : String?
  end

  struct NodeVersion
    include JSON::Serializable

    property version : String
  end
end
