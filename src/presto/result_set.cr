module Presto
  alias PrestoValue = String | Nil | Bool | Int32 | Float32 | Float64 | Time | JSON::Any

  struct PrestoColumn
    include JSON::Serializable

    property name : String
    property type : String
  end

  struct PrestoWarning
  end

  struct StatementStats
  end

  struct QueryError
  end

  class QueryResult
    include JSON::Serializable
    include JSON::Serializable::Unmapped

    # original java client properties
    #
    #@JsonProperty("id") String id,
    #@JsonProperty("infoUri") URI infoUri,
    #@JsonProperty("partialCancelUri") URI partialCancelUri,
    #@JsonProperty("nextUri") URI nextUri,
    #@JsonProperty("columns") List<Column> columns,
    #@JsonProperty("data") List<List<Object>> data,
    #@JsonProperty("stats") StatementStats stats,
    #@JsonProperty("error") QueryError error,
    #@JsonProperty("warnings") List<PrestoWarning> warnings,
    #@JsonProperty("updateType") String updateType,
    #@JsonProperty("updateCount") Long updateCount)

    property id : String
    property columns : JSON::Any?
    property data : JSON::Any?
    property stats : JSON::Any?
    property error : JSON::Any?
    property warnings : JSON::Any?
    property update_count : Int64?

    @[JSON::Field(key: "infoUri")]
    property info_uri : String?

    @[JSON::Field(key: "partialCancelUri")]
    property partial_cancel_uri : String?

    @[JSON::Field(key: "nextUri")]
    property next_uri : String?

    @[JSON::Field(key: "updateType")]
    property update_type : String?

  end

  # todo bring this class closer to the results that come back from presto:
  #      https://github.com/prestodb/presto/blob/master/presto-client/src/main/java/com/facebook/presto/client/QueryResults.java
  #
  # todo because of the way presto works all the data gets returned at once we don't have a cursor
  #      consider do the serialization at the time the data comes back.
  class ResultSet < ::DB::ResultSet
    getter query_results
    getter data : JSON::Any
    getter columns : JSON::Any
    getter row_count : Int32
    getter request_options : Presto::ConnectionOptions

    def initialize(statement, @query_results : JSON::Any, response : HTTP::Client::Response, @request_options)
      super(statement)
      @column_index = -1
      @row_index = -1

      @data = @query_results["data"]? || JSON.parse("[]")
      @columns = @query_results["columns"]? || JSON.parse("[]")
      @row_count = @data.size

      @http_response = response

      # todo parse the columns for the data types into hash to make type conversion easier
    end

    def move_next : Bool
      return false if @end

      if @row_index < @row_count - 1
        @row_index += 1
        @column_index = -1
        true
      else
        @end = true
        false
      end
    end

    def column_count : Int32
      @columns.size
    end

    def column_name(index : Int32) : String
      @columns[index]["name"].to_s
    end

    def read
      @column_index += 1
      # todo serialize the types to crystal types here before returning the row
      return @data[@row_index][@column_index]
    end

    def response_headers
      @http_response.headers
    end

    private def self.serialize_types
    end
  end
end
