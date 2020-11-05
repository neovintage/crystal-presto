module Presto

  # todo because of the way presto works all the data gets returned at once we don't have a cursor
  #      consider do the serialization at the time the data comes back.
  #
  # todo use the query_results struct when getting information back from presto
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
