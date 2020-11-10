module Presto

  # todo because of the way presto works all the data gets returned at once we don't have a cursor
  #      consider do the serialization at the time the data comes back.
  #
  class ResultSet < ::DB::ResultSet
    getter query_results
    getter row_count : Int32
    getter request_options : Presto::ConnectionOptions

    def initialize(statement, @query_results : QueryResult, response : HTTP::Client::Response, @request_options)
      super(statement)
      @column_index = -1
      @row_index = -1
      @row_count = @query_results.data.size

      @http_response = response
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
      if @query_results.columns.nil?
        0
      else
        @query_results.columns.not_nil!.size
      end
    end

    def column_name(index : Int32) : String
      #if @query_results.columns.nil?
        #""
      #else
        @query_results.columns.not_nil![index].name
      #end
    end

    def read
      @column_index += 1
      return Decoder.decode_value(@query_results, @row_index, @column_index)

      #return @query_results.decode_value(@row_index, @column_index)
      # todo serialize the types to crystal types here before returning the row
      #return @query_results.data[@row_index].not_nil![@column_index]
    end

    def response_headers
      @http_response.headers
    end

    private def self.serialize_types
    end
  end
end
