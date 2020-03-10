require "db"
require "http/client"
require "json"
require "uri"
require "./presto/*"

module Presto
  HEADERS = HTTP::Headers{
    "User-Agent" => "presto-crystal/#{VERSION}"
  }

  class Connection < ::DB::Connection
    protected getter connection

    # todo throw error if username isnt defined. that's required
    def initialize(context)
      super(context)
      context.uri.scheme = "http"
      @connection = HTTP::Client.new(context.uri)
      @connection.basic_auth(context.uri.user, context.uri.password)
      @connection.before_request do |request|
        request.headers["User-Agent"] = "presto-crystal"
      end
    end

    def build_unprepared_statement(query) : Statement
      Statement.new(self, query)
    end

    def build_prepared_statement(query) : Statement
      Statement.new(self, query)
    end
  end

  class Statement < ::DB::Statement
    STATES = [
      "QUEUED",
      "PLANNING",
      "STARTING",
      "RUNNING",
      "BLOCKED",
      "FINISHING",
      "FINISHED",
      "FAILED"
    ]

    def initialize(connection, @sql : String)
      super(connection)
    end

    protected def conn
      connection.as(Connection).connection
    end

    protected def perform_query(args : Enumerable) : ResultSet
      start_time = Time.monotonic
      timeout = statement_timeout
      http_response = conn.post("/v1/statement", headers: nil, body: @sql)
      json = uninitialized JSON::Any

      loop do
        json = JSON.parse(http_response.body)
        break if (Time.monotonic - start_time) > timeout || json["nextUri"]?.nil? || json["data"]?

        http_response = conn.get(json["nextUri"].to_s)
      end

      puts json
      ResultSet.new(self, json)
    end

    protected def perform_exec(args : Enumerable) : ::DB::ExecResult
    end

    # todo enable this to be overriden by user
    private def statement_timeout
      Time::Span.new(seconds: 10, nanoseconds: 0)
    end
  end

  class ResultSet < ::DB::ResultSet
    getter query_results
    getter data : JSON::Any
    getter columns : JSON::Any
    getter row_count : Int32

    def initialize(statement, @query_results : JSON::Any)
      super(statement)
      @column_index = -1
      @row_index = -1

      @data = @query_results["data"]? || JSON.parse("[]")
      @columns = @query_results["columns"]? || JSON.parse("[]")
      @row_count = @data.size.as(Int32)

      # todo parse the columns for the data types into hash to make type conversion easier
    end

    def move_next : Bool
      return false if @end

      if @row_index < @row_count
        @row_index += 1
        @column_index = 0
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
      return @data[@row_index][@column_index]
    end
  end

  class Driver < ::DB::Driver
    def build_connection(context : ::DB::ConnectionContext) : Connection
      Connection.new(context)
    end
  end
end

DB.register_driver "presto", Presto::Driver
