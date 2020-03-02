require "db"
require "http/client"
require "uri"
require "./presto/*"

module Presto
  HEADERS = HTTP::Headers{
    "User-Agent" => "presto-crystal/#{VERSION}"
  }

  class Connection < ::DB::Connection
    protected getter connection

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
    def initialize(connection, @sql : String)
      super(connection)
    end

    protected def conn
      connection.as(Connection).connection
    end

    protected def perform_query(args : Enumerable) : ResultSet
      response = conn.post("/v1/statement", headers: nil, body: @sql)
      # todo need a loop to wait for the result. Seems like clients are responsible for managing the response
      # todo can likely turn this into a fiber if need be.
      ResultSet.new(self)
    end

    protected def perform_exec(args : Enumerable) : ::DB::ExecResult
    end
  end

  class ResultSet < ::DB::ResultSet
    def move_next : Bool
    end

    def column_count : Int32
    end

    def column_index(index : Int32)
    end

    def column_name(index : Int32) : String
    end

    def read
    end
  end

  class Driver < ::DB::Driver
    def build_connection(context : ::DB::ConnectionContext) : Connection
      Connection.new(context)
    end
  end
end

DB.register_driver "presto", Presto::Driver
