require "db"
require "http/client"
require "json"
require "uri"
require "./presto/*"

module Presto
  class Statement < ::DB::Statement
    def initialize(connection, @sql : String)
      super(connection)
    end

    protected def http_client
      connection.as(::Presto::Connection).connection
    end

    # todo the args in enumerable should have the options that can be overridden
    #      the connection should have the default options that were set up for the data base. On
    #      a query basis you should be able to override the options.
    protected def perform_query(args : Enumerable) : ResultSet
      start_time = Time.monotonic
      timeout = connection.options.statement_timeout

      http_response = http_client.post("/v1/statement", headers: connection.options.http_headers, body: @sql)
      json = uninitialized JSON::Any

      loop do
        json = JSON.parse(http_response.body)
        # todo there could be an error that would result in this failing
        break if ((Time.monotonic - start_time) > timeout) || json["nextUri"]?.nil? || json["data"]?

        http_response = http_client.get(json["nextUri"].to_s, headers: connection.options.http_headers)
      end

      ResultSet.new(self, json, http_response, connection.options)
    end

    protected def perform_exec(args : Enumerable) : ::DB::ExecResult
    end

    # todo this is needed to release statement resources. for the purposes
    #      of presto well call DELETE to end the query if it hasn't finished
    #protected def do_close
    #end
  end


  class Driver < ::DB::Driver
    def build_connection(context : ::DB::ConnectionContext) : ::Presto::Connection
      ::Presto::Connection.new(context)
    end
  end
end

DB.register_driver "presto", Presto::Driver
