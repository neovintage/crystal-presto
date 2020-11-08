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
      query_result = uninitialized QueryResult

      loop do
        query_result = QueryResult.from_json(http_response.body)
        # todo there could be an error that would result in this failing
        break if ((Time.monotonic - start_time) > timeout) || query_result.next_uri.nil? || !query_result.data.empty?

        http_response = http_client.get(query_result.next_uri.to_s, headers: connection.options.http_headers)
      end

      ResultSet.new(self, query_result, http_response, connection.options)
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
