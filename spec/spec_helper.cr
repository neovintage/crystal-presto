require "spec"
require "../src/presto"

DB_URL = ENV["DATABASE_URL"]? ||
          ENV["PRESTO_HOST"]? && ENV["PRESTO_PORT"]? ? "presto://presto:@#{ENV["PRESTO_HOST"]}:#{ENV["PRESTO_PORT"]}/tpch/sf1" : "presto://presto:@localhost:8080/tpch/sf1"

def presto_queued_query_json
  <<-JSON
  {
    "id": "20201108_133222_00000_3ztmi",
    "infoUri": "http://localhost:8080/ui/query.html?20201108_133222_00000_3ztmi",
    "nextUri": "http://localhost:8080/v1/statement/queued/20201108_133222_00000_3ztmi/y69ca1cd1bfa1ea4d26894547157814c7b389ac8f/1",
    "stats": {
      "state": "QUEUED",
      "queued": true,
      "scheduled": false,
      "nodes": 0,
      "totalSplits": 0,
      "queuedSplits": 0,
      "runningSplits": 0,
      "completedSplits": 0,
      "cpuTimeMillis": 0,
      "wallTimeMillis": 0,
      "queuedTimeMillis": 0,
      "elapsedTimeMillis": 0,
      "processedRows": 0,
      "processedBytes": 0,
      "peakMemoryBytes": 0,
      "spilledBytes": 0
    },
    "warnings": []
  }
  JSON
end

def presto_success_query_json
  <<-JSON
  {
    "id": "20201108_144651_00026_3ztmi",
    "infoUri": "http://localhost:8080/ui/query.html?20201108_144651_00026_3ztmi",
    "partialCancelUri": "http://localhost:8080/v1/statement/executing/partialCancel/20201108_144651_00026_3ztmi/0/y947ba51e2a004c17234b7916323c383d872d52f0/1",
    "nextUri": "http://localhost:8080/v1/statement/executing/20201108_144651_00026_3ztmi/y947ba51e2a004c17234b7916323c383d872d52f0/1",
    "columns": [
      {
        "name": "custkey",
        "type": "bigint",
        "typeSignature": {
          "rawType": "bigint",
          "arguments": []
        }
      },
      {
        "name": "name",
        "type": "varchar(25)",
        "typeSignature": {
          "rawType": "varchar",
          "arguments": [
            {
              "kind": "LONG",
              "value": 25
            }
          ]
        }
      },
      {
        "name": "address",
        "type": "varchar(40)",
        "typeSignature": {
          "rawType": "varchar",
          "arguments": [
            {
              "kind": "LONG",
              "value": 40
            }
          ]
        }
      },
      {
        "name": "nationkey",
        "type": "bigint",
        "typeSignature": {
          "rawType": "bigint",
          "arguments": []
        }
      },
      {
        "name": "phone",
        "type": "varchar(15)",
        "typeSignature": {
          "rawType": "varchar",
          "arguments": [
            {
              "kind": "LONG",
              "value": 15
            }
          ]
        }
      },
      {
        "name": "acctbal",
        "type": "double",
        "typeSignature": {
          "rawType": "double",
          "arguments": []
        }
      },
      {
        "name": "mktsegment",
        "type": "varchar(10)",
        "typeSignature": {
          "rawType": "varchar",
          "arguments": [
            {
              "kind": "LONG",
              "value": 10
            }
          ]
        }
      },
      {
        "name": "comment",
        "type": "varchar(117)",
        "typeSignature": {
          "rawType": "varchar",
          "arguments": [
            {
              "kind": "LONG",
              "value": 117
            }
          ]
        }
      }
    ],
    "data": [
      [
        37501,
        "Customer#000037501",
        "Ftb6T5ImHuJ",
        2,
        "12-397-688-6719",
        -324.85,
        "HOUSEHOLD",
        "pending ideas use carefully. express, ironic platelets use among the furiously regular instructions. "
      ]
    ],
    "stats": {
      "state": "RUNNING",
      "queued": false,
      "scheduled": true,
      "nodes": 1,
      "totalSplits": 21,
      "queuedSplits": 17,
      "runningSplits": 0,
      "completedSplits": 4,
      "cpuTimeMillis": 327,
      "wallTimeMillis": 1520,
      "queuedTimeMillis": 1,
      "elapsedTimeMillis": 661,
      "processedRows": 21908,
      "processedBytes": 0,
      "peakMemoryBytes": 0,
      "spilledBytes": 0,
      "rootStage": {
        "stageId": "0",
        "state": "RUNNING",
        "done": false,
        "nodes": 1,
        "totalSplits": 17,
        "queuedSplits": 17,
        "runningSplits": 0,
        "completedSplits": 0,
        "cpuTimeMillis": 0,
        "wallTimeMillis": 0,
        "processedRows": 0,
        "processedBytes": 0,
        "subStages": [
          {
            "stageId": "1",
            "state": "FINISHED",
            "done": true,
            "nodes": 1,
            "totalSplits": 4,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 4,
            "cpuTimeMillis": 327,
            "wallTimeMillis": 1520,
            "processedRows": 21908,
            "processedBytes": 0,
            "subStages": []
          }
        ]
      },
      "progressPercentage": 19.047619047619047
    },
    "warnings": []
  }
  JSON
end

def malformed_query_presto_response
  <<-JSON
  {
    "id": "20201108_145227_00030_3ztmi",
    "infoUri": "http://localhost:8080/ui/query.html?20201108_145227_00030_3ztmi",
    "stats": {
      "state": "FAILED",
      "queued": false,
      "scheduled": false,
      "nodes": 0,
      "totalSplits": 0,
      "queuedSplits": 0,
      "runningSplits": 0,
      "completedSplits": 0,
      "cpuTimeMillis": 0,
      "wallTimeMillis": 0,
      "queuedTimeMillis": 0,
      "elapsedTimeMillis": 0,
      "processedRows": 0,
      "processedBytes": 0,
      "peakMemoryBytes": 0,
      "spilledBytes": 0
    },
    "error": {
      "message": "line 1:8: mismatched input 'select'. Expecting: '*', 'ALL', 'DISTINCT', <expression>",
      "errorCode": 1,
      "errorName": "SYNTAX_ERROR",
      "errorType": "USER_ERROR",
      "errorLocation": {
        "lineNumber": 1,
        "columnNumber": 8
      },
      "failureInfo": {
        "type": "io.prestosql.sql.parser.ParsingException",
        "message": "line 1:8: mismatched input 'select'. Expecting: '*', 'ALL', 'DISTINCT', <expression>",
        "suppressed": [],
        "stack": [
          "io.prestosql.sql.parser.ErrorHandler.syntaxError(ErrorHandler.java:107)",
          "org.antlr.v4.runtime.ProxyErrorListener.syntaxError(ProxyErrorListener.java:41)",
          "org.antlr.v4.runtime.Parser.notifyErrorListeners(Parser.java:544)",
          "org.antlr.v4.runtime.DefaultErrorStrategy.reportUnwantedToken(DefaultErrorStrategy.java:377)",
          "org.antlr.v4.runtime.DefaultErrorStrategy.singleTokenDeletion(DefaultErrorStrategy.java:548)",
          "org.antlr.v4.runtime.DefaultErrorStrategy.sync(DefaultErrorStrategy.java:266)",
          "io.prestosql.sql.parser.SqlBaseParser.querySpecification(SqlBaseParser.java:4831)",
          "io.prestosql.sql.parser.SqlBaseParser.queryPrimary(SqlBaseParser.java:4609)",
          "io.prestosql.sql.parser.SqlBaseParser.queryTerm(SqlBaseParser.java:4414)",
          "io.prestosql.sql.parser.SqlBaseParser.queryNoWith(SqlBaseParser.java:4169)",
          "io.prestosql.sql.parser.SqlBaseParser.query(SqlBaseParser.java:3532)",
          "io.prestosql.sql.parser.SqlBaseParser.statement(SqlBaseParser.java:1809)",
          "io.prestosql.sql.parser.SqlBaseParser.singleStatement(SqlBaseParser.java:244)",
          "io.prestosql.sql.parser.SqlParser.invokeParser(SqlParser.java:146)",
          "io.prestosql.sql.parser.SqlParser.createStatement(SqlParser.java:86)",
          "io.prestosql.execution.QueryPreparer.prepareQuery(QueryPreparer.java:55)",
          "io.prestosql.dispatcher.DispatchManager.createQueryInternal(DispatchManager.java:174)",
          "io.prestosql.dispatcher.DispatchManager.lambda$createQuery$0(DispatchManager.java:146)",
          "io.prestosql.$gen.Presto_330____20201108_133202_2.run(Unknown Source)",
          "java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)",
          "java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)",
          "java.base/java.lang.Thread.run(Thread.java:834)"
        ],
        "errorLocation": {
          "lineNumber": 1,
          "columnNumber": 8
        }
      }
    },
    "warnings": []
  }
  JSON
end
