module Presto
  alias PrestoValue = String | Nil | Bool | Int32 | Int64 | Float32 | Float64 | Time | JSON::Any

  struct PrestoColumn
    include JSON::Serializable

    property name : String
    property type : String

    @[JSON::Field(key: "typeSignature")]
    property type_signature : TypeSignature
  end

  struct TypeSignature
    include JSON::Serializable

    @[JSON::Field(key: "rawType")]
    property raw_type : String
    property arguments : Array(TypeSignatureParameter)
  end

  struct TypeSignatureParameter
    include JSON::Serializable

    property kind : String
    property value : JSON::Any
  end

  struct PrestoWarning
    include JSON::Serializable

    property message : String

    @[JSON::Field(key: "warningCode")]
    property warning_code : String
  end

  struct ErrorLocation
    include JSON::Serializable

    @[JSON::Field(key: "lineNumber")]
    property line_number : Int32

    @[JSON::Field(key: "columnNumber")]
    property column_number : Int32
  end

  class FailureInfo
    include JSON::Serializable

    property type : String
    property message : String
    property cause : FailureInfo?
    property supressed : Array(FailureInfo)?
    property stack : Array(String)

    @[JSON::Field(key: "errorLocation")]
    property error_location : ErrorLocation?
  end

  class StageStats
    include JSON::Serializable

    property state : String
    property done : Bool
    property nodes : Int32

    @[JSON::Field(key: "stageId")]
    property stageId : String

    @[JSON::Field(key: "totalSplits")]
    property total_splits : Int32

    @[JSON::Field(key: "queuedSplits")]
    property queued_splits : Int32

    @[JSON::Field(key: "runningSplits")]
    property running_splits : Int32

    @[JSON::Field(key: "completedSplits")]
    property completed_splits : Int32

    @[JSON::Field(key: "cpuTimeMillis")]
    property cpu_time_milliseconds : Float32

    @[JSON::Field(key: "wallTimeMillis")]
    property wall_time_milliseconds : Float32

    @[JSON::Field(key: "processedRows")]
    property processed_rows : Float32

    @[JSON::Field(key: "processedBytes")]
    property processed_bytes : Float32

    @[JSON::Field(key: "physicalInputBytes")]
    property physical_input_bytes : Float32?

    @[JSON::Field(key: "subStages")]
    property sub_stages : Array(StageStats)
  end

  struct StatementStats
    include JSON::Serializable

    property state : String
    property queued : Bool
    property scheduled : Bool
    property nodes : Int32

    @[JSON::Field(key: "totalSplits")]
    property total_splits : Int32

    @[JSON::Field(key: "queuedSplits")]
    property queued_splits : Int32

    @[JSON::Field(key: "runningSplits")]
    property running_splits : Int32

    @[JSON::Field(key: "completedSplits")]
    property completed_splits : Int32

    @[JSON::Field(key: "cpuTimeMillis")]
    property cpu_time_milliseconds : Float32

    @[JSON::Field(key: "wallTimeMillis")]
    property wall_time_milliseconds : Float32

    @[JSON::Field(key: "processedRows")]
    property processed_rows : Float32

    @[JSON::Field(key: "processedBytes")]
    property processed_bytes : Float32

    @[JSON::Field(key: "physicalInputBytes")]
    property physical_input_bytes : Float32?

    @[JSON::Field(key: "queuedTimeMillis")]
    property queued_time_milliseconds : Float32

    @[JSON::Field(key: "elapsedTimeMillis")]
    property elapsed_time_milliseconds : Float32

    @[JSON::Field(key: "peakMemoryBytes")]
    property peak_memory_bytes : Float32

    @[JSON::Field(key: "spilledBytes")]
    property spilled_bytes : Float32

    @[JSON::Field(key: "rootStage")]
    property root_stage : StageStats?
  end

  struct QueryError
    include JSON::Serializable

    property message : String

    @[JSON::Field(key: "sqlState")]
    property sql_state : String?

    @[JSON::Field(key: "errorCode")]
    property error_code : Int32

    @[JSON::Field(key: "errorName")]
    property error_name : String

    @[JSON::Field(key: "errorType")]
    property error_type : String

    @[JSON::Field(key: "errorLocation")]
    property error_location : ErrorLocation?

    @[JSON::Field(key: "failureInfo")]
    property failure_info : FailureInfo
  end

  class QueryResult
    include JSON::Serializable
    include JSON::Serializable::Unmapped

    property id : String
    property stats : StatementStats
    property error : QueryError?
    property warnings : Array(PrestoWarning)?
    property columns : Array(PrestoColumn)?

    # Data is left as JSON::Any because the serialization in Crystal types
    # will happen lazily at the time the row is read.
    #
    property data : Array(JSON::Any?) = [] of JSON::Any?

    @[JSON::Field(key: "updateCount")]
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
end
