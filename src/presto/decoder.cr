module Presto
  module Decoder
    extend self

    # https://prestodb.io/docs/current/language/types.html

    TYPE_CONVERSION = {
      "boolean" => BoolDecoder,
      "tinyint" => Int32Decoder,
      "smallint" => Int32Decoder,
      "integer" => Int32Decoder,
      "bigint"  => Int64Decoder,
      "real" => Float32Decoder,
      "double" => Float64Decoder,
      "varchar" => StringDecoder,
      "char" => StringDecoder,
      "varbinary" => StringDecoder,
      "json" => JsonDecoder,
      "date" => DateDecoder,
      "time" => TimeDecoder,
      "time with time zone" => TimeWithZoneDecoder,
      "timestamp" => TimestampDecoder,
      "timestamp with time zone" => TimestampWithTimeZoneDecoder,
      "interval year to month" => IntervalYearToMonthDecoder,
      "interval day to second" => IntervalDayToSecondDecoder,
    }

    macro define_decoders(hash)
      {% for name, func in hash %}
        struct {{ name.id }}Decoder
          def self.decode(val)
            val.{{ func.id }}?
          end
        end
      {% end %}
    end

    define_decoders({
      "Bool" => "as_bool",
      "Int32" => "as_i",
      "Int64" => "as_i64",
      "Float32" => "as_f32",
      "Float64" => "as_f",
      "String" => "as_s"
    })

    struct JsonDecoder
      def self.decode(val)
        JSON.parse(val.as_s)
      end
    end

    struct DateDecoder
      def self.decode(val)
        Time.parse_utc(val.as_s, "%F")
      end
    end

    struct TimeDecoder
      def self.decode(val)
        Time.parse_utc(val.as_s, "%T.%3N")
      end
    end

    struct TimeWithZoneDecoder
      def self.decode(val)
        tokens = val.as_s.split(" ")
        Time.parse(tokens[0], "%T.%3N", Time::Location.load(tokens[1]))
      end
    end

    struct TimestampDecoder
      def self.decode(val)
        Time.parse_utc(val.as_s, "%F %T.%3N")
      end
    end

    struct TimestampWithTimeZoneDecoder
      def self.decode(val)
        tokens = val.as_s.rpartition(" ")
        Time.parse(tokens[0], "%F %T.%3N", Time::Location.load(tokens[2]))
      end
    end

    struct IntervalYearToMonthDecoder
      def self.decode(val)
        tokens = val.as_s.split("-")
        Time::MonthSpan.new(tokens[0].to_i * 12 + tokens[1].to_i)
      end
    end

    # 1 02:00:00.000
    struct IntervalDayToSecondDecoder
      def self.decode(val)
        tokens = val.as_s.split(" ")
        time = tokens[1].split(/:|\./)
        Time::Span.new(days: tokens[0].to_i,
                       hours: time[0].to_i,
                       minutes: time[1].to_i,
                       seconds: time[2].to_i,
                       nanoseconds: time[3].to_i * 1_000_000)
      end
    end

    # todo
    #struct ArrayDecoder
    #end

    # todo
    #struct MapDecoder
    #end

    # todo
    #struct RowDecoder
    #end

    # todo
    #struct IpaddressDecoder
    #end

    # todo
    #struct IpprefixDecoder
    #end

    def decode_value(query_result, row_index, column_index)
      row = query_result.data[row_index].not_nil!
      column = query_result.columns.not_nil![column_index]
      raw_type = column.type_signature.raw_type
      return TYPE_CONVERSION[raw_type].decode(row[column_index])
    end

  end
end
