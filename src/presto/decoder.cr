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
      "date" => DateDecoder
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

    def decode_value(query_result, row_index, column_index)
      row = query_result.data[row_index].not_nil!
      column = query_result.columns.not_nil![column_index]
      raw_type = column.type_signature.raw_type
      return TYPE_CONVERSION[raw_type].decode(row[column_index])
    end

  end
end
