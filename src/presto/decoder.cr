module Presto
  module Decoder
    extend self

    TYPE_CONVERSION = {
      "boolean" => BoolDecoder
    }

    struct BoolDecoder

      def self.decode(val)
        val.as_bool
      end

    end

    def decode_value(query_result, row_index, column_index)
      row = query_result.data[row_index].not_nil!
      column_type = query_result.columns.not_nil![column_index].type
      return TYPE_CONVERSION[column].decode(row[column_type])
    end
  end
end
