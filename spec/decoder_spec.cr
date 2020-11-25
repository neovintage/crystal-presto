require "./spec_helper"

describe Presto::Decoder do

  # Handing nulls
  test_decode "Null", "cast(x as boolean) from ( values (NULL) ) as t(x)", nil
  test_decode "Null", "cast(x as integer) from ( values (null) ) as t(x)", nil

  # Boolean
  #
  test_decode "Boolean", "true", true
  test_decode "Boolean", "false", false

  # Numeric
  #
  test_decode "Tinyint", "cast(1 as tinyint)", 1
  test_decode "smallint", "cast(2 as smallint)", 2
  test_decode "integer", "cast(3 as integer)", 3
  test_decode "bigint", "cast(4 as bigint)", 4
  test_decode "real", "cast(4.1 as real)", 4.1.to_f32
  test_decode "double", "cast(4.2 as double)", 4.2

  # String
  #
  test_decode "varchar", "cast('thing' as varchar)", "thing"
  test_decode "varchar(6)", "cast('thing2' as varchar(6))", "thing2"
  test_decode "char", "cast('it' as char)", "i"
  test_decode "char(6)", "cast('thing' as char(6))", "thing "
  test_decode "varbinary", "to_big_endian_64(1234)", "AAAAAAAABNI="
  test_decode "json", "cast(ARRAY[1, 2, 3] as JSON)", JSON.parse("[1, 2, 3]")

  # Date and Time
  #
  test_decode "date", "DATE '2001-08-22'", Time.utc(2001, 8, 22)
  test_decode "time", "TIME '01:02:03.456'", Time.utc(1, 1, 1, 1, 2, 3, nanosecond: 456000000)
  test_decode "time with time zone",
    "TIME '01:02:03.456 America/Los_Angeles'",
    Time.local(1, 1, 1, 1, 2, 3, nanosecond: 456000000, location: Time::Location.load("America/Los_Angeles"))
  test_decode "timestamp",
    "TIMESTAMP '2001-08-22 03:04:05.321'", Time.utc(2001, 8, 22, 3, 4, 5, nanosecond: 321000000)
  test_decode "timestamp with time zone",
    "TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'",
    Time.local(2001, 8, 22, 3, 4, 5, nanosecond: 321000000, location: Time::Location.load("America/Los_Angeles"))
  test_decode "year month interval", "INTERVAL '13' MONTH", Time::MonthSpan.new(13)
  test_decode "day second interval", "INTERVAL '26' HOUR", Time::Span.new(days: 1, hours: 2)

end
