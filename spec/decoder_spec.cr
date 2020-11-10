require "./spec_helper"

describe Presto::Decoder do

  test_decode "Boolean", "true", true
  test_decode "Boolean", "false", false

end
