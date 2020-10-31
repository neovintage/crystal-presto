require "http/client"

module Presto
  module PrestoHeaders
    PRESTO_USER   = "X-Presto-User"
    PRESTO_SOURCE = "X-Presto-Source"
  end

  # todo this is for doing operations outside of the standard crystal db interface
  class Client
  end
end
