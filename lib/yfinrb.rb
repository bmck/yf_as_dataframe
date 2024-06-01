# frozen_string_literal: true

require_relative 'yfin/version'
require_relative 'yfin/yfinance_exception'
require_relative 'yfin/ticker'
require_relative 'yfin/price_history'
require_relative 'yfin/quote'
require_relative 'yfin/analysis'
require_relative 'yfin/fundamentals'
require_relative 'yfin/financials'
require_relative 'yfin/holders'
require_relative "yfinrb/version"

module Yfinrb
  class Error < StandardError; end
  # Your code goes here...
end
