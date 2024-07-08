class Yfinrb
end

# frozen_string_literal: true

require_relative 'yfinrb/version'
require_relative 'yfinrb/utils'
require_relative 'yfinrb/yfinance_exception'
require_relative 'yfinrb/yf_connection'
require_relative 'yfinrb/price_technical'
require_relative 'yfinrb/price_history'
require_relative 'yfinrb/quote'
require_relative 'yfinrb/analysis'
require_relative 'yfinrb/fundamentals'
require_relative 'yfinrb/financials'
require_relative 'yfinrb/holders'
require_relative 'yfinrb/ticker'
require_relative "yfinrb/version"

class Yfinrb

  extend Yfinrb::PriceTechnical
end
