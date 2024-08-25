class YfAsDataframe
end

# frozen_string_literal: true

require_relative 'yf_as_dataframe/version'
require_relative 'yf_as_dataframe/utils'
require_relative 'yf_as_dataframe/yfinance_exception'
require_relative 'yf_as_dataframe/yf_connection'
require_relative 'yf_as_dataframe/price_technical'
require_relative 'yf_as_dataframe/price_history'
require_relative 'yf_as_dataframe/quote'
require_relative 'yf_as_dataframe/analysis'
require_relative 'yf_as_dataframe/fundamentals'
require_relative 'yf_as_dataframe/financials'
require_relative 'yf_as_dataframe/holders'
require_relative 'yf_as_dataframe/ticker'
require_relative "yf_as_dataframe/version"

class YfAsDataframe

  extend YfAsDataframe::PriceTechnical
end

