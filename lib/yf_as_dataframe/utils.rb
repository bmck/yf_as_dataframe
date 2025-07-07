require 'polars-df'

class YfAsDataframe
  class Utils
    BASE_URL = 'https://query1.finance.yahoo.com'

    class << self
      attr_accessor :logger
    end

    def self.get_all_by_isin(isin, proxy: nil, session: nil)
      raise ArgumentError, 'Invalid ISIN number' unless is_isin(isin)

      session ||= Net::HTTP
      url = "#{BASE_URL}/v1/finance/search?q=#{isin}"
      data = session.get(URI(url), 'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36', 'Accept' => 'application/json')
      data = JSON.parse(data.body)
      ticker = data['quotes'][0] || {}
      {
        'ticker' => {
          'symbol' => ticker['symbol'],
          'shortname' => ticker['shortname'],
          'longname' => ticker['longname'],
          'type' => ticker['quoteType'],
          'exchange' => ticker['exchDisp']
        },
        'news' => data['news'] || []
      }
    rescue StandardError
      {}
    end

    def self.get_ticker_by_isin(isin, proxy: nil, session: nil)
      data = get_all_by_isin(isin, proxy: proxy, session: session)
      data.dig('ticker', 'symbol') || ''
    end

    def self.get_info_by_isin(isin, proxy: nil, session: nil)
      data = get_all_by_isin(isin, proxy: proxy, session: session)
      data['ticker'] || {}
    end

    def self.get_news_by_isin(isin, proxy: nil, session: nil)
      data = get_all_by_isin(isin, proxy: proxy, session: session)
      data['news'] || {}
    end

    def self.empty_df(index = nil)
      # index ||= []
      empty = Polars::DataFrame.new({
        'Timestamps' => DateTime.new(2000,1,1,0,0,0),
        'Open' => Float::NAN, 'High' => Float::NAN, 'Low' => Float::NAN,
        'Close' => Float::NAN, 'Adj Close' => Float::NAN, 'Volume' => Float::NAN
      })
      # empty = index.each_with_object({}) { |i, h| h[i] = empty }
      # empty['Date'] = 'Date'
      empty
    end

    def self.empty_earnings_dates_df
      {
        'Symbol' => 'Symbol', 'Company' => 'Company', 'Earnings Date' => 'Earnings Date',
        'EPS Estimate' => 'EPS Estimate', 'Reported EPS' => 'Reported EPS', 'Surprise(%)' => 'Surprise(%)'
      }
    end

    def self.build_template(data)
      template_ttm_order = []
      template_annual_order = []
      template_order = []
      level_detail = []

      def traverse(node, level)
        return if level > 5

        template_ttm_order << "trailing#{node['key']}"
        template_annual_order << "annual#{node['key']}"
        template_order << node['key']
        level_detail << level
        return unless node['children']

        node['children'].each { |child| traverse(child, level + 1) }
      end

      data['template'].each { |key| traverse(key, 0) }

      [template_ttm_order, template_annual_order, template_order, level_detail]
    end

    def self.retrieve_financial_details(data)
      ttm_dicts = []
      annual_dicts = []

      data['timeSeries'].each do |key, timeseries|
        next unless timeseries

        time_series_dict = { 'index' => key }
        timeseries.each do |each|
          next unless each

          time_series_dict[each['asOfDate']] = each['reportedValue']
        end
        if key.include?('trailing')
          ttm_dicts << time_series_dict
        elsif key.include?('annual')
          annual_dicts << time_series_dict
        end
      end

      [ttm_dicts, annual_dicts]
    end

    def self.format_annual_financial_statement(level_detail, annual_dicts, annual_order, ttm_dicts = nil, ttm_order = nil)
      annual = annual_dicts.each_with_object({}) { |d, h| h[d['index']] = d }
      annual = annual_order.each_with_object({}) { |k, h| h[k] = annual[k] }
      annual = annual.transform_keys { |k| k.gsub('annual', '') }

      if ttm_dicts && ttm_order
        ttm = ttm_dicts.each_with_object({}) { |d, h| h[d['index']] = d }
        ttm = ttm_order.each_with_object({}) { |k, h| h[k] = ttm[k] }
        ttm = ttm.transform_keys { |k| k.gsub('trailing', '') }
        statement = annual.merge(ttm)
      else
        statement = annual
      end

      statement = statement.transform_keys { |k| camel2title(k) }
      statement.transform_values { |v| v.transform_keys { |k| camel2title(k) } }
    end

    def self.format_quarterly_financial_statement(statement, level_detail, order)
      statement = order.each_with_object({}) { |k, h| h[k] = statement[k] }
      statement = statement.transform_keys { |k| camel2title(k) }
      statement.transform_values { |v| v.transform_keys { |k| camel2title(k) } }
    end

    def self.camel2title(strings, sep: ' ', acronyms: nil)
      raise TypeError, "camel2title() 'strings' argument must be iterable of strings" unless strings.is_a?(Enumerable)
      raise TypeError, "camel2title() 'strings' argument must be iterable of strings" unless strings.all? { |s| s.is_a?(String) }
      raise ValueError, "camel2title() 'sep' argument = '#{sep}' must be single character" unless sep.is_a?(String) && sep.length == 1
      raise ValueError, "camel2title() 'sep' argument = '#{sep}' cannot be alpha-numeric" if sep.match?(/[a-zA-Z0-9]/)
      raise ValueError, "camel2title() 'sep' argument = '#{sep}' cannot be special character" if sep != Regexp.escape(sep) && !%w[ -].include?(sep)

      if acronyms.nil?
        pat = /([a-z])([A-Z])/
        rep = '\1' + sep + '\2'
        strings.map { |s| s.gsub(pat, rep).capitalize }
      else
        raise TypeError, "camel2title() 'acronyms' argument must be iterable of strings" unless acronyms.is_a?(Enumerable)
        raise TypeError, "camel2title() 'acronyms' argument must be iterable of strings" unless acronyms.all? { |a| a.is_a?(String) }
        acronyms.each do |a|
          raise ValueError, "camel2title() 'acronyms' argument must only contain upper-case, but '#{a}' detected" unless a.match?(/^[A-Z]+$/)
        end

        pat = /([a-z])([A-Z])/
        rep = '\1' + sep + '\2'
        strings = strings.map { |s| s.gsub(pat, rep) }

        acronyms.each do |a|
          pat = /(#{a})([A-Z][a-z])/
          rep = '\1' + sep + '\2'
          strings = strings.map { |s| s.gsub(pat, rep) }
        end

        strings.map do |s|
          s.split(sep).map do |w|
            if acronyms.include?(w)
              w
            else
              w.capitalize
            end
          end.join(sep)
        end
      end
    end

    def self.snake_case_2_camelCase(s)
      s.split('_').first + s.split('_')[1..].map(&:capitalize).join
    end

    # def self.parse_quotes(data)
    #   timestamps = data['timestamp']
    #   ohlc = data['indicators']['quote'][0]
    #   volumes = ohlc['volume']
    #   opens = ohlc['open']
    #   closes = ohlc['close']
    #   lows = ohlc['low']
    #   highs = ohlc['high']

    #   adjclose = closes
    #   adjclose = data['indicators']['adjclose'][0]['adjclose'] if data['indicators']['adjclose']

    #   quotes = {
    #     'Open' => opens,
    #     'High' => highs,
    #     'Low' => lows,
    #     'Close' => closes,
    #     'Adj Close' => adjclose,
    #     'Volume' => volumes
    #   }

    #   quotes.each { |k, v| quotes[k] = v.map { |x| x.nil? ? Float::NAN : x } }
    #   quotes['Date'] = timestamps.map { |x| Time.at(x).to_datetime }

    #   quotes
    # end

    # def self.auto_adjust(data)
    #   ratio = data['Adj Close'] / data['Close']
    #   data['Adj Open'] = data['Open'] * ratio
    #   data['Adj High'] = data['High'] * ratio
    #   data['Adj Low'] = data['Low'] * ratio

    #   data.delete('Open')
    #   data.delete('High')
    #   data.delete('Low')
    #   data.delete('Close')

    #   data['Open'] = data.delete('Adj Open')
    #   data['High'] = data.delete('Adj High')
    #   data['Low'] = data.delete('Adj Low')

    #   data
    # end

    # def self.back_adjust(data)
    #   ratio = data['Adj Close'] / data['Close']
    #   data['Adj Open'] = data['Open'] * ratio
    #   data['Adj High'] = data['High'] * ratio
    #   data['Adj Low'] = data['Low'] * ratio

    #   data.delete('Open')
    #   data.delete('High')
    #   data.delete('Low')
    #   data.delete('Adj Close')

    #   data['Open'] = data.delete('Adj Open')
    #   data['High'] = data.delete('Adj High')
    #   data['Low'] = data.delete('Adj Low')

    #   data
    # end

    def self.is_isin(string)
      /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.match?(string)
    end

    def self.parse_user_dt(dt, exchange_tz)
      if dt.is_a?(Integer)
        return Time.at(dt)
      elsif dt.is_a?(String)
        dt = DateTime.strptime(dt.to_s, '%Y-%m-%d')
      elsif dt.is_a?(Date)
        dt = dt.to_datetime
      end
      # If it's a DateTime, convert to Time
      if dt.is_a?(DateTime)
        # If zone is nil, try to set it, else just convert
        dt = dt.in_time_zone(exchange_tz) if dt.zone.nil? && dt.respond_to?(:in_time_zone)
        dt = dt.to_time
      end
      dt.to_i
    end

    def self.interval_to_timedelta(interval)
      case interval
      when '1mo'
        # Calculate 1 month from now to get accurate days
        now = Time.now
        next_month = Time.new(now.year, now.month + 1, now.day)
        # Handle year rollover
        next_month = Time.new(now.year + 1, 1, now.day) if next_month.month == 1
        (next_month - now).to_i
      when '2mo'
        # Calculate 2 months from now
        now = Time.now
        next_month = Time.new(now.year, now.month + 2, now.day)
        # Handle year rollover
        next_month = Time.new(now.year + 1, next_month.month, now.day) if next_month.month <= 2
        (next_month - now).to_i
      when '3mo'
        # Calculate 3 months from now
        now = Time.now
        next_month = Time.new(now.year, now.month + 3, now.day)
        # Handle year rollover
        next_month = Time.new(now.year + 1, next_month.month, now.day) if next_month.month <= 3
        (next_month - now).to_i
      when '6mo'
        # Calculate 6 months from now
        now = Time.now
        next_month = Time.new(now.year, now.month + 6, now.day)
        # Handle year rollover
        next_month = Time.new(now.year + 1, next_month.month, now.day) if next_month.month <= 6
        (next_month - now).to_i
      when '9mo'
        # Calculate 9 months from now
        now = Time.now
        next_month = Time.new(now.year, now.month + 9, now.day)
        # Handle year rollover
        next_month = Time.new(now.year + 1, next_month.month, now.day) if next_month.month <= 9
        (next_month - now).to_i
      when '12mo'
        # Calculate 12 months (1 year) from now
        now = Time.now
        next_year = Time.new(now.year + 1, now.month, now.day)
        (next_year - now).to_i
      when '1y'
        # Calculate 1 year from now
        now = Time.now
        next_year = Time.new(now.year + 1, now.month, now.day)
        (next_year - now).to_i
      when '2y'
        # Calculate 2 years from now
        now = Time.now
        next_year = Time.new(now.year + 2, now.month, now.day)
        (next_year - now).to_i
      when '3y'
        # Calculate 3 years from now
        now = Time.now
        next_year = Time.new(now.year + 3, now.month, now.day)
        (next_year - now).to_i
      when '4y'
        # Calculate 4 years from now
        now = Time.now
        next_year = Time.new(now.year + 4, now.month, now.day)
        (next_year - now).to_i
      when '5y'
        # Calculate 5 years from now
        now = Time.now
        next_year = Time.new(now.year + 5, now.month, now.day)
        (next_year - now).to_i
      when '1wk'
        7.days
      when '2wk'
        14.days
      when '3wk'
        21.days
      when '4wk'
        28.days
      else
        # Logger.new(STDOUT).warn { "#{__FILE__}:#{__LINE__} #{interval} not a recognized interval" }
        interval
      end
    end

    # def _interval_to_timedelta(interval)
    #   if interval == "1mo"
    #     return ActiveSupport::Duration.new(months: 1)
    #   elsif interval == "3mo"
    #     return ActiveSupport::Duration.new(months: 3)
    #   elsif interval == "1y"
    #     return ActiveSupport::Duration.new(years: 1)
    #   elsif interval == "1wk"
    #     return 7.days
    #   else
    #     return ActiveSupport::Duration.parse(interval)
    #   end
    # end    
  end
end

# module Yfin
#   class << self
#     attr_accessor :logger
#   end

#   self.logger = Logger.new(STDOUT)
#   self.logger.level = Logger::WARN
# end

def attributes(obj)
  disallowed_names = Set.new(obj.class.instance_methods(false).map(&:to_s))
  obj.instance_variables.each_with_object({}) do |var, h|
    name = var.to_s[1..]
    next if name.start_with?('_') || disallowed_names.include?(name)

    h[name] = obj.instance_variable_get(var)
  end
end

def print_once(msg)
  puts msg
end

def get_yf_logger
  # Yfin.logger
  Rails.logger
end

def setup_debug_formatting
  logger = get_yf_logger

  return unless logger.level == Logger::DEBUG

  logger.formatter = MultiLineFormatter.new('%(levelname)-8s %(message)s')
end

def enable_debug_mode
  Rails.logger.level = Logger::DEBUG
  setup_debug_formatting
end
