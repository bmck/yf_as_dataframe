require 'polars'
require 'polars-df'

class Yfinrb
  module PriceHistory
    extend ActiveSupport::Concern
    include ActionView::Helpers::NumberHelper

    PRICE_COLNAMES = ['Open', 'High', 'Low', 'Close', 'Adj Close']
    BASE_URL = 'https://query2.finance.yahoo.com'

    # attr_accessor :ticker

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_price_history # (your module code here)
        end
      end
    end

    def initialize_price_history #(ticker)
      # ticker = ticker

      @history = nil
      @history_metadata = nil
      @history_metadata_formatted = false
      @reconstruct_start_interval = nil

      yfconn_initialize
    end

    def history(period: "1mo", interval: "1d", start: nil, fin: nil, prepost: false,
                actions: true, auto_adjust: true, back_adjust: false, repair: false, keepna: false,
                rounding: false, raise_errors: false, returns: false)
      logger = Rails.logger # Yfin.get_yf_logger
      start_user = start
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here" }
      end_user = fin || DateTime.now

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here" }
      params = _preprocess_params(start, fin, interval, period, prepost, raise_errors)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} params=#{params.inspect}" }

      params_pretty = params.dup

      ["period1", "period2"].each do |k|
        params_pretty[k] = DateTime.strptime(params[k].to_s, '%s').new_offset(0).to_time.strftime('%Y-%m-%d %H:%M:%S %z') if params_pretty.key?(k)
      end

      data = _get_data(ticker, params, fin, raise_errors)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data[chart][result].first.keys = #{data['chart']['result'].first.keys.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data[chart][result].first[events] = #{data['chart']['result'].first['events'].inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data[chart][result].first[events][dividends] = #{data['chart']['result'].first['events']['dividends'].inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data[chart][result].first[events][splits] = #{data['chart']['result'].first['events']['splits'].inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history = #{@history.inspect}" }

      @history_metadata = data["chart"]["result"][0]["meta"] rescue {}
      @history = data["chart"]["result"][0]

      intraday = params["interval"][-1] == "m" || params["interval"][-1] == "h"

      err_msg = _get_err_msg(params['period1'], period, start, params['period2'], fin, params['interval'], params['intraday'])
      # err_msg = _get_err_msg(start, period, start_user, fin, end_user, interval, intraday)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} err_msg = #{err_msg}" }

      f = _did_it_fail(data, period, @history_metadata)
      failed = f[:fail]
      err_msg = f[:msg]

      if failed
        if raise_errors
          raise Exception.new("#{ticker}: #{err_msg}")
        else
          logger.error("#{ticker}: #{err_msg}")
        end
        if @reconstruct_start_interval && @reconstruct_start_interval == interval
          @reconstruct_start_interval = nil
        end
        return Yfinrb::Utils.empty_df
      end

      # begin
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data[chart][result][0] = #{data["chart"]["result"][0].inspect}" }
      quotes = _parse_quotes(data["chart"]["result"][0], interval)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history = #{@history.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}" }

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} quotes=#{quotes.inspect}" }
      # if fin && !quotes.empty?
      #   endDt = fin.to_datetime.to_i # DateTime.strptime(fin.to_s, '%s').new_offset(0)
      #   if quotes.index[quotes.shape[0] - 1] >= endDt
      #     quotes = quotes[0..quotes.shape[0] - 2]
      #   end
      # end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here" }
      # rescue Exception
      #   if raise_errors
      #     raise Exception.new("#{ticker}: #{err_msg}")
      #   else
      #     logger.error("#{ticker}: #{err_msg}")
      #   end
      #   if @reconstruct_start_interval && @reconstruct_start_interval == interval
      #     @reconstruct_start_interval = nil
      #   end
      #   return nil
      # end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here" }
      quote_type = @history_metadata["instrumentType"]
      expect_capital_gains = quote_type == 'MUTUALFUND' || quote_type == 'ETF'
      tz_exchange = @history_metadata["exchangeTimezoneName"]

      quotes = _set_df_tz(quotes, params["interval"], tz_exchange)
      quotes = _fix_yahoo_dst_issue(quotes, params["interval"])
      quotes = _fix_yahoo_returning_live_separate(quotes, params["interval"], tz_exchange)

      intraday = params["interval"][-1] == "m" || params["interval"][-1] == "h"

      if !prepost && intraday && @history_metadata.key?("tradingPeriods")
        tps = @history_metadata["tradingPeriods"]
        if !tps.is_a?(Polars::DataFrame)
          @history_metadata = _format_history_metadata(@history_metadata, tradingPeriodsOnly: true)
          tps = @history_metadata["tradingPeriods"]
        end
        quotes = _fix_yahoo_returning_prepost_unrequested(quotes, params["interval"], tps)
      end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} quotes = #{quotes.inspect}" }
      df = _get_stock_data(quotes, params, fin)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

      if repair
        #   df = _fix_unit_mixups(df, interval, tz_exchange, prepost)
        #   df = _fix_bad_stock_split(df, interval, tz_exchange)
        #   df = _fix_zeroes(df, interval, tz_exchange, prepost)
        #   df = _fix_missing_div_adjust(df, interval, tz_exchange)
        #   df = df.sort_index
      end

      if auto_adjust
        #   df = _auto_adjust(df)
      elsif back_adjust
        #   df = _back_adjust(df)
      end

      if rounding
        # df = df.round(data["chart"]["result"][0]["meta"]["priceHint"])
      end

      df["Volume"] = df["Volume"].fill_nan(0) #.astype(Integer)

      # df.index.name = intraday ? "Datetime" : "Date"
      # [0..df['Timestamps'].length-2].each{|i| df['Timestamps'][i] = df['Timestamps'][i].round("1d") } unless intraday
      unless intraday
        s = Polars::Series.new(df['Timestamps']).to_a
        df['Timestamps'] = (0..s.length-1).to_a.map{|i| Time.at(s[i]).to_date }
      end

      @history = df.dup

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} actions = #{actions}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history = #{@history.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      df = df.drop(["Dividends", "Stock Splits", "Capital Gains"], errors: 'ignore') unless actions

      if !keepna
          # price_colnames = ['Open', 'High', 'Low', 'Close', 'Adj Close']
          # data_colnames = price_colnames + ['Volume'] + ['Dividends', 'Stock Splits', 'Capital Gains']
          # data_colnames = data_colnames.select { |c| df.columns.include?(c) }
          # mask_nan_or_zero = (df[data_colnames].isnan? | (df[data_colnames] == 0)).all(axis: 1)
          # df = df.drop(mask_nan_or_zero.index[mask_nan_or_zero])
      end

      # logger.debug("#{ticker}: yfinance returning OHLC: #{df.index[0]} -> #{df.index[-1]}")

      @reconstruct_start_interval = nil if @reconstruct_start_interval && @reconstruct_start_interval == interval

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df.rows = #{df.rows}" }
      if returns && df.shape.first > 1
        df['Returns'] = [Float::NAN] + (1..df.length-1).to_a.map {|i| (df['Close'][i]-df['Close'][i-1])/df['Close'][i-1] }
      end
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

      return df
    end


    def history_metadata
      history(period: "1wk", interval: "1h", prepost: true) if @history_metadata.nil?

      if !@history_metadata_formatted
        @history_metadata = _format_history_metadata(@history_metadata)
        @history_metadata_formatted = true
      end
      return @history_metadata
    end

    def exchange
      return @exchange ||= _get_exchange_metadata["exchangeName"]
    end

    def timezone
      return @timezone ||= _get_exchange_metadata["exchangeTimezoneName"]
    end

    def dividends
      history(period: "max") if @history.nil?

      if !@history.nil? # && @history['events'].keys.include?("dividends")
        df = @history.dup.drop('Open','High','Low','Close','Adj Close', 'Volume','Stock Splits','Capital Gains')
        return df.filter(Polars.col('Dividends')>0.0)
        # divi = []
        # @history['events']["dividends"].each_pair {|k,v| divi << { Timestamps: Time.at(k.to_i).utc.to_date, Value: v['amount']} }
        # return Polars::DataFrame.new( divi )
      end
      return Polars::Series.new
    end

    def capital_gains
      history(period: "max") if @history.nil?

      if !@history.nil? # && @history['events'].keys.include?("capital gains")
        # caga = []
        # @history['events']['capital gains'].each_pair {|k,v| caga << { Timestamps: Time.at(k).utc.to_date, Value: v['amount']} }
        # capital_gains = @history["Capital Gains"]
        # return capital_gains[capital_gains != 0]
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history = #{@history.inspect}" }
        df = @history.dup.drop('Open','High','Low','Close','Adj Close', 'Volume','Stock Splits', 'Dividends')
        return df.filter(Polars.col('Capital Gains')>0.0)
      end
      return Polars::Series.new
    end

    def splits
      history(period: "max") if @history.nil?

      if !@history.nil?  #&& @history['events'].keys.include?("stock splits") # @history.columns.include?("Stock Splits")
        # stspl = []
        # @history['events']['stock splits'].each_pair {|k,v| stspl << { Timestamps: Time.at(k.to_i).utc.to_date, Ratio: v['numerator'].to_f/v['denominator'].to_f } }

        # splits = @history["Stock Splits"]
        # return splits[splits != 0]
        df = @history.dup.drop('Open','High','Low','Close','Adj Close', 'Volume','Capital Gains','Dividends')
        return df.filter(Polars.col('Stock Splits')>0.0) #Polars::DataFrame.new(stspl)
      end
      return Polars::Series.new
    end

    def actions
      history(period: "max") if @history.nil?

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history = #{@history.inspect}" }
      if !@history.nil? #&& @history.columns.include?("Dividends") && @history.columns.include?("Stock Splits")
        # action_columns = ["Dividends", "Stock Splits"]

        # action_columns.append("Capital Gains") if @history.columns.include?("Capital Gains")

        # actions = @history[action_columns]
        # return actions[actions != 0].dropna(how: 'all').fillna(0)
        df = @history.dup.drop('Open','High','Low','Close','Adj Close', 'Volume')
        return df.filter((Polars.col('Stock Splits')>0.0) | (Polars.col('Dividends')>0.0) | (Polars.col('Capital Gains')>0.0)) #Polars::DataFrame.new(stspl)
      end
      return Polars::Series.new
    end

    def currency
      if @currency.nil?

        md = history_metadata #(proxy=self.proxy)
        @currency = md["currency"]
      end
      return @currency
    end

    def quote_type
      if @quote_type.nil?

        md = history_metadata #(proxy=self.proxy)
        @quote_type = md["instrumentType"]
      end
      return @quote_type
    end

    def last_price
      return @last_price unless @last_price.nil?

      prices = _get_1y_prices

      if prices.empty?
        @md ||= _get_exchange_metadata
        @last_price = md["regularMarketPrice"] if "regularMarketPrice".in?(@md)

      else
        @last_price = (prices["Close"][-1]).to_f
        if @last_price.nan?
          @md ||= _get_exchange_metadata
          @last_price = md["regularMarketPrice"] if "regularMarketPrice".in?(@md)
        end
      end

      return @last_price
    end

    def previous_close
      return @prev_close unless @prev_close.nil?

      prices = _get_1wk_1h_prepost_prices

      fail = prices.empty?
      prices = fail ? prices : prices[["Close"]].groupby('Timestamps', maintain_order: true).agg([Polars.col("Close")]).to_f 

      # Very few symbols have previousClose despite no
      # no trading data e.g. 'QCSTIX'.
      fail = prices.shape.first < 2
      @prev_close = fail ? nil : (prices["Close"][-2]).to_f

      # if fail
      #   # Fallback to original info[] if available.
      #   info  # trigger fetch
      #   k = "previousClose"
      #   @prev_close = _quote._retired_info[k] if !_quote._retired_info.nil? && k.in?(_quote._retired_info)
      # end
      return @prev_close
    end

    def regular_market_previous_close
      return @reg_prev_close unless @reg_prev_close.nil?

      prices = _get_1y_prices
      if prices.shape[0] == 1
        # Tiny % of tickers don't return daily history before last trading day,
        # so backup option is hourly history:
        prices = _get_1wk_1h_reg_prices
        prices = prices[["Close"]].groupby(prices.index.date).last
      end

      # if prices.shape[0] < 2
      #   # Very few symbols have regularMarketPreviousClose despite no
      #   # no trading data. E.g. 'QCSTIX'.
      #   # So fallback to original info[] if available.
      #   info  # trigger fetch
      #   k = "regularMarketPreviousClose"
      #   @reg_prev_close = _quote._retired_info[k] if !_quote._retired_info.nil? && k.in?(_quote._retired_info)

      # else
      #   @reg_prev_close = float(prices["Close"].iloc[-2])
      # end

      return @reg_prev_close
    end

    def open
      return @open unless @open.nil?

      prices = _get_1y_prices
      if prices.empty
        @open = nil

      else
        @open = (prices["Open"][-1])
        @open = nil if @open.nan?
      end

      return @open
    end

    def day_high
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @day_high = #{@day_high}" }
      return @day_high unless @day_high.nil?

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @day_high = #{@day_high}" }
      prices = _get_1y_prices
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} prices = #{prices.inspect}" }
      # if prices.empty?
      #   @day_high = nil

      # else
      @day_high = (prices["High"][-1])
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @day_high = #{@day_high}" }
      @day_high = nil if @day_high.nan?
      # end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} @day_high = #{@day_high}" }
      return @day_high
    end

    def day_low
      return @day_low unless @day_low.nil?

      prices = _get_1y_prices
      if prices.empty?
        @day_low = nil

      else
        @day_low = (prices["Low"][-1])
        @day_low = nil if @day_low.nan?
      end

      return @day_low
    end

    def last_volume
      return @last_volume unless @last_volume.nil?

      prices = _get_1y_prices
      @last_volume = prices.empty? ? nil : (prices["Volume"][-1])
      return @last_volume
    end

    def fifty_day_average
      return @_50d_day_average unless @_50d_day_average.nil?

      prices = _get_1y_prices(fullDaysOnly=true)
      if prices.empty?
        @_50d_day_average = nil

      else
        n = prices.shape.first
        a = n-50
        b = n
        a = 0 if a < 0

        @_50d_day_average = (prices["Close"][a..b].mean)
      end

      return @_50d_day_average
    end

    def two_hundred_day_average
      return @_200d_day_average unless @_200d_day_average.nil?

      prices = _get_1y_prices(fullDaysOnly=true)
      if prices.empty?
        @_200d_day_average = nil

      else
        n = prices.shape[0]
        a = n-200
        b = n
        a = 0 if a < 0

        @_200d_day_average = (prices["Close"][a..b].mean)
      end

      return @_200d_day_average
    end

    def ten_day_average_volume
      return @_10d_avg_vol unless @_10d_avg_vol.nil?

      prices = _get_1y_prices(fullDaysOnly=true)
      if prices.empty?
        @_10d_avg_vol = nil

      else
        n = prices.shape[0]
        a = n-10
        b = n
        a = 0 if a < 0

        @_10d_avg_vol = (prices["Volume"][a..b].mean)

      end
      return @_10d_avg_vol
    end

    def three_month_average_volume
      return @_3mo_avg_vol unless @_3mo_avg_vol.nil?

      prices = _get_1y_prices(fullDaysOnly=true)
      if prices.empty
        @_3mo_avg_vol = nil

      else
        dt1 = prices.index[-1]
        dt0 = dt1 - 3.months + 1.day
        @_3mo_avg_vol = (prices[dt0..dt1]["Volume"].mean)
      end

      return @_3mo_avg_vol
    end

    def year_high
      if @year_high.nil?
        prices = _get_1y_prices(fullDaysOnly=true)
        prices = _get_1y_prices(fullDaysOnly=false) if prices.empty?

        @year_high = (prices["High"].max)
      end
      return @year_high
    end

    def year_low
      if @year_low.nil?
        prices = _get_1y_prices(fullDaysOnly=true)
        prices = _get_1y_prices(fullDaysOnly=false) if prices.empty?

        @year_low = (prices["Low"].min)
      end
      return @year_low
    end

    def year_change
      if @year_change.nil?
        prices = _get_1y_prices(fullDaysOnly=true)
        @year_change = (prices["Close"][-1] - prices["Close"][0]) / prices["Close"][0] if prices.shape[0] >= 2
      end
      return @year_change
    end

    def market_cap
      return @mcap unless @mcap.nil?

      begin
        # shares = self.shares
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} shares = #{shares}" }
        sh = shares
        lp = last_price
        @mcap = shares * last_price
        # @mcap = 'US$' + number_to_human((shares * last_price), precision: 4)
      rescue Exception => e
        if "Cannot retrieve share count".in?(e.message) || "failed to decrypt Yahoo".in?(e.message)
          shares = nil
        else
          raise
        end

        # if shares.nil?
        #   # Very few symbols have marketCap despite no share count.
        #   # E.g. 'BTC-USD'
        #   # So fallback to original info[] if available.
        #   info
        #   k = "marketCap"
        #   @mcap = _quote._retired_info[k] if !_quote._retired_info.nil? && k.in?(_quote._retired_info)

        # else
        #   @mcap = float(shares * self.last_price)
        # end

        return nil #@mcap
      end
    end

    # price_history_methods = [:get_history_metadata, :get_dividends, :get_capital_gains, \
    #                          :get_splits, :get_actions]
    # price_history_methods.each { |meth| alias_method meth.to_s.gsub(/^get_/, '').to_sym, meth }







    private

    def _preprocess_params(start, fin, interval, period, prepost, raise_errors)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start.inspect}, end_date = #{fin.inspect}, interval = #{interval}, period = #{period}, tz = #{tz}, prepost = #{prepost}, raise_errors = #{raise_errors}" }

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here start = #{fin}, period = #{period}" } 
      if start || period.nil? || period.downcase == "max"
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} here fin = #{fin}" } 
        if tz.nil?
          err_msg = "No timezone found, symbol may be delisted"
          # Yfin.shared_DFS[@ticker] = Yfinrb::Utils.empty_df
          # Yfin.shared_ERRORS[@ticker] = err_msg
          if raise_errors
            raise Exception.new("#{@ticker}: #{err_msg}")
          else
            Rails.logger.error("#{@ticker}: #{err_msg}")
          end
          return Yfinrb::Utils.empty_df
        end

        # Rails.logger.info { "#{__FILE__}:#{__LINE__} here fin = #{fin}" } 
        fin = fin.nil? ? Time.now.to_i : Yfinrb::Utils.parse_user_dt(fin, tz)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} fin = #{fin.inspect}" }

        if start.nil?
          if interval == "1m"
            start = (fin - 1.week).to_i
          else
            max_start_datetime = (DateTime.now - (99.years)).to_i
            start = max_start_datetime.to_i
          end
        else
          start = Yfinrb::Utils.parse_user_dt(start, tz)
        end

        params = { "period1" => start, "period2" => fin }
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} params = #{params.inspect}" }

      else
        period = period.downcase
        # params = { "range" => period }
        fin = DateTime.now.to_i
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} here fin= #{fin}, period = #{period}" } 
        start = (fin - Yfinrb::Utils.interval_to_timedelta(period)).to_i
        params = { "period1" => start, "period2" => fin }
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} params = #{params.inspect}" }
      end

      params["interval"] = interval.downcase
      params["includePrePost"] = prepost
      params["interval"] = "15m" if params["interval"] == "30m"
      params["events"] = "div,splits,capitalGains"

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} params = #{params.inspect}" }
      return params
    end

    def _get_data(ticker, params, fin, raise_errors)
      url = "https://query2.finance.yahoo.com/v8/finance/chart/#{ticker}"
      # url = "https://query1.finance.yahoo.com/v7/finance/download/#{ticker}" ... Deprecated
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} url = #{url}" }
      data = nil
      # get_fn = @data.method(:get)

      if fin
        end_dt = DateTime.strptime(fin.to_s, '%s') #.new_offset(0)
        dt_now = DateTime.now #.new_offset(0)
        data_delay = Rational(30, 24 * 60)

        # get_fn = @data.method(:cache_get) if end_dt + data_delay <= dt_now
      end

      begin
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} url = #{url}, params = #{params.inspect}" }
        data = get(url, nil, params).parsed_response
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}" }

        raise RuntimeError.new(
          "*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n" +
          "Our engineers are working quickly to resolve the issue. Thank you for your patience."
        ) if data.text.include?("Will be right back") || data.nil?

        data = HashWithIndifferentAccess.new(data)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}" }
      rescue Exception
        raise if raise_errors
      end

      data
    end

    def _get_err_msg(start, period, start_user, fin, end_user, interval, intraday)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start}, period = #{period}, start_user = #{start_user}, fin = #{fin}, end_user = #{end_user}, interval = #{interval}, intraday = #{intraday}"}
      err_msg = "No price data found, symbol may be delisted"

      if start.nil? || period.nil? || period.downcase == "max"
        err_msg += " (#{interval} "

        if start_user
          err_msg += "#{start_user}"
        elsif !intraday
          # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start}" }
          err_msg += "#{(Time.at(start).to_date).strftime('%Y-%m-%d')}"
        else
          err_msg += "#{Time.at(start).strftime('%Y-%m-%d %H:%M:%S %z')}"
        end

        err_msg += " -> "

        if end_user
          err_msg += "#{end_user})"
        elsif !intraday
          err_msg += "#{(Time.at(fin).to_date).strftime('%Y-%m-%d')})"
        else
          err_msg += "#{Time.at(fin).strftime('%Y-%m-%d %H:%M:%S %z')})"
        end
      else
        err_msg += " (period=#{period})"
      end
      err_msg
    end

    def _did_it_fail(data, period, hist_metadata)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}, period = #{period}, hist_metadata = #{hist_metadata.inspect}" }
      failed = false

      if data.nil? || !data.is_a?(Hash)
        failed = true
      elsif data.is_a?(Hash) && data.key?("status_code")
        err_msg += "(yahoo status_code = #{data['status_code']})"
        failed = true
      elsif data["chart"].nil? || data["chart"]["error"]
        err_msg = data["chart"]["error"]["description"]
        failed = true
      elsif data["chart"].nil? || data["chart"]["result"].nil? || !data["chart"]["result"]
        failed = true
      elsif period && !data["chart"]["result"][0].key?("timestamp") && !hist_metadata["validRanges"].include?(period)
        err_msg = "Period '#{period}' is invalid, must be one of #{hist_metadata['validRanges']}"
        failed = true
      end

      {fail: failed, msg: err_msg}
    end

    def _get_stock_data(quotes, params, fin = nil)
      df = quotes #.sort_index
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      ts = Polars::Series.new(df['Timestamps']).to_a

      if quotes.shape.first > 0
        # startDt = quotes.index[0].floor('D')
        startDt = quotes['Timestamps'].to_a.map(&:to_date).min
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} startDt = #{startDt.inspect}" }
        endDt = fin.present? ? fin : Time.at(DateTime.now.tomorrow).to_i

        # Rails.logger.info { "#{__FILE__}:#{__LINE__} @history[events][dividends] = #{@history['events']["dividends"].inspect}" }
        # divi = {}
        # @history['events']["dividends"].select{|k,v| 
        #   Time.at(k.to_i).utc.to_date >= startDt && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
        #     divi['date'] = v['amount']} unless @history.try(:[],'events').try(:[],"dividends").nil?
        d = [0.0] * df.length
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} df.length = #{df.length}" }
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} ts = #{ts.inspect}" }
        @history['events']["dividends"].select{|k,v| 
          Time.at(k.to_i).utc.to_date >= startDt && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
            d[ts.index(Time.at(k.to_i).utc)] = v['amount'].to_f} unless @history.try(:[],'events').try(:[],"dividends").nil?
        df['Dividends'] = Polars::Series.new(d)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

        # caga = {}
        # @history['events']["capital gains"].select{|k,v| 
        #   Time.at(k.to_i).utc.to_date >= startDt  && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
        #     caga['date'] = v['amount']} unless @history.try(:[],'events').try(:[],"capital gains").nil?
        # capital_gains = capital_gains.loc[startDt:] if capital_gains.shape.first > 0
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} caga = #{caga.inspect}" }
        d = [0.0] * df.length
        @history['events']["capital gains"].select{|k,v| 
          Time.at(k.to_i).utc.to_date >= startDt  && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
            d[ts.index(Time.at(k.to_i).utc)] = v['amount'].to_f} unless @history.try(:[],'events').try(:[],"capital gains").nil?
        df['Capital Gains'] = Polars::Series.new(d)

        # splits = splits.loc[startDt:] if splits.shape[0] > 0
        # stspl = {}
        # @history['events']['stock splits'].select{|k,v| 
        #   Time.at(k.to_i).utc.to_date >= startDt  && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
        #     stspl['date'] = v['numerator'].to_f/v['denominator'].to_f} unless @history.try(:[],'events').try(:[],"stock splits").nil?
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} stspl = #{stspl.inspect}" }
        d = [0.0] * df.length
        @history['events']["capital gains"].select{|k,v| 
          Time.at(k.to_i).utc.to_date >= startDt  && Time.at(k.to_i).utc.to_date <= endDt }.each{|k,v| 
            d[ts.index(Time.at(k.to_i).utc)] = v['numerator'].to_f/v['denominator'].to_f} unless @history.try(:[],'events').try(:[],"capital gains").nil?
        df['Stock Splits'] = Polars::Series.new(d)
      end

      # intraday = params["interval"][-1] == "m" || params["interval"][-1] == "h"

      # if !intraday
      #   quotes.index = quotes.index.map { |i| DateTime.strptime(i.to_s, '%s').new_offset(tz).to_time }

      #   dividends.index = \
      #     dividends.index.map { |i| DateTime.strptime(i.to_s, '%s').new_offset(tz).to_time } if dividends.shape[0] > 0

      #   splits.index = \
      #     splits.index.map { |i| DateTime.strptime(i.to_s, '%s').new_offset(tz).to_time } if splits.shape[0] > 0

      # end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} quotes = #{quotes.inspect}" }
      # df = quotes
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

      # df = _safe_merge_dfs(df, dividends, interval) if dividends.shape[0] > 0
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }


      # if df.columns.include?("Dividends")
      #   df.loc[df["Dividends"].isna?, "Dividends"] = 0
      # else
      #   df["Dividends"] = 0.0
      # end
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.filter(Polars.col("Dividends") > 0.0)}" }

      # df = _safe_merge_dfs(df, splits, interval) if splits.shape[0] > 0
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }


      # if df.columns.include?("Stock Splits")
      #   df.loc[df["Stock Splits"].isna?, "Stock Splits"] = 0
      # else
      #   df["Stock Splits"] = 0.0
      # end
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

      # if expect_capital_gains

      #   df = _safe_merge_dfs(df, capital_gains, interval) if capital_gains.shape[0] > 0
      #   Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }

      #   if df.columns.include?("Capital Gains")
      #     df.loc[df["Capital Gains"].isna?, "Capital Gains"] = 0
      #   else
      #     df["Capital Gains"] = 0.0
      #   end
      # end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      # df = df[~df.index.duplicated(keep: 'first')]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}" }
      df
    end

    def _auto_adjust(data)
      col_order = data.columns
      df = data.dup
      ratio = (df["Adj Close"] / df["Close"]).to_a
      df["Adj Open"] = df["Open"] * ratio
      df["Adj High"] = df["High"] * ratio
      df["Adj Low"] = df["Low"] * ratio

      df.drop(
        ["Open", "High", "Low", "Close"],
      axis: 1, inplace: true)

      df.rename(columns: {
                  "Adj Open": "Open", "Adj High": "High",
                  "Adj Low": "Low", "Adj Close": "Close"
      }, inplace: true)

      return df
    end

    def _back_adjust(data)
      col_order = data.columns
      df = data.dup
      ratio = df["Adj Close"] / df["Close"]
      df["Adj Open"] = df["Open"] * ratio
      df["Adj High"] = df["High"] * ratio
      df["Adj Low"] = df["Low"] * ratio

      df.drop(
        ["Open", "High", "Low", "Adj Close"],
      axis: 1, inplace: true)

      df.rename(columns: {
                  "Adj Open": "Open", "Adj High": "High",
                  "Adj Low": "Low"
      }, inplace: true)

      return df
    end

    def _set_df_tz(df, interval, tz)

      # df.index = df.index.tz_localize("UTC") if df.index.tz.nil?

      # df.index = df.index.tz_convert(tz)
      return df
    end

    def _fix_yahoo_dst_issue(df, interval)
      # if interval.in?(["1d", "1w", "1wk"])
      #   f_pre_midnight = (df.index.minute == 0) & (df.index.hour.in?([22, 23]))
      #   dst_error_hours = [0] * df.shape[0]
      #   dst_error_hours[f_pre_midnight] = 24 - df.index[f_pre_midnight].hour
      #   df.index += dst_error_hours.map { |h| ActiveSupport::Duration.new(hours: h) }
      # end
      return df
    end

    def _fix_yahoo_returning_live_separate(quotes, interval, tz_exchange)
      n = quotes.shape[0]
      # if n > 1
      #   dt1 = quotes['Timestamps'][n - 1]
      #   dt2 = quotes['Timestamps'][n - 2]
      #   if quotes['Timestamps'].tz.nil?
      #     dt1 = dt1.tz_localize("UTC")
      #     dt2 = dt2.tz_localize("UTC")
      #   end
      #   dt1 = dt1.tz_convert(tz_exchange)
      #   dt2 = dt2.tz_convert(tz_exchange)

      #   if interval == "1d"
      #     quotes = quotes.drop(quotes.index[n - 2]) if dt1.to_date == dt2.to_date

      #   else
      #     if interval == "1wk"
      #       last_rows_same_interval = dt1.year == dt2.year && dt1.cweek == dt2.cweek
      #     elsif interval == "1mo"
      #       last_rows_same_interval = dt1.month == dt2.month
      #     elsif interval == "3mo"
      #       last_rows_same_interval = dt1.year == dt2.year && dt1.quarter == dt2.quarter
      #     else
      #       last_rows_same_interval = (dt1 - dt2) < ActiveSupport::Duration.parse(interval)
      #     end

      #     if last_rows_same_interval
      #       idx1 = quotes.index[n - 1]
      #       idx2 = quotes.index[n - 2]

      #       return quotes if idx1 == idx2

      #       quotes.loc[idx2, "Open"] = quotes["Open"].iloc[n - 1] if quotes.loc[idx2, "Open"].nan?

      #       if !quotes["High"].iloc[n - 1].nan?
      #         quotes.loc[idx2, "High"] = [quotes["High"].iloc[n - 1], quotes["High"].iloc[n - 2]].max
      #         if quotes.columns.include?("Adj High")
      #           quotes.loc[idx2, "Adj High"] = [quotes["Adj High"].iloc[n - 1], quotes["Adj High"].iloc[n - 2]].max
      #         end
      #       end
      #       if !quotes["Low"].iloc[n - 1].nan?
      #         quotes.loc[idx2, "Low"] = [quotes["Low"].iloc[n - 1], quotes["Low"].iloc[n - 2]].min
      #         if quotes.columns.include?("Adj Low")
      #           quotes.loc[idx2, "Adj Low"] = [quotes["Adj Low"].iloc[n - 1], quotes["Adj Low"].iloc[n - 2]].min
      #         end
      #       end
      #       quotes.loc[idx2, "Close"] = quotes["Close"].iloc[n - 1]
      #       if quotes.columns.include?("Adj Close")
      #         quotes.loc[idx2, "Adj Close"] = quotes["Adj Close"].iloc[n - 1]
      #       end
      #       quotes.loc[idx2, "Volume"] += quotes["Volume"].iloc[n - 1]
      #       quotes = quotes.drop(quotes.index[n - 1])
      #     end
      #   end
      # end
      return quotes
    end

    def _fix_yahoo_returning_prepost_unrequested(quotes, interval, tradingPeriods)
      tps_df = tradingPeriods.dup
      tps_df["_date"] = tps_df.index.map(&:to_date)
      quotes["_date"] = quotes.index.map(&:to_date)
      idx = quotes.index.dup
      quotes = quotes.merge(tps_df, how: "left")
      quotes.index = idx
      f_drop = quotes.index >= quotes["end"]
      f_drop = f_drop | (quotes.index < quotes["start"])
      if f_drop.any?
        quotes = quotes[~f_drop]
      end
      quotes = quotes.drop(["_date", "start", "end"], axis: 1)
      return quotes
    end

    def _format_history_metadata(md, tradingPeriodsOnly = true)
      return md unless md.is_a?(Hash)
      return md if md.length.zero?

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} md = #{md.inspect}"}
      tz = md["exchangeTimezoneName"]

      if !tradingPeriodsOnly
        ["firstTradeDate", "regularMarketTime"].each do |k|
          if md.key?(k) && !md[k].nil?
            if md[k].is_a?(Integer)
              md[k] = Time.at(md[k]).in_time_zone(tz)
            end
          end
        end

        if md.key?("currentTradingPeriod")
          ["regular", "pre", "post"].each do |m|
            if md["currentTradingPeriod"].key?(m) && md["currentTradingPeriod"][m]["start"].is_a?(Integer)
              ["start", "end"].each do |t|
                md["currentTradingPeriod"][m][t] = Time.at(md["currentTradingPeriod"][m][t]).utc.in_time_zone(tz)
              end
              md["currentTradingPeriod"][m].delete("gmtoffset")
              md["currentTradingPeriod"][m].delete("timezone")
            end
          end
        end
      end

      if md.key?("tradingPeriods")
        tps = md["tradingPeriods"]
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} tps = #{tps.inspect}"}
        if tps == {"pre" => [], "post" => []}
          # Ignore
        elsif tps.is_a?(Array) || tps.is_a?(Hash)
          if tps.is_a?(Array)
            df = Polars::DataFrame.from_records(_np.hstack(tps))
            df = df.drop(["timezone", "gmtoffset"], axis: 1)
            df["start"] = Time.at(df["start"]).in_time_zone(tz)
            df["end"] = Time.at(df['end']).in_time_zone(tz)
          else   #if tps.is_a?(Hash)
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} tps[pre] = #{tps['pre'].flatten.inspect}"}
            pre_df = {} ; tps['pre'].flatten.each{|yy| yy.keys.each{|yyk| pre_df[yyk] ||= []; pre_df[yyk] << yy[yyk] }}; pre_df = Polars::DataFrame.new(pre_df) # Polars::DataFrame.from_records(_np.hstack(tps["pre"]))
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} pre_df = #{pre_df.inspect}"}
            post_df = {}; tps['post'].flatten.each{|yy| yy.keys.each{|yyk| post_df[yyk] ||= []; post_df[yyk] << yy[yyk] }}; post_df = Polars::DataFrame.new(post_df)  # Polars::DataFrame.from_records(_np.hstack(tps["post"]))
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} post_df = #{post_df.inspect}"}
            regular_df = {}; tps['regular'].flatten.each{|yy| yy.keys.each{|yyk| regular_df[yyk] ||= []; regular_df[yyk] << yy[yyk] }}; regular_df = Polars::DataFrame.new(regular_df)  # Polars::DataFrame.from_records(_np.hstack(tps["regular"]))
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} regular_df = #{regular_df.inspect}"}

            pre_df = pre_df.rename({"start" => "pre_start", "end" => "pre_end"}).drop(["timezone", "gmtoffset"]) #, axis: 1)
            post_df = post_df.rename({"start" => "post_start", "end" => "post_end"}).drop(["timezone", "gmtoffset"]) #, axis: 1)
            regular_df = regular_df.drop(["timezone", "gmtoffset"]) #, axis: 1)

            cols = ["pre_start", "pre_end", "end", "post_end"]
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} pre_df = #{pre_df.inspect}"}
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} post_df = #{post_df.inspect}"}
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} regular_df = #{regular_df.inspect}"}
            df = pre_df.join(regular_df, left_on: 'pre_end', right_on: 'start')
            df = df.join(post_df, left_on: 'end', right_on: 'post_start')
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} df = #{df.inspect}"}
            cols.each do |c|
              # Rails.logger.info { "#{__FILE__}:#{__LINE__} c = #{c}"}
              # Rails.logger.info { "#{__FILE__}:#{__LINE__} df[c].map{|t| Time.at(t).in_time_zone(tz) } = #{df[c].map{|t| Time.at(t).in_time_zone(tz) }.inspect}" }
              s = Polars::Series.new(df[c].map{|t| Time.at(t).in_time_zone(tz) }, dtype: :i64)
              # Rails.logger.info { "#{__FILE__}:#{__LINE__} s = #{s.inspect}" }
              df.replace(c, s)
            end

            df = Polars::DataFrame.new({'pre_start' => df['pre_start'], 'pre_end' => df['pre_end'], 'start' => df['pre_end'], 'end' => df['end'], 'post_start' => df['end'], 'post_end' => df['post_end']})
            # df = df[cols]
          end

          # df.index = _pd.to_datetime(df["start"].dt.date)
          # df.index = df.index.tz_localize(tz)
          # df.index.name = "Date"

          md["tradingPeriods"] = df
        end
      end

      return md
    end

    def _safe_merge_dfs(df_main, df_sub, interval)
      if df_sub.empty?
        raise Exception.new("No data to merge")
      end
      if df_main.empty?
        return df_main
      end

      df = df_main
      return df
    end


    def _parse_quotes(data, interval)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} data = #{data.inspect}" }
      timestamps = data["timestamp"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} timestamps = #{timestamps.inspect}" }
      ohlc = data["indicators"]["quote"][0]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} ohlc = #{ohlc.inspect}" }
      volumes = ohlc["volume"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} volumes = #{volumes.inspect}" }
      opens = ohlc["open"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} opens = #{opens.inspect}" }
      closes = ohlc["close"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} closes = #{closes.inspect}" }
      lows = ohlc["low"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} lows = #{lows.inspect}" }
      highs = ohlc["high"]
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} highs = #{highs.inspect}" }

      adjclose = closes
      if data["indicators"].key?("adjclose")
        adjclose = data["indicators"]["adjclose"][0]["adjclose"]
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} adjclose = #{adjclose.inspect}" }
      end

      quotes = Polars::DataFrame.new(
        {
          'Timestamps': timestamps.map{|t| Time.at(t) },
          "Open": opens,
          "High": highs,
          "Low": lows,
          "Close": closes,
          "Adj Close": adjclose,
          "Volume": volumes
        }
      )

      # quotes.index = _pd.to_datetime(timestamps, unit: "s")
      # quotes.sort_index!(inplace: true)

      if interval.downcase == "30m"
        logger.debug("#{ticker}: resampling 30m OHLC from 15m")
        quotes2 = quotes.resample('30T')
        quotes = Polars::DataFrame.new(index: quotes2.last.index, data: {
                                         'Open' => quotes2['Open'].first,
                                         'High' => quotes2['High'].max,
                                         'Low' => quotes2['Low'].min,
                                         'Close' => quotes2['Close'].last,
                                         'Adj Close' => quotes2['Adj Close'].last,
                                         'Volume' => quotes2['Volume'].sum
        })
        begin
          quotes['Dividends'] = quotes2['Dividends'].max
          quotes['Stock Splits'] = quotes2['Stock Splits'].max
        rescue Exception
        end
      end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} quotes = #{quotes.inspect}" }
      return quotes
    end

    def _fix_unit_mixups(df, interval, tz_exchange, prepost)
      # TODO: Implement _fix_unit_mixups
      return df
    end

    # def _fix_bad_stock_split(df, interval, tz_exchange)
    #   # TODO: Implement _fix_bad_stock_split
    #   return df
    # end

    # def _fix_zeroes(df, interval, tz_exchange, prepost)
    #   # TODO: Implement _fix_zeroes
    #   return df
    # end

    # def _fix_missing_div_adjust(df, interval, tz_exchange)
    #   # TODO: Implement _fix_missing_div_adjust
    #   return df
    # end

    def _reconstruct_intervals_batch(df, interval, prepost, tag=-1)
      #   # Reconstruct values in df using finer-grained price data. Delimiter marks what to reconstruct
      #   logger = Rails.logger # Yfinrb::Utils.get_yf_logger

      #   # raise Exception.new("'df' must be a Polars DataFrame not", type(df)) unless df.is_a?(Polars::DataFrame)
      #   return df if interval == "1m"

      #   if interval[1:].in?(['d', 'wk', 'mo'])
      #     # Interday data always includes pre & post
      #     prepost = true
      #     intraday = false
      #   else
      #     intraday = true
      #   end

      #   price_cols = df.columns.select { |c| PRICE_COLNAMES.include?(c) }
      #   data_cols = price_cols + ["Volume"]

      #   # If interval is weekly then can construct with daily. But if smaller intervals then
      #   # restricted to recent times:
      #   intervals = ["1wk", "1d", "1h", "30m", "15m", "5m", "2m", "1m"]
      #   itds = intervals.map { |i| [i, Yfinrb::Utils.interval_to_timedelta(interval)] }.to_h
      #   nexts = intervals.each_cons(2).to_h
      #   min_lookbacks = {"1wk" => nil, "1d" => nil, "1h" => 730.days }
      #   ["30m", "15m", "5m", "2m"].each { |i| min_lookbacks[i] = 60.days }
      #   min_lookbacks["1m"] = 30.days

      #   if interval.in?(nexts)
      #     sub_interval = nexts[interval]
      #     td_range = itds[interval]
      #   else
      #     logger.warning("Have not implemented price repair for '#{interval}' interval. Contact developers") unless df.columns.include?("Repaired?")
      #     return df
      #   end

      #   # Limit max reconstruction depth to 2:
      #   @reconstruct_start_interval = interval if @reconstruct_start_interval.nil?
      #   if interval != @reconstruct_start_interval && interval != nexts[@reconstruct_start_interval]
      #     logger.debug("#{ticker}: Price repair has hit max depth of 2 ('%s'->'%s'->'%s')", @reconstruct_start_interval, nexts[@reconstruct_start_interval], interval)
      #     return df
      #   end

      #   df = df.sort_index

      #   f_repair = df[data_cols].to_numpy == tag
      #   f_repair_rows = f_repair.any(axis=1)

      #   # Ignore old intervals for which yahoo won't return finer data:
      #   m = min_lookbacks[sub_interval]

      #   if m.nil?
      #     min_dt = nil
      #   else
      #     m -= _datetime.timedelta(days=1)  # allow space for 1-day padding
      #     min_dt = DateTime.now.utc - m
      #     min_dt = min_dt.tz_convert(df.index.tz).ceil("D")
      #   end

      #   logger.debug("min_dt=#{min_dt} interval=#{interval} sub_interval=#{sub_interval}")

      #   if min_dt.nil?
      #     f_recent = df.index >= min_dt
      #     f_repair_rows = f_repair_rows & f_recent
      #     unless f_repair_rows.any?
      #       logger.info("Data too old to repair") unless df.columns.include?("Repaired?")
      #       return df
      #     end
      #   end

      #   dts_to_repair = df.index[f_repair_rows]

      #   if dts_to_repair.length == 0
      #     logger.info("Nothing needs repairing (dts_to_repair[] empty)") unless df.columns.include?("Repaired?")
      #     return df
      #   end

      #   df_v2 = df.copy
      #   df_v2["Repaired?"] = false unless df_v2.columns.include?("Repaired?")
      #   f_good = ~(df[price_cols].isna.any(axis=1))
      #   f_good = f_good && (df[price_cols].to_numpy != tag).all(axis=1)
      #   df_good = df[f_good]

      #   # Group nearby NaN-intervals together to reduce number of yahoo fetches
      #   dts_groups = [[dts_to_repair[0]]]
      #   # Note on setting max size: have to allow space for adding good data
      #   if sub_interval == "1mo"
      #     grp_max_size = _dateutil.relativedelta.relativedelta(years=2)
      #   elsif sub_interval == "1wk"
      #     grp_max_size = _dateutil.relativedelta.relativedelta(years=2)
      #   elsif sub_interval == "1d"
      #     grp_max_size = _dateutil.relativedelta.relativedelta(years=2)
      #   elsif sub_interval == "1h"
      #     grp_max_size = _dateutil.relativedelta.relativedelta(years=1)
      #   elsif sub_interval == "1m"
      #     grp_max_size = _datetime.timedelta(days=5)  # allow 2 days for buffer below
      #   else
      #     grp_max_size = _datetime.timedelta(days=30)
      #   end

      #   logger.debug("grp_max_size = #{grp_max_size}")

      #   (1..dts_to_repair.length).each do |i|
      #     dt = dts_to_repair[i]
      #     if dt.date < dts_groups[-1][0].date + grp_max_size
      #       dts_groups[-1].append(dt)
      #     else
      #       dts_groups.append([dt])
      #     end
      #   end

      #   logger.debug("Repair groups:")
      #   dts_groups.each { |g| logger.debug("- #{g[0]} -> #{g[-1]}") }

      #   # Add some good data to each group, so can calibrate prices later:
      #   (0..dts_groups.length).each do |i|
      #     g = dts_groups[i]
      #     g0 = g[0]
      #     i0 = df_good.index.get_indexer([g0], method="nearest")[0]
      #     if i0 > 0
      #       if (min_dt.nil? || df_good.index[i0 - 1] >= min_dt) && \
      #           ((!intraday) || df_good.index[i0 - 1].date == g0.date)
      #         i0 -= 1
      #       end
      #     end
      #     gl = g[-1]
      #     il = df_good.index.get_indexer([gl], method="nearest")[0]
      #     if il < len(df_good) - 1
      #         il += 1 if (!intraday) || df_good.index[il + 1].date == gl.date
      #     end
      #     good_dts = df_good.index[i0:il + 1]
      #     dts_groups[i] += good_dts.to_list
      #     dts_groups[i].sort
      #   end

      #   n_fixed = 0
      #   dts_groups.each do |g|
      #     df_block = df[df.index.isin(g)]
      #     logger.debug("df_block:\n" + str(df_block))

      #     start_dt = g[0]
      #     start_d = start_dt.date

      #     reject = false
      #     if sub_interval == "1h" && (DateTime::now - start_d) > 729.days
      #       reject = true
      #     elsif sub_interval.in?(["30m", "15m"]) && (DateTime::now - start_d) > 59.days
      #       reject = true
      #     end

      #     if reject
      #       # Don't bother requesting more price data, yahoo will reject
      #       msg = "Cannot reconstruct #{interval} block starting"
      #       msg += intraday ? " #{start_dt}" : " #{start_d}"
      #       msg += ", too old, yahoo will reject request for finer-grain data"
      #       logger.info(msg)
      #       next
      #     end

      #     td_1d = _datetime.timedelta(days=1)
      #     end_dt = g[-1]
      #     end_d = end_dt.date + td_1d

      #     if interval == "1wk"
      #       fetch_start = start_d - td_range  # need previous week too
      #       fetch_end = g[-1].date + td_range
      #     elsif interval == "1d"
      #       fetch_start = start_d
      #       fetch_end = g[-1].date + td_range
      #     else
      #       fetch_start = g[0]
      #       fetch_end = g[-1] + td_range
      #     end

      #     # The first and last day returned by yahoo can be slightly wrong, so add buffer:
      #     fetch_start -= td_1d
      #     fetch_end += td_1d
      #     if intraday
      #       fetch_start = fetch_start.date
      #       fetch_end = fetch_end.date + td_1d
      #     end

      #     fetch_start = max(min_dt.date, fetch_start) if min_dt.nil?
      #     logger.debug("Fetching #{sub_interval} prepost=#{prepost} #{fetch_start}->#{fetch_end}")

      #     df_fine = self.history(start: fetch_start, fin: fetch_end, interval: sub_interval, auto_adjust: false, actions: true, prepost: prepost, repair: true, keepna: true)
      #     if df_fine.nil? || df_fine.empty?
      #       msg = "Cannot reconstruct #{interval} block starting"
      #       msg += intraday ? " #{start_dt}" : " #{start_d}"
      #       msg += ", too old, yahoo is rejecting request for finer-grain data"
      #       logger.debug(msg)
      #       next
      #     end

      #     # Discard the buffer
      #     df_fine = df_fine.loc[g[0]: g[-1] + itds[sub_interval] - 1.milliseconds].copy

      #     if df_fine.empty?
      #       msg = "Cannot reconstruct #{interval} block range"
      #       msg += (intraday ? " #{start_dt}->#{end_dt}" : " #{start_d}->#{end_d}")
      #       msg += ", yahoo not returning finer-grain data within range"
      #       logger.debug(msg)
      #       next
      #     end

      #     df_fine["ctr"] = 0
      #     if interval == "1wk"
      #       weekdays = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
      #       week_end_day = weekdays[(df_block.index[0].weekday + 7 - 1) % 7]
      #       df_fine["Week Start"] = df_fine.index.tz_localize(nil).to_period("W-" + week_end_day).start_time
      #       grp_col = "Week Start"
      #     elsif interval == "1d"
      #       df_fine["Day Start"] = pd.to_datetime(df_fine.index.date)
      #       grp_col = "Day Start"
      #     else
      #       df_fine.loc[df_fine.index.isin(df_block.index), "ctr"] = 1
      #       df_fine["intervalID"] = df_fine["ctr"].cumsum
      #       df_fine = df_fine.drop("ctr", axis=1)
      #       grp_col = "intervalID"
      #     end
      #     df_fine = df_fine[~df_fine[price_cols + ['Dividends']].isna.all(axis=1)]

      #     df_fine_grp = df_fine.groupby(grp_col)
      #     df_new = df_fine_grp.agg(
      #       Open: ["Open", "first"],
      #       Close: ["Close", "last"],
      #       AdjClose: ["Adj Close", "last"],
      #       Low: ["Low", "min"],
      #       High: ["High", "max"],
      #       Dividends: ["Dividends", "sum"],
      #       Volume: ["Volume", "sum"]
      #     ).rename(columns: {"AdjClose": "Adj Close"})

      #     if grp_col.in?(["Week Start", "Day Start"])
      #       df_new.index = df_new.index.tz_localize(df_fine.index.tz)
      #     else
      #       df_fine["diff"] = df_fine["intervalID"].diff
      #       new_index = np.append([df_fine.index[0]], df_fine.index[df_fine["intervalID"].diff > 0])
      #       df_new.index = new_index
      #     end
      #     logger.debug('df_new:' + '\n' + str(df_new))
      #     # df_new = df_fine

      #     # Calibrate!
      #     common_index = np.intersect1d(df_block.index, df_new.index)
      #     if common_index.length == 0
      #       # Can't calibrate so don't attempt repair
      #       logger.info("Can't calibrate #{interval} block starting #{start_d} so aborting repair")
      #       next
      #     end

      #     # First, attempt to calibrate the 'Adj Close' column. OK if cannot.
      #     # Only necessary for 1d interval, because the 1h data is not div-adjusted.
      #     if interval == '1d'

      #       df_new_calib = df_new[df_new.index.isin(common_index)]
      #       df_block_calib = df_block[df_block.index.isin(common_index)]
      #       f_tag = df_block_calib['Adj Close'] == tag

      #       if f_tag.any?

      #         div_adjusts = df_block_calib['Adj Close'] / df_block_calib['Close']
      #         # The loop below assumes each 1d repair is isolated, i.e. surrounded by
      #         # good data. Which is case most of time.
      #         # But in case are repairing a chunk of bad 1d data, back/forward-fill the
      #         # good div-adjustments - not perfect, but a good backup.
      #         div_adjusts[f_tag] = np.nan
      #         div_adjusts = div_adjusts.ffill.bfill

      #         (0..np.where(f_tag)[0].length).each do |idx|
      #           dt = df_new_calib.index[idx]
      #           n = len(div_adjusts)

      #           if df_new.loc[dt, "Dividends"] != 0
      #             if idx < n - 1
      #               # Easy, take div-adjustment from next-day
      #               div_adjusts.iloc[idx] = div_adjusts.iloc[idx + 1]
      #             else
      #               # Take previous-day div-adjustment and reverse todays adjustment
      #               div_adj = 1.0 - df_new_calib["Dividends"].iloc[idx] / df_new_calib['Close'].iloc[idx - 1]
      #               div_adjusts.iloc[idx] = div_adjusts.iloc[idx - 1] / div_adj
      #             end

      #           else
      #             if idx > 0
      #               # Easy, take div-adjustment from previous-day
      #               div_adjusts.iloc[idx] = div_adjusts.iloc[idx - 1]
      #             else
      #               # Must take next-day div-adjustment
      #               div_adjusts.iloc[idx] = div_adjusts.iloc[idx + 1]
      #               if df_new_calib["Dividends"].iloc[idx + 1] != 0
      #                 div_adjusts.iloc[idx] *= 1.0 - df_new_calib["Dividends"].iloc[idx + 1] / \
      #                   df_new_calib['Close'].iloc[idx]
      #               end
      #             end
      #           end
      #         end

      #         f_close_bad = df_block_calib['Close'] == tag
      #         div_adjusts = div_adjusts.reindex(df_block.index, fill_value=np.nan).ffill.bfill
      #         df_new['Adj Close'] = df_block['Close'] * div_adjusts

      #         if f_close_bad.any?
      #           f_close_bad_new = f_close_bad.reindex(df_new.index, fill_value=false)
      #           div_adjusts_new = div_adjusts.reindex(df_new.index, fill_value=np.nan).ffill.bfill
      #           div_adjusts_new_np = f_close_bad_new.to_numpy
      #           df_new.loc[div_adjusts_new_np, 'Adj Close'] = df_new['Close'][div_adjusts_new_np] * div_adjusts_new[div_adjusts_new_np]
      #         end
      #       end

      #       # Check whether 'df_fine' has different split-adjustment.
      #       # If different, then adjust to match 'df'
      #       calib_cols = ['Open', 'Close']
      #       df_new_calib = df_new[df_new.index.isin(common_index)][calib_cols].to_numpy
      #       df_block_calib = df_block[df_block.index.isin(common_index)][calib_cols].to_numpy
      #       calib_filter = (df_block_calib != tag)

      #       if !calib_filter.any?
      #         # Can't calibrate so don't attempt repair
      #         logger.info("Can't calibrate #{interval} block starting #{start_d} so aborting repair")
      #         next
      #       end

      #       # Avoid divide-by-zero warnings:
      #       (0..calib_cols.length).each do |j|
      #         f = ~calib_filter[:, j]
      #         if f.any?
      #           df_block_calib[f, j] = 1
      #           df_new_calib[f, j] = 1
      #         end
      #       end

      #       ratios = df_block_calib[calib_filter] / df_new_calib[calib_filter]
      #       weights = df_fine_grp.size
      #       weights.index = df_new.index
      #       weights = weights[weights.index.isin(common_index)].to_numpy.astype(float)
      #       weights = weights[:, None]  # transpose
      #       weights = np.tile(weights, len(calib_cols))  # 1D -> 2D
      #       weights = weights[calib_filter]  # flatten
      #       not1 = ~np.isclose(ratios, 1.0, rtol=0.00001)

      #       if np.sum(not1) == len(calib_cols)
      #         # Only 1 calibration row in df_new is different to df_block so ignore
      #         ratio = 1.0
      #       else
      #         ratio = np.average(ratios, weights=weights)
      #       end

      #       logger.debug("Price calibration ratio (raw) = #{ratio:6f}")
      #       ratio_rcp = round(1.0 / ratio, 1)
      #       ratio = round(ratio, 1)
      #       if ratio == 1 && ratio_rcp == 1
      #         # Good!
      #         next

      #       else
      #         if ratio > 1
      #           # data has different split-adjustment than fine-grained data
      #           # Adjust fine-grained to match
      #           df_new[price_cols] *= ratio
      #           df_new["Volume"] /= ratio
      #         elsif ratio_rcp > 1
      #           # data has different split-adjustment than fine-grained data
      #           # Adjust fine-grained to match
      #           df_new[price_cols] *= 1.0 / ratio_rcp
      #           df_new["Volume"] *= ratio_rcp
      #         end
      #       end

      #       # Repair!
      #       bad_dts = df_block.index[(df_block[price_cols + ["Volume"]] == tag).to_numpy.any(axis=1)]

      #       no_fine_data_dts = []
      #       bad_dts.each do |idx|
      #         if !df_new.index.include?(idx)
      #           # yahoo didn't return finer-grain data for this interval,
      #           # so probably no trading happened.
      #           no_fine_data_dts.append(idx)
      #         end
      #       end

      #       unless no_fine_data_dts.length == 0
      #         logger.debug("yahoo didn't return finer-grain data for these intervals: " + str(no_fine_data_dts))
      #       end

      #       bad_dts.each do |idx|

      #         # yahoo didn't return finer-grain data for this interval,
      #         # so probably no trading happened.
      #         next if !df_new.index.include?(idx)

      #         df_new_row = df_new.loc[idx]

      #         if interval == "1wk"
      #           df_last_week = df_new.iloc[df_new.index.get_loc(idx) - 1]
      #           df_fine = df_fine.loc[idx:]
      #         end

      #         df_bad_row = df.loc[idx]
      #         bad_fields = df_bad_row.index[df_bad_row == tag].to_numpy

      #         df_v2.loc[idx, "High"] = df_new_row["High"] if bad_fields.include?("High")

      #         df_v2.loc[idx, "Low"] = df_new_row["Low"] if bad_fields.include?("Low")

      #         if bad_fields.include?("Open")
      #           if interval == "1wk" && idx != df_fine.index[0]
      #             # Exchange closed Monday. In this case, yahoo sets Open to last week close
      #             df_v2.loc[idx, "Open"] = df_last_week["Close"]
      #             df_v2.loc[idx, "Low"] = [df_v2.loc[idx, "Open"], df_v2.loc[idx, "Low"]].min
      #           else
      #             df_v2.loc[idx, "Open"] = df_new_row["Open"]
      #           end
      #         end

      #         if bad_fields.include?("Close")
      #           df_v2.loc[idx, "Close"] = df_new_row["Close"]
      #           # Assume 'Adj Close' also corrupted, easier than detecting whether true
      #           df_v2.loc[idx, "Adj Close"] = df_new_row["Adj Close"]
      #         elsif bad_fields.include?("Adj Close")
      #           df_v2.loc[idx, "Adj Close"] = df_new_row["Adj Close"]
      #         end
      #         if bad_fields.include?("Volume")
      #           df_v2.loc[idx, "Volume"] = df_new_row["Volume"]
      #         end
      #         df_v2.loc[idx, "Repaired?"] = true
      #         n_fixed += 1
      #       end
      #     end
      #   end
      #   return df_v2
      # end
      return df
    end

    def _fix_unit_mixups(df, interval, tz_exchange, prepost)
      #   return df if df.empty?
      #   df2 = self._fix_unit_switch(df, interval, tz_exchange)
      #   df3 = self._fix_unit_random_mixups(df2, interval, tz_exchange, prepost)
      #   return df3
    end

    def _fix_unit_random_mixups(df, interval, tz_exchange, prepost)
      #   # Sometimes yahoo returns few prices in cents/pence instead of $/£
      #   # I.e. 100x bigger
      #   # 2 ways this manifests:
      #   # - random 100x errors spread throughout table
      #   # - a sudden switch between $<->cents at some date
      #   # This function fixes the first.

      #   return df if df.empty?

      #   # Easy to detect and fix, just look for outliers = ~100x local median
      #   logger = Rails.logger # Yfinrb::Utils.get_yf_logger

      #   if df.shape[0] == 0
      #     df["Repaired?"] = false if !df.columns.include?("Repaired?")
      #     return df
      #   end
      #   if df.shape[0] == 1
      #     # Need multiple rows to confidently identify outliers
      #     logger.info("price-repair-100x: Cannot check single-row table for 100x price errors")
      #     df["Repaired?"] = false if !df.columns.include?("Repaired?")

      #     return df
      #   end

      #   df2 = df.copy

      #   if df2.index.tz.nil?
      #     df2.index = df2.index.tz_localize(tz_exchange)
      #   elsif df2.index.tz != tz_exchange
      #     df2.index = df2.index.tz_convert(tz_exchange)
      #   end

      #   # Only import scipy if users actually want function. To avoid
      #   # adding it to dependencies.
      #   require 'scipy'

      #   data_cols = ["High", "Open", "Low", "Close", "Adj Close"]  # Order important, separate High from Low
      #   data_cols = data_cols.select { |c| df2.columns.include?(c) }
      #   f_zeroes = (df2[data_cols] == 0).any(axis=1).to_numpy

      #   if f_zeroes.any?
      #     df2_zeroes = df2[f_zeroes]
      #     df2 = df2[~f_zeroes]
      #     df = df[~f_zeroes]  # all row slicing must be applied to both df and df2

      #   else
      #     df2_zeroes = nil
      #   end

      #   if df2.shape[0] <= 1
      #     logger.info("price-repair-100x: Insufficient good data for detecting 100x price errors")
      #     df["Repaired?"] = false if !df.columns.include?("Repaired?")

      #     return df
      #   end

      #   df2_data = df2[data_cols].to_numpy
      #   median = scipy.ndimage.median_filter(df2_data, size: [3, 3], mode: "wrap")
      #   ratio = df2_data / median
      #   ratio_rounded = (ratio / 20).round * 20  # round ratio to nearest 20
      #   f = ratio_rounded == 100
      #   ratio_rcp = 1.0 / ratio
      #   ratio_rcp_rounded = (ratio_rcp / 20).round * 20  # round ratio to nearest 20
      #   f_rcp = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
      #   f_either = f | f_rcp

      #   if !f_either.any?
      #     logger.info("price-repair-100x: No sporadic 100x errors")

      #     df["Repaired?"] = false if !df.columns.include?("Repaired?")

      #     return df
      #   end

      #   # Mark values to send for repair
      #   tag = -1.0
      #   data_cols.each_with_index do |c, i|
      #     fi = f_either[:, i]
      #     df2.loc[fi, c] = tag
      #   end

      #   n_before = (df2_data == tag).sum
      #   df2 = _reconstruct_intervals_batch(df2, interval, prepost, tag)
      #   df2_tagged = df2[data_cols].to_numpy == tag
      #   n_after = (df2[data_cols].to_numpy == tag).sum

      #   if n_after > 0
      #     # This second pass will *crudely* "fix" any remaining errors in High/Low
      #     # simply by ensuring they don't contradict e.g. Low = 100x High.
      #     f = (df2[data_cols].to_numpy == tag) & f
      #     f.each_with_index do |fi, i|
      #       next if !fi.any?

      #       idx = df2.index[i]

      #       ['Open', 'Close'].each do |c|
      #         j = data_cols.index(c)
      #         df2.loc[idx, c] = df.loc[idx, c] * 0.01 if fi[j]
      #       end
      #     end

      #     c = "High"
      #     j = data_cols.index(c)
      #     df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].max if fi[j]

      #     c = "Low"
      #     j = data_cols.index(c)
      #     df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].min if fi[j]
      #   end

      #   f_rcp = (df2[data_cols].to_numpy == tag) & f_rcp
      #   f_rcp.each_with_index do |fi, i|
      #     next if !fi.any?

      #     idx = df2.index[i]

      #     ['Open', 'Close'].each do |c|
      #       j = data_cols.index(c)

      #       df2.loc[idx, c] = df.loc[idx, c] * 100.0 if fi[j]
      #     end

      #     c = "High"
      #     j = data_cols.index(c)
      #     df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].max if fi[j]

      #     c = "Low"
      #     j = data_cols.index(c)
      #     df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].min if fi[j]
      #   end

      #   df2_tagged = df2[data_cols].to_numpy == tag
      #   n_after_crude = df2_tagged.sum

      #   else
      #     n_after_crude = n_after
      #   end

      #   n_fixed = n_before - n_after_crude
      #   n_fixed_crudely = n_after - n_after_crude
      #   if n_fixed > 0
      #     report_msg = "#{ticker}: fixed #{n_fixed}/#{n_before} currency unit mixups "
      #     report_msg += "(#{n_fixed_crudely} crudely) " if n_fixed_crudely > 0

      #     report_msg += "in #{interval} price data"
      #     logger.info('price-repair-100x: ' + report_msg)
      #   end

      #   # Restore original values where repair failed
      #   f_either = df2[data_cols].to_numpy == tag
      #   f_either.each_with_index do |fj, j|
      #     if fj.any?
      #       c = data_cols[j]
      #       df2.loc[fj, c] = df.loc[fj, c]
      #     end
      #   end
      #   if df2_zeroes
      #     df2_zeroes["Repaired?"] = false if !df2_zeroes.columns.include?("Repaired?")

      #     df2 = pd.concat([df2, df2_zeroes]).sort_index
      #     df2.index = pd.to_datetime(df2.index)
      #   end

      #   return df2
      return df
    end

    def _fix_unit_switch(df, interval, tz_exchange)
      # Sometimes yahoo returns few prices in cents/pence instead of $/£
      # I.e. 100x bigger
      # 2 ways this manifests:
      # - random 100x errors spread throughout table
      # - a sudden switch between $<->cents at some date
      # This function fixes the second.
      # Eventually yahoo fixes but could take them 2 weeks.

      return fix_prices_sudden_change(df, interval, tz_exchange, 100.0)
    end

    def _fix_zeroes(df, interval, tz_exchange, prepost)
      # # Sometimes yahoo returns prices=0 or NaN when trades occurred.
      # # But most times when prices=0 or NaN returned is because no trades.
      # # Impossible to distinguish, so only attempt repair if few or rare.

      # return df if df.empty?

      # logger = Rails.logger #utils.get_yf_logger

      # if df.shape[0] == 0
      #   df["Repaired?"] = false if !df.columns.include?("Repaired?")
      #   return df
      # end

      # intraday = interval[-1] in ["m", 'h']

      # df = df.sort_index  # important!
      # df2 = df.copy

      # if df2.index.tz.nil?
      #   df2.index = df2.index.tz_localize(tz_exchange)
      # elsif df2.index.tz != tz_exchange
      #   df2.index = df2.index.tz_convert(tz_exchange)
      # end

      # price_cols = ["High", "Open", "Low", "Close", "Adj Close"].select { |c| df2.columns.include?(c) }
      # f_prices_bad = (df2[price_cols] == 0.0) | df2[price_cols].isna
      # df2_reserve = nil
      # if intraday
      #   # Ignore days with >50% intervals containing NaNs
      #   grp = Polars::Series(f_prices_bad.any(axis=1), name: "nan").groupby(f_prices_bad.index.date)
      #   nan_pct = grp.sum / grp.count
      #   dts = nan_pct.index[nan_pct > 0.5]
      #   f_zero_or_nan_ignore = np.isin(f_prices_bad.index.date, dts)
      #   df2_reserve = df2[f_zero_or_nan_ignore]
      #   df2 = df2[~f_zero_or_nan_ignore]
      #   f_prices_bad = (df2[price_cols] == 0.0) | df2[price_cols].isna
      # end

      # f_high_low_good = (~df2["High"].isna.to_numpy) & (~df2["Low"].isna.to_numpy)
      # f_change = df2["High"].to_numpy != df2["Low"].to_numpy
      # f_vol_bad = (df2["Volume"] == 0).to_numpy & f_high_low_good & f_change

      # # If stock split occurred, then trading must have happened.
      # # I should probably rename the function, because prices aren't zero ...
      # if df2.columns.include?('Stock Splits')
      #   f_split = (df2['Stock Splits'] != 0.0).to_numpy
      #   if f_split.any?
      #     f_change_expected_but_missing = f_split & ~f_change

      #     f_prices_bad[f_change_expected_but_missing] = true if f_change_expected_but_missing.any?
      #   end
      # end

      # # Check whether worth attempting repair
      # f_prices_bad = f_prices_bad.to_numpy
      # f_bad_rows = f_prices_bad.any(axis=1) | f_vol_bad
      # if !f_bad_rows.any?
      #   logger.info("price-repair-missing: No price=0 errors to repair")

      #   df["Repaired?"] = false if !df.columns.include?("Repaired?")

      #   return df
      # end
      # if f_prices_bad.sum == len(price_cols) * len(df2)
      #   # Need some good data to calibrate
      #   logger.info("price-repair-missing: No good data for calibration so cannot fix price=0 bad data")

      #   df["Repaired?"] = false if !df.columns.include?("Repaired?")

      #   return df
      # end

      # data_cols = price_cols + ["Volume"]

      # # Mark values to send for repair
      # tag = -1.0
      # price_cols.each_with_index { |c, i| df2.loc[f_prices_bad[:, i], c] = tag }

      # df2.loc[f_vol_bad, "Volume"] = tag
      # # If volume=0 or NaN for bad prices, then tag volume for repair
      # f_vol_zero_or_nan = (df2["Volume"].to_numpy == 0) | (df2["Volume"].isna.to_numpy)
      # df2.loc[f_prices_bad.any(axis=1) & f_vol_zero_or_nan, "Volume"] = tag
      # # If volume=0 or NaN but price moved in interval, then tag volume for repair
      # df2.loc[f_change & f_vol_zero_or_nan, "Volume"] = tag

      # df2_tagged = df2[data_cols].to_numpy == tag
      # n_before = df2_tagged.sum
      # dts_tagged = df2.index[df2_tagged.any(axis=1)]
      # df2 = _reconstruct_intervals_batch(df2, interval, prepost, tag)
      # df2_tagged = df2[data_cols].to_numpy == tag
      # n_after = df2_tagged.sum
      # dts_not_repaired = df2.index[df2_tagged.any(axis=1)]
      # n_fixed = n_before - n_after
      # if n_fixed > 0
      #   msg = "#{ticker}: fixed #{n_fixed}/#{n_before} value=0 errors in #{interval} price data"
      #   if n_fixed < 4
      #     dts_repaired = (dts_tagged - dts_not_repaired).to_list.sort
      #     msg += ": #{dts_repaired}"
      #   end
      #   logger.info('price-repair-missing: ' + msg)
      # end

      # if df2_reserve
      #   df2_reserve["Repaired?"] = false if !df2_reserve.columns.include?("Repaired?")

      #   df2 = pd.concat([df2, df2_reserve]).sort_index
      # end

      # # Restore original values where repair failed (i.e. remove tag values)
      # f = df2[data_cols].to_numpy == tag
      # f.each_with_index do |fj, j|
      #   if fj.any?
      #     c = data_cols[j]
      #     df2.loc[fj, c] = df.loc[fj, c]
      #   end
      # end

      # return df2
    end

    def _fix_missing_div_adjust(df, interval, tz_exchange)
      # # Sometimes, if a dividend occurred today, then yahoo has not adjusted historic data.
      # # Easy to detect and correct BUT ONLY IF the data 'df' includes today's dividend.
      # # E.g. if fetching historic prices before todays dividend, then cannot fix.

      # logger = Rails.logger # utils.get_yf_logger

      # return df if df.nil? || df.empty?

      # interday = interval in ['1d', '1wk', '1mo', '3mo']

      # return df if !interday

      # df = df.sort_index

      # f_div = (df["Dividends"] != 0.0).to_numpy
      # if !f_div.any?
      #   logger.debug('div-adjust-repair: No dividends to check')
      #   return df
      # end

      # df2 = df.copy
      # if df2.index.tz.nil?
      #   df2.index = df2.index.tz_localize(tz_exchange)
      # elsif df2.index.tz != tz_exchange
      #   df2.index = df2.index.tz_convert(tz_exchange)
      # end

      # div_indices = np.where(f_div)[0]
      # last_div_idx = div_indices[-1]
      # if last_div_idx == 0
      #   # Not enough data to recalculate the div-adjustment,
      #   # because need close day before
      #   logger.debug('div-adjust-repair: Insufficient data to recalculate div-adjustment')
      #   return df
      # end

      # # To determine if yahoo messed up, analyse price data between today's dividend and
      # # the previous dividend
      # if div_indices.length == 1
      #   # No other divs in data
      #   prev_idx = 0
      #   prev_dt = nil
      # else
      #   prev_idx = div_indices[-2]
      #   prev_dt = df2.index[prev_idx]
      # end
      # f_no_adj = (df2['Close'] == df2['Adj Close']).to_numpy[prev_idx:last_div_idx]
      # threshold_pct = 0.5
      # yahoo_failed = (np.sum(f_no_adj) / len(f_no_adj)) > threshold_pct

      # # Fix yahoo
      # if yahoo_failed
      #   last_div_dt = df2.index[last_div_idx]
      #   last_div_row = df2.loc[last_div_dt]
      #   close_day_before = df2['Close'].iloc[last_div_idx - 1]
      #   adj = 1.0 - df2['Dividends'].iloc[last_div_idx] / close_day_before
      #   div = last_div_row['Dividends']
      #   msg = "Correcting missing div-adjustment preceding div = #{div} @ #{last_div_dt.date} (prev_dt=#{prev_dt})"
      #   logger.debug('div-adjust-repair: ' + msg)

      #   if interval == '1d'
      #     # exclusive
      #     df2.loc[:last_div_dt - _datetime.timedelta(seconds=1), 'Adj Close'] *= adj
      #   else
      #     # inclusive
      #     df2.loc[:last_div_dt, 'Adj Close'] *= adj
      #   end
      # end

      # return df2
      return df
    end

    def _fix_bad_stock_split(df, interval, tz_exchange)
      # # Repair idea is to look for BIG daily price changes that closely match the
      # # most recent stock split ratio. This indicates yahoo failed to apply a new
      # # stock split to old price data.
      # #
      # # There is a slight complication, because yahoo does another stupid thing.
      # # Sometimes the old data is adjusted twice. So cannot simply assume
      # # which direction to reverse adjustment - have to analyse prices and detect.
      # # Not difficult.

      # return df if df.empty?

      # logger = Rails.logger # utils.get_yf_logger

      # interday = interval.in?(['1d', '1wk', '1mo', '3mo'])

      # return df if !interday

      # # Find the most recent stock split
      # df = df.sort_index(ascending: false)
      # split_f = df['Stock Splits'].to_numpy != 0
      # if !split_f.any?
      #   logger.debug('price-repair-split: No splits in data')
      #   return df
      # end
      # most_recent_split_day = df.index[split_f].max
      # split = df.loc[most_recent_split_day, 'Stock Splits']
      # if most_recent_split_day == df.index[0]
      #   logger.info("price-repair-split: Need 1+ day of price data after split to determine true price. Won't repair")
      #   return df
      # end

      # # logger.debug("price-repair-split: Most recent split = #{split:.4f} @ #{most_recent_split_day.date}")

      # return _fix_prices_sudden_change(df, interval, tz_exchange, split, correct_volume: true)
      return df
    end

    def _get_1y_prices( fullDaysOnly=false)
      if @prices_1y.nil?
        @prices_1y = history(period: "380d", auto_adjust: false, keepna: true) #, proxy: self.proxy)
        @md = get_history_metadata #(proxy=self.proxy)
        begin
          ctp = @md["currentTradingPeriod"]
          # Rails.logger.info { "#{__FILE__}:#{__LINE__} ctp = #{ctp.inspect}" }
          @today_open = Time.at(ctp["regular"]["start"]).in_time_zone(tz)
          @today_close = Time.at(ctp["regular"]["end"]).in_time_zone(tz)
          @today_midnight = @today_close.midnight
        rescue Exception => e
          @today_open = nil
          @today_close = nil
          @today_midnight = nil
          raise
        end
      end

      return @prices_1y unless @prices_1y.nil? || @prices_1y.empty?

      dnow = DateTime.now.utc.to_date
      d1 = dnow
      d0 = (d1 + datetime.timedelta(days=1)) - 1.year
      if fullDaysOnly && @_exchange_open_now
        # Exclude today
        d1 -= 1.day
      end
      return @prices_1y[str(d0)..str(d1)]
    end

    def _get_1wk_1h_prepost_prices
      return @prices_1wk_1h_prepost ||= history(period: "1wk", interval: "1h", auto_adjust: false, prepost: true)
    end

    def _get_1wk_1h_reg_prices
      return @prices_1wk_1h_reg ||= history(period: "1wk", interval: "1h", auto_adjust: false, prepost: false)
    end

    def _get_exchange_metadata
      if @md.nil?

        _get_1y_prices
        @md = get_history_metadata #(proxy=self.proxy)
      end
      return @md
    end

    def _exchange_open_now
      t = DateTime.now
      _get_exchange_metadata

      # if self._today_open is nil and self._today_close.nil?
      #     r = false
      # else:
      #     r = self._today_open <= t and t < self._today_close

      # if self._today_midnight.nil?
      #     r = false
      # elsif self._today_midnight.date > t.tz_convert(self.timezone).date:
      #     r = false
      # else:
      #     r = t < self._today_midnight

      last_day_cutoff = @get_1y_prices[-1] + 1.days
      last_day_cutoff += 20.minutes
      r = t < last_day_cutoff

      # print("_exchange_open_now returning", r)
      # return r
    end
  end
end
