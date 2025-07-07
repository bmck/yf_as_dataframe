require 'polars-df'
require 'logger'

class YfAsDataframe
  class Multi

    def download(tickers, start: nil, fin: nil, actions: false, threads: true,
                 ignore_tz: nil, group_by: 'column', auto_adjust: false,
                 back_adjust: false, repair: false, keepna: false, progress: true,
                 period: "max", show_errors: nil, interval: "1d", prepost: false,
                 proxy: nil, rounding: false, timeout: 10, session: nil)
      # """Download yahoo tickers
      # :Parameters:
      #     tickers : str, list
      #         List of tickers to download
      #     period : str
      #         Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
      #         Either Use period parameter or use start and end
      #     interval : str
      #         Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
      #         Intraday data cannot extend last 60 days
      #     start: str
      #         Download start date string (YYYY-MM-DD) or _datetime, inclusive.
      #         Default is 99 years ago
      #         E.g. for start="2020-01-01", the first data point will be on "2020-01-01"
      #     fin: str
      #         Download end date string (YYYY-MM-DD) or _datetime, exclusive.
      #         Default is now
      #         E.g. for end="2023-01-01", the last data point will be on "2022-12-31"
      #     group_by : str
      #         Group by 'ticker' or 'column' (default)
      #     prepost : bool
      #         Include Pre and Post market data in results?
      #         Default is false
      #     auto_adjust: bool
      #         Adjust all OHLC automatically? Default is false
      #     repair: bool
      #         Detect currency unit 100x mixups and attempt repair
      #         Default is false
      #     keepna: bool
      #         Keep NaN rows returned by Yahoo?
      #         Default is false
      #     actions: bool
      #         Download dividend + stock splits data. Default is false
      #     threads: bool / int
      #         How many threads to use for mass downloading. Default is true
      #     ignore_tz: bool
      #         When combining from different timezones, ignore that part of datetime.
      #         Default depends on interval. Intraday = false. Day+ = true.
      #     proxy: str
      #         Optional. Proxy server URL scheme. Default is None
      #     rounding: bool
      #         Optional. Round values to 2 decimal places?
      #     show_errors: bool
      #         Optional. Doesn't print errors if false
      #         DEPRECATED, will be removed in future version
      #     timeout: None or float
      #         If not None stops waiting for a response after given number of
      #         seconds. (Can also be a fraction of a second e.g. 0.01)
      #     session: None or Session
      #         Optional. Pass your own session object to be used for all requests
      # """
      logger = Logger.new(STDOUT)

      # YfAsDataframe::Utils.print_once("yfinance: download(show_errors=#{show_errors}) argument is deprecated and will be removed in future version. Do this instead: logging.getLogger('yfinance').setLevel(logging.ERROR)")

      if show_errors
        # YfAsDataframe::Utils.print_once("yfinance: download(show_errors=#{show_errors}) argument is deprecated and will be removed in future version. Do this instead to suppress error messages: logging.getLogger('yfinance').setLevel(logging.CRITICAL)")
        # logger.level = Logger::CRITICAL
      else
        # logger.level = Logger::CRITICAL
      end

      # if logger.debug?
      #   threads = false if threads
      #   logger.debug('Disabling multithreading because DEBUG logging enabled')
      #   progress = false if progress
      # end

      ignore_tz = interval[1..-1].match?(/[mh]/) ? false : true if ignore_tz.nil?

      tickers = tickers.is_a?(Array) ? tickers : tickers.gsub(',', ' ').split
      _tickers_ = []
      tickers.each do |ticker|
        if YfAsDataframe::Utils.is_isin(ticker)
          isin = ticker
          ticker = YfAsDataframe::Utils.get_ticker_by_isin(ticker, proxy, session: session)
          # @shared::_ISINS[ticker] = isin
        end
        _tickers_ << ticker
      end
      tickers = _tickers_

      tickers = tickers.map(&:upcase).uniq

      if threads
        threads = [tickers.length, Multitasking.cpu_count * 2].min if threads == true
        Multitasking.set_max_threads(threads)
        tickers.each_with_index do |ticker, i|
          _download_one_threaded(ticker, period: period, interval: interval,
                                 start: start, fin: fin, prepost: prepost,
                                 actions: actions, auto_adjust: auto_adjust,
                                 back_adjust: back_adjust, repair: repair,
                                 keepna: keepna, progress: (progress && i.positive?),
                                 proxy: proxy, rounding: rounding, timeout: timeout)
        end
        sleep 0.01 until @shared::_DFS.length == tickers.length
      else
        tickers.each_with_index do |ticker, i|
          data = _download_one(ticker, period: period, interval: interval,
                               start: start, fin: fin, prepost: prepost,
                               actions: actions, auto_adjust: auto_adjust,
                               back_adjust: back_adjust, repair: repair,
                               keepna: keepna, proxy: proxy,
                               rounding: rounding, timeout: timeout)
          @shared::_PROGRESS_BAR.animate if progress
        end
      end

      @shared::_PROGRESS_BAR.completed if progress

      unless @shared::_ERRORS.empty?
        # logger.error("\n#{@shared::_ERRORS.length} Failed download#{@shared::_ERRORS.length > 1 ? 's' : ''}:")

        errors = {}
        @shared::_ERRORS.each do |ticker, err|
          err = err.gsub(/%ticker%/, ticker)
          errors[err] ||= []
          errors[err] << ticker
        end
        # errors.each do |err, tickers|
        #   logger.error("#{tickers.join(', ')}: #{err}")
        # end

        tbs = {}
        @shared::_TRACEBACKS.each do |ticker, tb|
          tb = tb.gsub(/%ticker%/, ticker)
          tbs[tb] ||= []
          tbs[tb] << ticker
        end
        # tbs.each do |tb, tickers|
        #   logger.debug("#{tickers.join(', ')}: #{tb}")
        # end
      end

      if ignore_tz
        @shared::_DFS.each do |tkr, df|
          next if df.nil? || df.empty?
          @shared::_DFS[tkr].index = df.index.tz_localize(nil)
        end
      end

      if tickers.length == 1
        ticker = tickers.first
        return @shared::_DFS[ticker]
      end

      begin
        data = Polars::concat(@shared::_DFS.values, axis: 1, sort: true,
                              keys: @shared::_DFS.keys, names: ['Ticker', 'Price'])
      rescue
        _realign_dfs
        data = Polars::concat(@shared::_DFS.values, axis: 1, sort: true,
                              keys: @shared::_DFS.keys, names: ['Ticker', 'Price'])
      end
      data.index = Polars.to_datetime(data.index)
      data.rename(columns: @shared::_ISINS, inplace: true)

      if group_by == 'column'
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level: 0, axis: 1, inplace: true)
      end

      data
    end

    def _realign_dfs
      idx_len = 0
      idx = nil

      @shared::_DFS.values.each do |df|
        if df.length > idx_len
          idx_len = df.length
          idx = df.index
        end
      end

      @shared::_DFS.each do |key, df|
        begin
          @shared::_DFS[key] = Polars::DataFrame.new(index: idx, data: df).drop_duplicates
        rescue
          @shared::_DFS[key] = Polars.concat([YfAsDataframe::Utils.empty_df(idx), df.dropna], axis: 0, sort: true)
        end

        @shared::_DFS[key] = @shared::_DFS[key].loc[!@shared::_DFS[key].index.duplicated(keep: 'last')]
      end
    end

    Multitasking.task :_download_one_threaded do |ticker, start: nil, fin: nil,
        auto_adjust: false, back_adjust: false,
        repair: false, actions: false,
        progress: true, period: "max",
        interval: "1d", prepost: false,
        proxy: nil, keepna: false,
        rounding: false, timeout: 10|
      _download_one(ticker, start, fin, auto_adjust, back_adjust, repair,
                    actions, period, interval, prepost, proxy, rounding,
                    keepna, timeout)
      @shared::_PROGRESS_BAR.animate if progress
    end

    def _download_one(ticker, start: nil, fin: nil,
                      auto_adjust: false, back_adjust: false, repair: false,
                      actions: false, period: "max", interval: "1d",
                      prepost: false, proxy: nil, rounding: false,
                      keepna: false, timeout: 10)
      data = nil
      begin
        data = Ticker.new(ticker).history(
          period: period, interval: interval,
          start: start, fin: fin, prepost: prepost,
          actions: actions, auto_adjust: auto_adjust,
          back_adjust: back_adjust, repair: repair, proxy: proxy,
          rounding: rounding, keepna: keepna, timeout: timeout,
          raise_errors: true
        )
      rescue Exception => e
        @shared::_DFS[ticker.upcase] = YfAsDataframe::Utils.empty_df
        @shared::_ERRORS[ticker.upcase] = e.to_s
        @shared::_TRACEBACKS[ticker.upcase] = e.backtrace.join("\n")
      else
        @shared::_DFS[ticker.upcase] = data
      end

      data
    end
  end
end


