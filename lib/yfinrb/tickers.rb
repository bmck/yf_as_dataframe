class Yfinrb
  class Tickers
    def initialize(tickers, session = nil)
      tickers = tickers.is_a?(Array) ? tickers : tickers.split(',')
      @symbols = tickers.map(&:upcase)
      @tickers = @symbols.each_with_object({}) do |ticker, hash|
        hash[ticker] = Ticker.new(ticker, session: session)
      end
    end

    def to_s
      "yfinance.Tickers object <#{@symbols.join(', ')}>"
    end

    def history(period: "1mo", interval: "1d", start: nil, fin: nil, prepost: false,
                actions: true, auto_adjust: true, repair: false, proxy: nil,
                threads: true, group_by: 'column', progress: true, timeout: 10, **kwargs)
      download(period: period, interval: interval, start: start, fin: fin, prepost: prepost,
               actions: actions, auto_adjust: auto_adjust, repair: repair, proxy: proxy,
               threads: threads, group_by: group_by, progress: progress, timeout: timeout, **kwargs)
    end

    def download(period: "1mo", interval: "1d", start: nil, fin: nil, prepost: false,
                 actions: true, auto_adjust: true, repair: false, proxy: nil,
                 threads: true, group_by: 'column', progress: true, timeout: 10, **kwargs)
      data = Multi.download(@symbols, start: start, fin: fin, actions: actions,
                            auto_adjust: auto_adjust, repair: repair, period: period,
                            interval: interval, prepost: prepost, proxy: proxy,
                            group_by: 'ticker', threads: threads, progress: progress,
                            timeout: timeout, **kwargs)

      @symbols.each do |symbol|
        @tickers[symbol]._history = data[symbol] if @tickers[symbol]
      end

      if group_by == 'column'
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level: 0, axis: 1, inplace: true)
      end

      data
    end

    def news
      @symbols.each_with_object({}) do |ticker, hash|
        hash[ticker] = Ticker.new(ticker).news
      end
    end
  end
end
