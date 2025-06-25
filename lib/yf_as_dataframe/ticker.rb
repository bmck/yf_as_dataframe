require 'tzinfo'
require 'logger'

class YfAsDataframe
  class Ticker
    ROOT_URL = 'https://finance.yahoo.com'.freeze
    BASE_URL = 'https://query2.finance.yahoo.com'.freeze

    include YfAsDataframe::YfConnection

    attr_accessor :tz, :proxy, :isin, :timeout
    attr_reader :error_message, :ticker

    class YahooFinanceException < Exception
    end

    class SymbolNotFoundException < YahooFinanceException
    end

    def initialize(ticker)
      @proxy = nil 
      @timeout = 30
      @tz = TZInfo::Timezone.get('America/New_York')

      @isin = nil
      @news = []
      @shares = nil

      @earnings_dates = {}
      @expirations = {}
      @underlying = {}

      @ticker = (YfAsDataframe::Utils.is_isin(ticker.upcase) ? YfAsDataframe::Utils.get_ticker_by_isin(ticker.upcase, nil, @session) : ticker).upcase

      yfconn_initialize
    end

    include YfAsDataframe::PriceHistory
    include YfAsDataframe::Analysis
    include YfAsDataframe::Fundamentals
    include YfAsDataframe::Holders
    include YfAsDataframe::Quote
    include YfAsDataframe::Financials

    alias_method :symbol, :ticker

    def symbol; @ticker; end

    def shares_full(start: nil, fin: nil)
      logger = Logger.new(STDOUT)

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start.inspect}, fin = #{fin.inspect}" } 

      if start
        start_ts = YfAsDataframe::Utils.parse_user_dt(start, tz)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} start_ts = #{start_ts}" }
        start = Time.at(start_ts).in_time_zone(tz)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start.inspect}, fin = #{fin.inspect}" } 
      end
      if fin
        end_ts = YfAsDataframe::Utils.parse_user_dt(fin, tz)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} end_ts = #{end_ts}" }
        fin = Time.at(end_ts).in_time_zone(tz)
        # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start.inspect}, fin = #{fin.inspect}" } 
      end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} start = #{start.inspect}, fin = #{fin.inspect}" } 

      dt_now = Time.now.in_time_zone(tz)
      fin ||= dt_now
      start ||= (fin - 548.days).midnight

      if start >= fin
        logger.error("Start date (#{start}) must be before end (#{fin})")
        return nil
      end

      ts_url_base = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/#{@ticker}?symbol=#{@ticker}"
      shares_url = "#{ts_url_base}&period1=#{start.to_i}&period2=#{fin.tomorrow.midnight.to_i}"

      begin
        json_data = get(shares_url).parsed_response
      rescue #_json.JSONDecodeError, requests.exceptions.RequestException
        logger.error("#{@ticker}: Yahoo web request for share count failed")
        return nil
      end

      fail = json_data["finance"]["error"]["code"] == "Bad Request" rescue false
      if fail
        logger.error("#{@ticker}: Yahoo web request for share count failed")
        return nil
      end

      shares_data = json_data["timeseries"]["result"]

      return nil if !shares_data[0].key?("shares_out")

      timestamps = shares_data[0]["timestamp"].map{|t| Time.at(t).to_datetime }

      df = Polars::DataFrame.new(
        {
          'Timestamps': timestamps,
          "Shares": shares_data[0]["shares_out"]
        }
      )

      return df
    end

    def shares
      return @shares unless @shares.nil?

      full_shares = shares_full(start: Time.now.utc.to_date-548.days, fin: Time.now.utc.to_date)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} full_shares = #{full_shares.inspect}" }

      # if shares.nil?
      #     # Requesting 18 months failed, so fallback to shares which should include last year
      #     shares = @ticker.get_shares()

      # if shares.nil?
      full_shares = full_shares['Shares'] if full_shares.is_a?(Polars::DataFrame)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} full_shares = #{full_shares.inspect}" }
      @shares = full_shares[-1].to_i
      # end
      # return @shares
    end

    def news()
      return @news unless @news.empty?

      url = "#{BASE_URL}/v1/finance/search?q=#{@ticker}"
      data = get(url).parsed_response
      if data.include?("Will be right back")
        raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\nOur engineers are working quickly to resolve the issue. Thank you for your patience.")
      end

      @news = {}
      data['news'].each do |item|
        @news[item['title']] = item['link']
      end

      return @news
    end

    def earnings_dates(limit = 12)
      #   """
      # Get earning dates (future and historic)
      # :param limit: max amount of upcoming and recent earnings dates to return.
      #               Default value 12 should return next 4 quarters and last 8 quarters.
      #               Increase if more history is needed.

      # :return: Polars dataframe
      # """
      return @earnings_dates[limit] if @earnings_dates && @earnings_dates[limit]

      logger = Logger.new(STDOUT)

      page_size = [limit, 100].min  # YF caps at 100, don't go higher
      page_offset = 0
      dates = nil
      # while true
        url = "#{ROOT_URL}/calendar/earnings?symbol=#{@ticker}&offset=#{page_offset}&size=#{page_size}"
        data = get(url).parsed_response # @data.cache_get(url: url).text

        if data.include?("Will be right back")
          raise RuntimeError, "*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\nOur engineers are working quickly to resolve the issue. Thank you for your patience."
        end

        csv = ''
        doc = Nokogiri::HTML(data)
        tbl = doc.xpath("//table").first
        tbl.search('tr').each do |tr|
          cells = tr.search('th, td')
          csv += CSV.generate_line(cells)
        end
        csv = CSV.parse(csv)

        df = {}
        (0..csv[0].length-1).each{|i| df[csv[0][i]] = csv[1..-1].transpose[i] }
        dates = Polars::DataFrame.new(df)
      # end

      # Drop redundant columns
      dates = dates.drop(["Symbol", "Company"]) #, axis: 1)

      # Convert types
      ["EPS Estimate", "Reported EPS", "Surprise(%)"].each do |cn|
        s = Polars::Series.new([Float::NAN] * (dates.shape.first))
        (0..(dates.shape.first-1)).to_a.each {|i| s[i] = dates[cn][i].to_f unless dates[cn][i] == '-' }
        dates.replace(cn, s)
      end

      # Convert % to range 0->1:
      dates["Surprise(%)"] *= 0.01

      # Parse earnings date string
      s = Polars::Series.new(dates['Earnings Date'].map{|t| Time.at(t.to_datetime.to_i).to_datetime }, dtype: :i64)
      dates.replace('Earnings Date', s)


      @earnings_dates[limit] = dates

      dates
    end

    def option_chain(date = nil, tz = nil)
      options = if date.nil?
        download_options
      else
        download_options if @expirations.empty? || date.nil?
        raise "Expiration `#{date}` cannot be found. Available expirations are: [#{@expirations.keys.join(', ')}]" unless @expirations.key?(date)

        download_options(@expirations[date])
      end

      df = OpenStruct.new(
        calls: _options_to_df(options['calls'], tz),
        puts: _options_to_df(options['puts'], tz),
        underlying: options['underlying']
      )
    end

    def options
      download_options if @expirations.empty?
      @expirations.keys
    end

    alias_method :option_expiration_dates, :options

    def is_valid_timezone(tz)
      begin
        _tz.timezone(tz)
      rescue UnknownTimeZoneError
        return false
      end
      return true
    end

    def to_s
      "yfinance.Ticker object <#{ticker}>"
    end

    def download_options(date = nil)
      url = date.nil? ? "#{BASE_URL}/v7/finance/options/#{@ticker}" : "#{BASE_URL}/v7/finance/options/#{@ticker}?date=#{date}"

      response = get(url).parsed_response  #Net::HTTP.get(uri)

      if response['optionChain'].key?('result') #r.dig('optionChain', 'result')&.any?
        response['optionChain']['result'][0]['expirationDates'].each do |exp|
          @expirations[Time.at(exp).utc.strftime('%Y-%m-%d')] = exp
        end

        @underlying = response['optionChain']['result'][0]['quote'] || {}

        opt = response['optionChain']['result'][0]['options'] || []

        return opt.empty? ? {} : opt[0].merge('underlying' => @underlying) 
      end
      {}
    end

    def isin()
      return @isin if !@isin.nil?

      # ticker = @ticker.upcase

      if ticker.include?("-") || ticker.include?("^")
        @isin = '-'
        return @isin
      end

      q = ticker
      @info ||= info
      return nil if @info.nil?

      # q = @info['quoteType'].try(:[],'shortName') # if @info.key?("shortName")

      url = "https://markets.businessinsider.com/ajax/SearchController_Suggest?max_results=25&query=#{(q)}"
      data = get(url).parsed_response

      search_str = "\"#{ticker}|"
      if !data.include?(search_str)
        if data.downcase.include?(q.downcase)
          search_str = '"|'
          if !data.include?(search_str)
            @isin = '-'
            return @isin
          end
        else
          @isin = '-'
          return @isin
        end
      end

      @isin = data.split(search_str)[1].split('"')[0].split('|')[0]
      return @isin
    end












    private

    # def _lazy_load_price_history
    #   @price_history ||= PriceHistory.new(@ticker, _get_ticker_tz(@proxy, timeout: 10), @data)
    # end

    alias_method :_get_ticker_tz, :tz 
    # def _get_ticker_tz(proxy=nil, timeout=nil)
    #   return @tz
    # end

    # def _fetch_ticker_tz(proxy, timeout)
    #   proxy ||= @proxy

    #   params = {"range": "1d", "interval": "1d"}

    #   url = "#{BASE_URL}/v8/finance/chart/#{@ticker}"

    #   begin
    #     data = @data.cache_get(url: url, params: params, proxy: proxy, timeout: timeout)
    #     data = data.json()
    #   rescue Exception => e
    #     Rails.logger.error("Failed to get ticker '#{@ticker}' reason: #{e}")
    #     return nil
    #   end

    #   error = data.get('chart', {}).get('error', nil)
    #   if error
    #     Rails.logger.debug("Got error from yahoo api for ticker #{@ticker}, Error: #{error}")
    #   else
    #     begin
    #       return data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
    #     rescue Exception => err
    #       Rails.logger.error("Could not get exchangeTimezoneName for ticker '#{@ticker}' reason: #{err}")
    #       Rails.logger.debug("Got response: ")
    #       Rails.logger.debug("-------------")
    #       Rails.logger.debug(" #{data}")
    #       Rails.logger.debug("-------------")
    #     end
    #   end

    #   return nil
    # end

    def _options_to_df(opt, tz = nil)
      data = opt.map do |o|
        {
          contractSymbol: o['contractSymbol'],
          lastTradeDate: DateTime.strptime(o['lastTradeDate'].to_s, '%s').new_offset(0),
          strike: o['strike'],
          lastPrice: o['lastPrice'],
          bid: o['bid'],
          ask: o['ask'],
          change: o['change'],
          percentChange: o['percentChange'],
          volume: o['volume'],
          openInterest: o['openInterest'],
          impliedVolatility: o['impliedVolatility'],
          inTheMoney: o['inTheMoney'],
          contractSize: o['contractSize'],
          currency: o['currency']
        }
      end

      if tz
        data.each do |d|
          d[:lastTradeDate] = d[:lastTradeDate].new_offset(tz)
        end
      end

      data
    end
  end
end
