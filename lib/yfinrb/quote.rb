
class Yfin
  module Quote
    extend ActiveSupport::Concern

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_quote # (your module code here)
        end
      end
    end

    def initialize_quote
      @info = nil
      @retired_info = nil
      @sustainability = nil
      @recommendations = nil
      @upgrades_downgrades = nil
      @calendar = nil

      @already_scraped = false
      @already_fetched = false
      @already_fetched_complementary = false
    end

    def info #(self)
      if @info.nil?
        _fetch_info() #(@proxy)
        _fetch_complementary()  #(@proxy)
      end
      return @info
    end

    def sustainability
      raise YFNotImplementedError.new('sustainability') if @sustainability.nil?
      return @sustainability
    end

    def recommendations
      if @recommendations.nil?

        result = _fetch(['recommendationTrend'])
        if result.nil?
          @recommendations = Utils.empty_df()  #Polars::DataFrame()
        else
          begin
            data = result["quoteSummary"]["result"][0]["recommendationTrend"]["trend"]
          rescue KeyError, IndexError => e
            raise YfinDataException(f"Failed to parse json response from Yahoo Finance: #{e.result}")
          end
          @recommendations = Utils.empty_df(data) #Polars::DataFrame(data)
        end
      end
      return @recommendations
    end

    alias_method :recommendation_summary, :recommendations
    alias_method :recommendations_summary, :recommendations

    def upgrades_downgrades
      if @upgrades_downgrades.nil?
        result = _fetch(['upgradeDowngradeHistory'])

        if result.nil?
          @upgrades_downgrades = Utils.empty_df()  #Polars::DataFrame()
        else
          begin
            data = result["quoteSummary"]["result"][0]["upgradeDowngradeHistory"]["history"]

            raise YfinDataException("No upgrade/downgrade history found for #{ticker.symbol}") if len(data).zero?

            df = Polars::DataFrame(data)
            df.rename({"epochGradeDate" => "GradeDate", 'firm' => 'Firm', 'toGrade' => 'ToGrade', 'fromGrade' => 'FromGrade', 'action' => 'Action'})
            # df.set_index('GradeDate', inplace=true)
            # df.index = pd.to_datetime(df.index, unit='s')
            @upgrades_downgrades = df
          rescue KeyError, IndexError => e
            raise YfinDataException("Failed to parse json response from Yahoo Finance: #{e.result}")
          end
        end
      end
      return @upgrades_downgrades
    end

    def calendar
      self._fetch_calendar() if @calendar.nil?
      return @calendar
    end

    def valid_modules()
      return QUOTE_SUMMARY_VALID_MODULES
    end

    # quote_methods = [:info, :sustainability, :recommendations, :recommendations_summary, :recommendation_summary, \
    #                  :upgrades_downgrades, :calendar]
    # quote_methods.each do |meth|
    #   # define_method "get_#{meth}".to_sym do
    #   #   data = @quote.send(meth.to_sym)
    #   #   return data
    #   # end
    #   alias_method "get_#{meth}".to_sym, meth
    # end






    private

    def _fetch(modules)  #(self, proxy, modules: list)
      raise YfinException("Should provide a list of modules, see available modules using `valid_modules`") if !modules.is_a?(Array)

      modules = modules.intersection(QUOTE_SUMMARY_VALID_MODULES)  #[m for m in modules if m in quote_summary_valid_modules])

      raise YfinException("No valid modules provided, see available modules using `valid_modules`") if modules.empty?

      params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "formatted": "false", "symbol": symbol}

      begin
        result = get_raw_json(QUOTE_SUMMARY_URL + "/#{symbol}", user_agent_headers=user_agent_headers, params=params_dict)
      rescue HTTPError => e
        Rails.logger.error(str(e))
        return nil
      end
      return result
    end

    def _format(k, v)
      v2 = nil
      if isinstance(v, dict) && "raw".in?(v) && "fmt".in?(v)
        v2 = k.in?(["regularMarketTime", "postMarketTime"]) ? v["fmt"] : v["raw"]
      elsif isinstance(v, list)
        v2 = v.map{|vv| _format(nil, vv)}
      elsif isinstance(v, dict)
        v2 = v.items().map{|k,x| _format(k,x) } #{k: _format(k, x) for k, x in v.items()}
      elsif isinstance(v, str)
        v2 = v.replace("\xa0", " ")
      else
        v2 = v
      end
      return v2

      query1_info.items().each do |k,v|
        query1_info[k] = _format(k, v)
        @info = query1_info
      end
    end

    def _fetch_complementary()   #(proxy) # (self, proxy)
      return if @already_fetched_complementary
      @already_fetched_complementary = true

      # self._scrape(proxy)  # decrypt broken
      self._fetch_info() #(proxy)
      return if @info.nil?

      # Complementary key-statistics. For now just want 'trailing PEG ratio'
      keys = ["trailingPegRatio"] #{"trailingPegRatio"}
      if keys
        # Simplified the original scrape code for key-statistics. Very expensive for fetching
        # just one value, best if scraping most/all:
        #
        # p = _re.compile(r'root\.App\.main = (.*);')
        # url = 'https://finance.yahoo.com/quote/{}/key-statistics?p={}'.format(self._ticker.ticker, self._ticker.ticker)
        # try:
        #     r = session.get(url, headers=utils.user_agent_headers)
        #     data = _json.loads(p.findall(r.text)[0])
        #     key_stats = data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']["timeSeries"]
        #     for k in keys:
        #         if k not in key_stats or len(key_stats[k])==0:
        #             # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
        #             v = nil
        #         else:
        #             # Select most recent (last) raw value in list:
        #             v = key_stats[k][-1]["reportedValue"]["raw"]
        #         self._info[k] = v
        # except Exception:
        #     raise
        #     pass
        #
        # For just one/few variable is faster to query directly:
        url = "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/#{symbol}?symbol=#{symbol}"
        keys.each { |k| url += "&type=" + k }

        # Request 6 months of data
        start = (DateTime.now.utc.midnight - 6.months).to_i #datetime.timedelta(days=365 // 2)
        # start = int(start.timestamp())

        ending = DateTime.tomorrow.utc.midnight.to_i
        # ending = int(ending.timestamp())
        url += f"&period1={start}&period2={ending}"

        json_str = get(url).parsed_response # , proxy=proxy).parsed_response   #@data.cache_get(url=url, proxy=proxy).text
        # json_data = json.loads(json_str)
        json_result = json_data.try(:[],"timeseries") or json_data.try(:[], "finance")
        unless json_result["error"].nil?
          raise YfinException("Failed to parse json response from Yahoo Finance: " + str(json_result["error"]))

          keys.each do |k|
            keydict = json_result["result"][0]

            @info[k] = k.in?(keydict) ? keydict[k][-1]["reportedValue"]["raw"] : nil
          end
        end
      end
    end

    def _fetch_calendar
      begin
        # secFilings return too old data, so not requesting it for now
        result = self._fetch(modules=['calendarEvents']) #(@proxy, modules=['calendarEvents'])
        if result.nil?
          @calendar = {}
          return
        end

      rescue KeyError, IndexError => e
        raise YfinDataException("Failed to parse json response from Yahoo Finance: #{e.result}")
      rescue => e
        @calendar = {} #dict()
        _events = result["quoteSummary"]["result"][0]["calendarEvents"]
        if 'dividendDate'.in?(_events)
          @calendar['Dividend Date'] = datetime.datetime.fromtimestamp(_events['dividendDate']).date()
          if 'exDividendDate'.in?(_events)
            @calendar['Ex-Dividend Date'] = datetime.datetime.fromtimestamp(_events['exDividendDate']).date()
            # splits = _events.get('splitDate')  # need to check later, i will add code for this if found data
            earnings = _events.get('earnings')
            if !earnings.nil?
              @calendar['Earnings Date'] = earnings.get('earningsDate', []).map{|d| d.to_date }  # [datetime.datetime.fromtimestamp(d).date() for d in earnings.get('earningsDate', [])]
              @calendar['Earnings High'] = earnings.get('earningsHigh', nil)
              @calendar['Earnings Low'] = earnings.get('earningsLow', nil)
              @calendar['Earnings Average'] = earnings.get('earningsAverage', nil)
              @calendar['Revenue High'] = earnings.get('revenueHigh', nil)
              @calendar['Revenue Low'] = earnings.get('revenueLow', nil)
              @calendar['Revenue Average'] = earnings.get('revenueAverage', nil)
              # Likely need to decipher and restore the following
              # except
            end
          end
        end
      end
    end

    def _fetch_info()
      return if @already_fetched

      @already_fetched = true
      modules = ['financialData', 'quoteType', 'defaultKeyStatistics', 'assetProfile', 'summaryDetail']

      result = self._fetch(modules=modules)
      if result.nil?
        @info = {}
        return
      end

      result["quoteSummary"]["result"][0]["symbol"] = @symbol
      # Likely need to decipher and restore the following
      # query1_info = next(
      #   (info for info in result.get("quoteSummary", {}).get("result", []) if info["symbol"] == @symbol),
      #   nil,
      # )

      # Most keys that appear in multiple dicts have same value. Except 'maxAge' because
      # Yahoo not consistent with days vs seconds. Fix it here:

      query1_info.each {|k|
        query1_info[k]["maxAge"] = 86400 if "maxAge".in?(query1_info[k]) && query1_info[k]["maxAge"] == 1
      }

      # Likely need to decipher and restore the following
      # query1_info = {
      #   k1: v1
      #   for k, v in query1_info.items()
      #     if isinstance(v, dict)
      #       for k1, v1 in v.items()
      #         if v1
      #           }
      #           # recursively format but only because of 'companyOfficers'

    end


    BASE_URL = 'https://query2.finance.yahoo.com'
    QUOTE_SUMMARY_URL = "#{BASE_URL}/v10/finance/quoteSummary"

    QUOTE_SUMMARY_VALID_MODULES = [
      "summaryProfile",  # contains general information about the company
      "summaryDetail",  # prices + volume + market cap + etc
      "assetProfile",  # summaryProfile + company officers
      "fundProfile",
      "price",  # current prices
      "quoteType",  # quoteType
      "esgScores",  # Environmental, social, and governance (ESG) scores, sustainability and ethical performance of companies
      "incomeStatementHistory",
      "incomeStatementHistoryQuarterly",
      "balanceSheetHistory",
      "balanceSheetHistoryQuarterly",
      "cashFlowStatementHistory",
      "cashFlowStatementHistoryQuarterly",
      "defaultKeyStatistics",  # KPIs (PE, enterprise value, EPS, EBITA, and more)
      "financialData",  # Financial KPIs (revenue, gross margins, operating cash flow, free cash flow, and more)
      "calendarEvents",  # future earnings date
      "secFilings",  # SEC filings, such as 10K and 10Q reports
      "upgradeDowngradeHistory",  # upgrades and downgrades that analysts have given a company's stock
      "institutionOwnership",  # institutional ownership, holders and shares outstanding
      "fundOwnership",  # mutual fund ownership, holders and shares outstanding
      "majorDirectHolders",
      "majorHoldersBreakdown",
      "insiderTransactions",  # insider transactions, such as the number of shares bought and sold by company executives
      "insiderHolders",  # insider holders, such as the number of shares held by company executives
      "netSharePurchaseActivity",  # net share purchase activity, such as the number of shares bought and sold by company executives
      "earnings",  # earnings history
      "earningsHistory",
      "earningsTrend",  # earnings trend
      "industryTrend",
      "indexTrend",
      "sectorTrend",
      "recommendationTrend",
      "futuresChain",
    ].freeze

  end
end
