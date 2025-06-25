require 'active_support'
require 'active_support/concern'
# require 'requests'
# require 'requests_cache'
require 'thread'
require 'date'
require 'nokogiri'
require 'zache'
require 'httparty'
require 'uri'
require 'json'

class YfAsDataframe
  module YfConnection
    extend ::ActiveSupport::Concern
    # extend HTTParty

    # """
    # Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations.
    # """
    @@user_agent_headers_selection = [
      # Chrome - Desktop
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",       # Windows
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",  # Mac
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",                # Linux
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",       # Windows
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36", # Mac
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",                # Linux

      # Chrome - Mobile
      "Mozilla/5.0 (Linux; Android 15; SM-S931B Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/127.0.6533.103 Mobile Safari/537.36",  # Samsung S25
      "Mozilla/5.0 (Linux; Android 15; Pixel 8 Pro Build/AP4A.250105.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.6834.163 Mobile Safari/537.36",   # Pixel 8 Pro
      "Mozilla/5.0 (Linux; Android 14; Pixel 9 Pro Build/AD1A.240418.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/124.0.6367.54 Mobile Safari/537.36",    # Pixel 9 Pro
      "Mozilla/5.0 (Linux; Android 14; SM-S928B/DS) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36",                                         # Samsung S24 Ultra

      # Firefox - Desktop
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",       # Windows
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:135.0) Gecko/20100101 Firefox/135.0",    # Mac
      "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",                # Linux
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",       # Windows
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:136.0) Gecko/20100101 Firefox/136.0",    # Mac
      "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",                # Linux

      # Firefox - Mobile
      "Mozilla/5.0 (Android 15; Mobile; SM-G556B/DS; rv:130.0) Gecko/130.0 Firefox/130.0",     # Samsung Xcover7
      "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36", # Pixel 7 Pro
      "Mozilla/5.0 (Linux; Android 13; Pixel 6 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36", # Pixel 6 Pro
      "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",           # Generic Android

      # Safari - Desktop
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",      # Mac
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.10 Safari/605.1.15",     # Mac

      # Safari - Mobile
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_7_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1", # iPhone
      "Mozilla/5.0 (iPad; CPU OS 17_7_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",         # iPad

      # Edge - Desktop
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",           # Windows
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/131.0.2903.86",        # Windows
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",            # Windows

      # Edge - Mobile
      "Mozilla/5.0 (Linux; Android 10; OnePlus HD1913) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36 EdgA/134.0.0.0", # Android

      # Opera - Desktop
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/117.0.0.0",            # Windows

      # Opera - Mobile
      "Mozilla/5.0 (Linux; Android 10; Huawei VOG-L29) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.2.4027.0 Mobile Safari/537.36 OPR/76.2.4027.0" # Android
    ]

    # adding more headers that a browser would often send. it seems they've recently implemented fingerprinting. We're not fingerprinting yet, but this could be closer
    @@user_agent_headers = {
      "User-Agent" => @@user_agent_headers_selection.sample,
      "Accept" => "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
      "Accept-Language" => "en-US,en;q=0.9",
      "Accept-Encoding" => "gzip, deflate, br",
      "Referer" => "https:://finance.yahoo.com/",
      "Cache-Control" => "max-age=0",
      "Connection" => "keep-alive"
    }
    @@proxy = nil

    cattr_accessor :user_agent_headers, :proxy

    def yfconn_initialize
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} here"}
      begin
        @@zache = ::Zache.new
        @@session_is_caching = true
      rescue NoMethodError
        # Not caching
        @@session_is_caching = false
      end

      @@crumb = nil
      @@cookie = nil
      @@cookie_strategy = 'basic'
      @@cookie_lock = ::Mutex.new()
      
      # Add session tracking
      @@session_created_at = Time.now
      @@session_refresh_interval = 3600 # 1 hour
      @@request_count = 0
      @@last_request_time = nil
      
      # Circuit breaker state
      @@circuit_breaker_state = :closed # :closed, :open, :half_open
      @@failure_count = 0
      @@last_failure_time = nil
      @@circuit_breaker_threshold = 3
      @@circuit_breaker_timeout = 60 # seconds
      @@circuit_breaker_base_timeout = 60 # seconds
    end


    def get(url, headers=nil, params=nil)
      # Check circuit breaker first
      unless circuit_breaker_allow_request?
        raise RuntimeError.new("Circuit breaker is open - too many recent failures. Please try again later.")
      end

      # Add request throttling to be respectful of rate limits
      throttle_request
      
      # Track session usage
      track_session_usage
      
      # Refresh session if needed
      refresh_session_if_needed
      
      # Only fetch crumb for /v7/finance/download endpoint
      crumb_needed = url.include?('/v7/finance/download')

      headers ||= {}
      params ||= {}
      # params.merge!(crumb: @@crumb) unless @@crumb.nil? # Commented out: crumb not needed for most endpoints
      if crumb_needed
        crumb = get_crumb_scrape_quote_page(params[:symbol] || params['symbol'])
        params.merge!(crumb: crumb) unless crumb.nil?
      end
      cookie, _, strategy = _get_cookie_and_crumb(crumb_needed)
      crumbs = {} # crumb logic handled above if needed

      request_args = {
        url: url,
        params: params.merge(crumbs),
        headers: headers || {}
      }

      proxy = _get_proxy
      ::HTTParty.http_proxy(addr = proxy.split(':').first, port = proxy.split(':').second.split('/').first) unless proxy.nil?

      cookie_hash = ::HTTParty::CookieHash.new
      cookie_hash.add_cookies(@@cookie)
      options = { headers: headers.dup.merge(@@user_agent_headers).merge({ 'cookie' => cookie_hash.to_cookie_string })} #,  debug_output: STDOUT }

      u = (request_args[:url]).dup.to_s
      joiner = (request_args[:url].include?('?') ? '&' : '?')
      u += (joiner + URI.encode_www_form(request_args[:params])) unless request_args[:params].empty?

      begin
        response = ::HTTParty.get(u, options)
        if response_failure?(response)
          circuit_breaker_record_failure
          raise RuntimeError.new("Yahoo Finance request failed: #{response.code} - #{response.body}")
        end
        circuit_breaker_record_success
        return response
      rescue => e
        circuit_breaker_record_failure
        raise e
      end
    end

    alias_method :cache_get, :get


    def get_raw_json(url, user_agent_headers=nil, params=nil)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} url = #{url.inspect}" }
      response = get(url, user_agent_headers, params)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} response = #{response.inspect}" }
      # response.raise_for_status()
      return response   #.json()
    end







    private


    def _get_proxy
      # setup proxy in requests format
      proxy = nil
      unless proxy.nil?
        proxy = {"https" => @@proxy["https"]} if @@proxy.is_a?(Hash) && @@proxy.include?("https")
      end

      return proxy
    end

    def _set_cookie_strategy(strategy, have_lock=false)
      return if strategy == @@cookie_strategy

      if !have_lock
        @@cookie_lock.synchronize do
          @@cookie_strategy = strategy
          @@cookie = nil
          @@crumb = nil
        end
      end
    end

    def _get_cookie_and_crumb(crumb_needed=false)
      cookie, crumb, strategy = nil, nil, nil
      @@cookie_lock.synchronize do
        if crumb_needed
          cookie, crumb = __get_cookie_and_crumb_basic()
        else
          cookie = _get_cookie_basic()
          crumb = nil
        end
        strategy = @@cookie_strategy
      end
      return cookie, crumb, strategy
    end

    def __get_cookie_and_crumb_basic()
      cookie = _get_cookie_basic()
      crumb = _get_crumb_basic()
      return cookie, crumb
    end

    def _get_cookie_basic()
      @@cookie ||= _load_cookie_basic()
      return @@cookie unless @@cookie.nil? || @@cookie.length.zero?

      headers = @@user_agent_headers.dup

      response = HTTParty.get('https://fc.yahoo.com', headers) #.merge(debug_output: STDOUT))

      cookie = response.headers['set-cookie']
      cookies ||= ''
      cookies += cookie.split(';').first
      @@cookie = cookies;

      _save_cookie_basic(@@cookie)

      return @@cookie
    end

    def _get_crumb_basic()
      return @@crumb unless @@crumb.nil?
      
      # Retry logic similar to yfinance: try up to 3 times
      3.times do |attempt|
        begin
          # Clear cookie on retry (except first attempt) to get fresh session
          if attempt > 0
            @@cookie = nil
            # Clear curl-impersonate executables cache to force re-selection
            CurlImpersonateIntegration.instance_variable_set(:@available_executables, nil)
            # warn "[yf_as_dataframe] Retrying crumb fetch (attempt #{attempt + 1}/3)"
            # Add delay between retries to be respectful of rate limits
            sleep(2 ** attempt) # Exponential backoff: 2s, 4s, 8s
          end
          
          return nil if (cookie = _get_cookie_basic()).nil?

          cookie_hash = ::HTTParty::CookieHash.new
          cookie_hash.add_cookies(cookie)
          options = {headers: @@user_agent_headers.dup.merge(
                       { 'cookie' => cookie_hash.to_cookie_string }
          )}

          crumb_response = ::HTTParty.get('https://query1.finance.yahoo.com/v1/test/getcrumb', options)
          @@crumb = crumb_response.parsed_response

          # Validate crumb: must be short, alphanumeric, no spaces, not an error message
          if crumb_valid?(@@crumb)
            # warn "[yf_as_dataframe] Successfully fetched valid crumb on attempt #{attempt + 1}"
            return @@crumb
          else
            # warn "[yf_as_dataframe] Invalid crumb received on attempt #{attempt + 1}: '#{@@crumb.inspect}'"
            @@crumb = nil
          end
        rescue => e
          # warn "[yf_as_dataframe] Error fetching crumb on attempt #{attempt + 1}: #{e.message}"
          @@crumb = nil
        end
      end
      
      # All attempts failed
      # warn "[yf_as_dataframe] Failed to fetch valid crumb after 3 attempts"
      raise "Could not fetch a valid Yahoo Finance crumb after 3 attempts"
    end

    def crumb_valid?(crumb)
      return false if crumb.nil?
      return false if crumb.include?('<html>')
      return false if crumb.include?('Too Many Requests')
      return false if crumb.strip.empty?
      return false if crumb.length < 8 || crumb.length > 20
      return false if crumb =~ /\s/
      true
    end

    def _get_cookie_csrf()
      return true unless @@cookie.nil?
      return (@@cookie = true) if _load_session_cookies()

      base_args = {
        headers: @@user_agent_headers,
        # proxies: proxy,
      }

      get_args = base_args.merge({url: 'https://guce.yahoo.com/consent'})

      get_args[:expire_after] = @expire_after if @session_is_caching
      response = @session.get(**get_args)

      soup = ::Nokogiri::HTML(response.content, 'html.parser')
      csrfTokenInput = soup.find('input', attrs: {'name': 'csrfToken'})

      # puts 'Failed to find "csrfToken" in response'
      return false if csrfTokenInput.nil?

      csrfToken = csrfTokenInput['value']
      # puts "csrfToken = #{csrfToken}"
      sessionIdInput = soup.find('input', attrs: {'name': 'sessionId'})
      sessionId = sessionIdInput['value']
      # puts "sessionId='#{sessionId}"

      originalDoneUrl = 'https://finance.yahoo.com/'
      namespace = 'yahoo'
      data = {
        'agree': ['agree', 'agree'],
        'consentUUID': 'default',
        'sessionId': sessionId,
        'csrfToken': csrfToken,
        'originalDoneUrl': originalDoneUrl,
        'namespace': namespace,
      }
      post_args = base_args.merge(
        {
          url: "https://consent.yahoo.com/v2/collectConsent?sessionId=#{sessionId}",
          data: data
        }
      )
      get_args = base_args.merge(
        {
          url: "https://guce.yahoo.com/copyConsent?sessionId=#{sessionId}",
          data: data
        }
      )
      if @session_is_caching
        post_args[:expire_after] = @expire_after
        get_args[:expire_after] = @expire_after
      end
      @session.post(**post_args)
      @session.get(**get_args)

      @@cookie = true
      _save_session_cookies()

      return true
    end

    def _get_crumb_csrf()
      # Credit goes to @bot-unit #1729

      # puts 'reusing crumb'
      return @@crumb unless @@crumb.nil?
      # This cookie stored in session
      cookie_csrf = _get_cookie_csrf()
      return nil if cookie_csrf.nil? || (cookie_csrf.respond_to?(:empty?) && cookie_csrf.empty?)

      get_args = {
        url: 'https://query2.finance.yahoo.com/v1/test/getcrumb',
        headers: @@user_agent_headers
      }

      get_args[:expire_after] = @expire_after if @session_is_caching
      r = @session.get(**get_args)

      @@crumb = r.text

      # puts "Didn't receive crumb"
      return nil if @@crumb.nil? || @@crumb.include?('<html>') || @@crumb.length.zero?
      return @@crumb
    end





    def _save_session_cookies()
      begin
        @@zache.put(:csrf, @session.cookies, lifetime: 60 * 60 * 24)
      rescue Exception
        return false
      end
      return true
    end

    def _load_session_cookies()
      return false if @@zache.expired?(:csrf)
      @session.cookies = @@zache.get(:csrf)
    end

    def _save_cookie_basic(cookie)
      begin
        @@zache.put(:basic, cookie, lifetime: 60*60*24)
      rescue Exception
        return false
      end
      return true
    end

    def _load_cookie_basic()
      @@zache.put(:basic, nil, lifetime: 1) unless @@zache.exists?(:basic, dirty: false)
      return @@zache.expired?(:basic) ? nil : @@zache.get(:basic)
    end

    def throttle_request
      # Random delay between 0.1 and 0.5 seconds to be respectful of rate limits
      # Similar to yfinance's approach
      sleep(rand(0.1..0.5))
    end

    def track_session_usage
      @@request_count += 1
      @@last_request_time = Time.now
    end

    def refresh_session_if_needed
      return unless session_needs_refresh?
      
      # warn "[yf_as_dataframe] Refreshing session (age: #{session_age} seconds, requests: #{@@request_count})"
      refresh_session
    end

    def session_needs_refresh?
      return true if session_age > @@session_refresh_interval
      return true if @@request_count > 100 # Refresh after 100 requests
      return true if @@cookie.nil? || @@crumb.nil?
      false
    end

    def session_age
      Time.now - @@session_created_at
    end

    def refresh_session
      @@cookie = nil
      @@crumb = nil
      @@session_created_at = Time.now
      @@request_count = 0
      # warn "[yf_as_dataframe] Session refreshed"
    end

    # Circuit breaker methods
    def circuit_breaker_allow_request?
      case @@circuit_breaker_state
      when :closed
        true
      when :open
        if Time.now - @@last_failure_time > @@circuit_breaker_timeout
          @@circuit_breaker_state = :half_open
          # warn "[yf_as_dataframe] Circuit breaker transitioning to half-open"
          true
        else
          false
        end
      when :half_open
        true
      end
    end

    def circuit_breaker_record_failure
      @@failure_count += 1
      @@last_failure_time = Time.now
      
      if @@failure_count >= @@circuit_breaker_threshold && @@circuit_breaker_state != :open
        @@circuit_breaker_state = :open
        # Exponential backoff: 60s, 120s, 240s, 480s, etc.
        @@circuit_breaker_timeout = @@circuit_breaker_base_timeout * (2 ** (@@failure_count - @@circuit_breaker_threshold))
        # warn "[yf_as_dataframe] Circuit breaker opened after #{@@failure_count} failures (timeout: #{@@circuit_breaker_timeout}s)"
      end
    end

    def circuit_breaker_record_success
      if @@circuit_breaker_state == :half_open
        @@circuit_breaker_state = :closed
        @@failure_count = 0
        @@circuit_breaker_timeout = @@circuit_breaker_base_timeout
        # warn "[yf_as_dataframe] Circuit breaker closed after successful request"
      elsif @@circuit_breaker_state == :closed
        # Reset failure count on success
        @@failure_count = 0
        @@circuit_breaker_timeout = @@circuit_breaker_base_timeout
      end
    end

    def response_failure?(response)
      return true if response.nil?
      return true if response.code >= 400
      return true if response.body.to_s.include?("Too Many Requests")
      return true if response.body.to_s.include?("Will be right back")
      return true if response.body.to_s.include?("<html>")
      false
    end

    def circuit_breaker_status
      {
        state: @@circuit_breaker_state,
        failure_count: @@failure_count,
        last_failure_time: @@last_failure_time,
        timeout: @@circuit_breaker_timeout,
        threshold: @@circuit_breaker_threshold
      }
    end

    # For /v7/finance/download, scrape crumb from quote page
    def get_crumb_scrape_quote_page(symbol)
      return nil if symbol.nil?
      url = "https://finance.yahoo.com/quote/#{symbol}"
      response = ::HTTParty.get(url, headers: @@user_agent_headers)
      # Look for root.App.main = { ... };
      m = response.body.match(/root\.App\.main\s*=\s*(\{.*?\});/m)
      return nil unless m
      json_blob = m[1]
      begin
        data = JSON.parse(json_blob)
        crumb = data.dig('context', 'dispatcher', 'stores', 'CrumbStore', 'crumb')
        # warn "[yf_as_dataframe] Scraped crumb from quote page: #{crumb.inspect}"
        return crumb
      rescue => e
        # warn "[yf_as_dataframe] Failed to parse crumb from quote page: #{e.message}"
        return nil
      end
    end
  end
end
