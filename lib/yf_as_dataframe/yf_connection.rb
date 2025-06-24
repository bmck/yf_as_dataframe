# require 'requests'
# require 'requests_cache'
require 'thread'
require 'date'
require 'nokogiri'
require 'zache'
require 'httparty'

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
    end


    def get(url, headers=nil, params=nil)
      # Important: treat input arguments as immutable.
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} url = #{url}, headers = #{headers}, params=#{params.inspect}" }

      headers ||= {}
      params ||= {}
      params.merge!(crumb: @@crumb) unless @@crumb.nil?
      cookie, crumb, strategy = _get_cookie_and_crumb()
      crumbs = !crumb.nil? ? {'crumb' => crumb} : {}

      request_args = {
        url: url,
        params: params.merge(crumbs),
        headers: headers || {}
      }

      proxy = _get_proxy
      ::HTTParty.http_proxy(addr = proxy.split(':').first, port = proxy.split(':').second.split('/').first) unless proxy.nil?

      cookie_hash = ::HTTParty::CookieHash.new
      cookie_hash.add_cookies(@@cookie)
      options = { headers: headers.dup.merge(@@user_agent_headers).merge({ 'cookie' => cookie_hash.to_cookie_string, 'crumb' => crumb })} #,  debug_output: STDOUT }

      u = (request_args[:url]).dup.to_s
      joiner = ('?'.in?(request_args[:url]) ? '&' : '?')
      u += (joiner + CGI.unescape(request_args[:params].to_query)) unless request_args[:params].empty?

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} u=#{u}, options = #{options.inspect}" }
      response = ::HTTParty.get(u, options)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} response=#{response.inspect}" }

      return response
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

    def _get_cookie_and_crumb()
      cookie, crumb, strategy = nil, nil, nil
      # puts "cookie_mode = '#{@@cookie_strategy}'"

      @@cookie_lock.synchronize do
        if @@cookie_strategy == 'csrf'
          crumb = _get_crumb_csrf()
          if crumb.nil?
            # Fail
            _set_cookie_strategy('basic', have_lock=true)
            cookie, crumb = __get_cookie_and_crumb_basic()
            # Rails.logger.info { "#{__FILE__}:#{__LINE__} cookie = #{cookie}, crumb = #{crumb}" }
          end
        else
          # Fallback strategy
          cookie, crumb = __get_cookie_and_crumb_basic()
          # Rails.logger.info { "#{__FILE__}:#{__LINE__} cookie = #{cookie}, crumb = #{crumb}" }
          if cookie.nil? || crumb.nil?
            # Fail
            _set_cookie_strategy('csrf', have_lock=true)
            crumb = _get_crumb_csrf()
          end
        end
        strategy = @@cookie_strategy
      end

      # Rails.logger.info { "#{__FILE__}:#{__LINE__} cookie = #{cookie}, crumb = #{crumb}, strategy=#{strategy}" }
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
      return nil if (cookie = _get_cookie_basic()).nil?

      cookie_hash = ::HTTParty::CookieHash.new
      cookie_hash.add_cookies(cookie)
      options = {headers: @@user_agent_headers.dup.merge(
                   { 'cookie' => cookie_hash.to_cookie_string }
      )} #,  debug_output: STDOUT }

      crumb_response = ::HTTParty.get('https://query1.finance.yahoo.com/v1/test/getcrumb', options)
      @@crumb = crumb_response.parsed_response

      return (@@crumb.nil? || '<html>'.in?(@@crumb)) ? nil : @@crumb
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
      return nil unless _get_cookie_csrf().present?

      get_args = {
        url: 'https://query2.finance.yahoo.com/v1/test/getcrumb',
        headers: @@user_agent_headers
      }

      get_args[:expire_after] = @expire_after if @session_is_caching
      r = @session.get(**get_args)

      @@crumb = r.text

      # puts "Didn't receive crumb"
      return nil if @@crumb.nil? || '<html>'.in?(@@crumb) || @@crumb.length.zero?
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
  end
end
