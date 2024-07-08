# require 'requests'
# require 'requests_cache'
require 'thread'
require 'date'
require 'nokogiri'
require 'zache'
require 'httparty'

class Yfinrb
  module YfConnection
    extend ActiveSupport::Concern
    # extend HTTParty

    # """
    # Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations.
    # """
    @@user_agent_headers = {
      'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
      # 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
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

      response = ::HTTParty.get(u, options)

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
