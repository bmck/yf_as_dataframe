# Minimal patch to make curl-impersonate the default behavior
# This file should be required after the main YfConnection class

require_relative 'curl_impersonate_integration'

class YfAsDataframe
  module YfConnection
    # Store original methods
    alias_method :get_original, :get
    alias_method :get_raw_json_original, :get_raw_json

    # Override get method to use curl-impersonate by default
    def get(url, headers=nil, params=nil)
      # Debug output
      # puts "DEBUG: curl_impersonate_enabled = #{CurlImpersonateIntegration.curl_impersonate_enabled}"
      # puts "DEBUG: curl_impersonate_fallback = #{CurlImpersonateIntegration.curl_impersonate_fallback}"
      
      # Try curl-impersonate first if enabled
      if CurlImpersonateIntegration.curl_impersonate_enabled
        # puts "DEBUG: Trying curl-impersonate..."
        begin
          # Prepare headers and params as in original method
          headers ||= {}
          params ||= {}
          
          # Only fetch crumb for /v7/finance/download endpoint
          crumb_needed = url.include?('/v7/finance/download')
          if crumb_needed
            crumb = get_crumb_scrape_quote_page(params[:symbol] || params['symbol'])
            params.merge!(crumb: crumb) unless crumb.nil?
          end
          
          cookie, _, strategy = _get_cookie_and_crumb(crumb_needed)
          crumbs = {} # crumb logic handled above if needed

          # Prepare headers for curl-impersonate
          curl_headers = headers.dup.merge(@@user_agent_headers)
          
          # Add cookie if available
          if cookie
            cookie_hash = ::HTTParty::CookieHash.new
            cookie_hash.add_cookies(cookie)
            curl_headers['Cookie'] = cookie_hash.to_cookie_string
          end

          # Add crumb if available
          curl_headers['crumb'] = crumb if crumb

          # Make curl-impersonate request with improved timeout handling
          response = CurlImpersonateIntegration.make_request(
            url, 
            headers: curl_headers, 
            params: params.merge(crumbs),
            timeout: CurlImpersonateIntegration.curl_impersonate_timeout,
            retries: CurlImpersonateIntegration.curl_impersonate_retries
          )

          if response && !response.empty?
            # puts "DEBUG: curl-impersonate succeeded"
            return response
          else
            # puts "DEBUG: curl-impersonate returned nil or failed"
          end
        rescue => e
          # puts "DEBUG: curl-impersonate exception: #{e.message}"
          # warn "curl-impersonate request failed: #{e.message}" if $VERBOSE
        end
      else
        # puts "DEBUG: curl-impersonate is disabled, skipping to fallback"
      end

      # Fallback to original HTTParty method
      if CurlImpersonateIntegration.curl_impersonate_fallback
        # puts "DEBUG: Using HTTParty fallback"
        return HTTParty.get(url, headers: headers).body
      else
        # puts "DEBUG: Fallback is disabled, but forcing fallback anyway"
        return HTTParty.get(url, headers: headers).body
      end
    end

    # get_raw_json uses get, so it automatically gets curl-impersonate behavior
    # No need to override it separately

    # Class-level configuration methods
    class << self
      def enable_curl_impersonate(enabled: true)
        CurlImpersonateIntegration.curl_impersonate_enabled = enabled
      end

      def enable_curl_impersonate_fallback(enabled: true)
        CurlImpersonateIntegration.curl_impersonate_fallback = enabled
      end

      def set_curl_impersonate_timeout(timeout)
        CurlImpersonateIntegration.curl_impersonate_timeout = timeout
      end

      def set_curl_impersonate_connect_timeout(timeout)
        CurlImpersonateIntegration.curl_impersonate_connect_timeout = timeout
      end

      def set_curl_impersonate_process_timeout(timeout)
        CurlImpersonateIntegration.curl_impersonate_process_timeout = timeout
      end

      def set_curl_impersonate_retries(retries)
        CurlImpersonateIntegration.curl_impersonate_retries = retries
      end

      def get_available_curl_impersonate_executables
        CurlImpersonateIntegration.available_executables
      end

      def get_curl_impersonate_config
        {
          enabled: CurlImpersonateIntegration.curl_impersonate_enabled,
          fallback: CurlImpersonateIntegration.curl_impersonate_fallback,
          timeout: CurlImpersonateIntegration.curl_impersonate_timeout,
          connect_timeout: CurlImpersonateIntegration.curl_impersonate_connect_timeout,
          process_timeout: CurlImpersonateIntegration.curl_impersonate_process_timeout,
          retries: CurlImpersonateIntegration.curl_impersonate_retries,
          retry_delay: CurlImpersonateIntegration.curl_impersonate_retry_delay
        }
      end
    end
  end
end 