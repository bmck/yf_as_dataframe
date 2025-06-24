# Minimal patch to make curl-impersonate the default behavior
# This file should be required after the main YfConnection class

require_relative 'curl_impersonate_integration'

module YfAsDataframe
  module YfConnection
    # Store original methods
    alias_method :get_original, :get
    alias_method :get_raw_json_original, :get_raw_json

    # Override get method to use curl-impersonate by default
    def get(url, headers=nil, params=nil)
      # Try curl-impersonate first if enabled
      if CurlImpersonateIntegration.curl_impersonate_enabled
        begin
          # Prepare headers and params as in original method
          headers ||= {}
          params ||= {}
          params.merge!(crumb: @@crumb) unless @@crumb.nil?
          cookie, crumb, strategy = _get_cookie_and_crumb()
          crumbs = !crumb.nil? ? {'crumb' => crumb} : {}

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

          # Make curl-impersonate request
          response = CurlImpersonateIntegration.make_request(
            url, 
            headers: curl_headers, 
            params: params.merge(crumbs),
            timeout: CurlImpersonateIntegration.curl_impersonate_timeout
          )

          if response && response.success?
            return response
          end
        rescue => e
          # Log error but continue to fallback
          warn "curl-impersonate request failed: #{e.message}" if $VERBOSE
        end
      end

      # Fallback to original HTTParty method
      if CurlImpersonateIntegration.curl_impersonate_fallback
        get_original(url, headers, params)
      else
        raise "curl-impersonate failed and fallback is disabled"
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

      def get_available_curl_impersonate_executables
        CurlImpersonateIntegration.available_executables
      end
    end
  end
end 