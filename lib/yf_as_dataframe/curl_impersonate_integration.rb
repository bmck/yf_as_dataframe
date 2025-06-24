require 'open3'
require 'json'
require 'ostruct'

module YfAsDataframe
  module CurlImpersonateIntegration
    # Configuration
    @@curl_impersonate_enabled = true
    @@curl_impersonate_fallback = true
    @@curl_impersonate_timeout = 30
    @@curl_impersonate_retries = 2
    @@curl_impersonate_retry_delay = 1

    class << self
      attr_accessor :curl_impersonate_enabled, :curl_impersonate_fallback, 
                   :curl_impersonate_timeout, :curl_impersonate_retries, 
                   :curl_impersonate_retry_delay
    end

    # Get the curl-impersonate executable directory from environment variable or default
    def self.executable_directory
      ENV['CURL_IMPERSONATE_DIR'] || '/usr/local/bin'
    end

    # Find available curl-impersonate executables
    def self.available_executables
      @available_executables ||= begin
        executables = []
        Dir.glob(File.join(executable_directory, "curl_*")).each do |path|
          executable = File.basename(path)
          if executable.start_with?('curl_')
            browser_type = case executable
                          when /^curl_chrome/ then :chrome
                          when /^curl_ff/ then :firefox
                          when /^curl_edge/ then :edge
                          when /^curl_safari/ then :safari
                          else :unknown
                          end
            executables << { path: path, executable: executable, browser: browser_type }
          end
        end
        executables
      end
    end

    # Get a random executable
    def self.get_random_executable
      available = available_executables
      return nil if available.empty?
      available.sample
    end

    # Make a curl-impersonate request
    def self.make_request(url, headers: {}, params: {}, timeout: nil)
      executable_info = get_random_executable
      return nil unless executable_info

      timeout ||= @@curl_impersonate_timeout

      # Build command
      cmd = [executable_info[:path], "--max-time", timeout.to_s]
      
      # Add headers
      headers.each do |key, value|
        cmd.concat(["-H", "#{key}: #{value}"])
      end

      # Add query parameters
      unless params.empty?
        query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')
        separator = url.include?('?') ? '&' : '?'
        url = "#{url}#{separator}#{query_string}"
      end

      # Add URL
      cmd << url

      # Execute
      stdout, stderr, status = Open3.capture3(*cmd)
      
      if status.success?
        # Create a response object similar to HTTParty
        response = OpenStruct.new
        response.body = stdout
        response.code = 200
        response.define_singleton_method(:success?) { true }
        response.parsed_response = parse_json_if_possible(stdout)
        response
      else
        nil
      end
    end

    private

    def self.parse_json_if_possible(response_body)
      JSON.parse(response_body)
    rescue JSON::ParserError
      response_body
    end
  end
end 