require 'open3'
require 'json'
require 'ostruct'
require 'timeout'

class YfAsDataframe
  module CurlImpersonateIntegration
    # Configuration
    @curl_impersonate_enabled = true
    @curl_impersonate_fallback = true
    @curl_impersonate_timeout = 30  # Increased from 5 to 30 seconds
    @curl_impersonate_connect_timeout = 10  # New: connection timeout
    @curl_impersonate_retries = 2
    @curl_impersonate_retry_delay = 1
    @curl_impersonate_process_timeout = 60  # New: process timeout protection

    class << self
      attr_accessor :curl_impersonate_enabled, :curl_impersonate_fallback, 
                   :curl_impersonate_timeout, :curl_impersonate_connect_timeout,
                   :curl_impersonate_retries, :curl_impersonate_retry_delay,
                   :curl_impersonate_process_timeout
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

    # Make a curl-impersonate request with improved timeout handling
    def self.make_request(url, headers: {}, params: {}, timeout: nil, retries: nil)
      executable_info = get_random_executable
      return nil unless executable_info

      timeout ||= @curl_impersonate_timeout
      retries ||= @curl_impersonate_retries

      cmd = [
        executable_info[:path],
        "--max-time", timeout.to_s,
        "--connect-timeout", @curl_impersonate_connect_timeout.to_s,
        "--retry", retries.to_s,
        "--retry-delay", @curl_impersonate_retry_delay.to_s,
        "--retry-max-time", (timeout * 2).to_s,
        "--fail",
        "--silent",
        "--show-error"
      ]
      headers.each { |key, value| cmd.concat(["-H", "#{key}: #{value}"]) }
      unless params.empty?
        query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')
        separator = url.include?('?') ? '&' : '?'
        url = "#{url}#{separator}#{query_string}"
      end
      cmd << url

      # puts "DEBUG: curl-impersonate command: #{cmd.join(' ')}"
      # puts "DEBUG: curl-impersonate timeout: #{timeout} seconds"

      begin
        stdout_str = ''
        stderr_str = ''
        status = nil
        Open3.popen3(*cmd) do |stdin, stdout, stderr, wait_thr|
          stdin.close
          pid = wait_thr.pid
          done = false
          monitor = Thread.new do
            sleep(timeout + 10)
            unless done
              # puts "DEBUG: Killing curl-impersonate PID \\#{pid} after timeout"
              Process.kill('TERM', pid) rescue nil
              sleep(1)
              Process.kill('KILL', pid) rescue nil if wait_thr.alive?
            end
          end
          stdout_str = stdout.read
          stderr_str = stderr.read
          status = wait_thr.value
          done = true
          monitor.kill
        end
        # puts "DEBUG: curl-impersonate stdout: #{stdout_str[0..200]}..." if stdout_str && !stdout_str.empty?
        # puts "DEBUG: curl-impersonate stderr: #{stderr_str}" if stderr_str && !stderr_str.empty?
        # puts "DEBUG: curl-impersonate status: #{status.exitstatus}"
        if status.success?
          response = OpenStruct.new
            response.body = stdout_str
          response.code = 200
          response.define_singleton_method(:success?) { true }
            response.parsed_response = parse_json_if_possible(stdout_str)
          response
        else
          # puts "DEBUG: curl-impersonate failed with error: \\#{error_message}"
          error_message = "curl failed with code \\#{status.exitstatus}: \\#{stderr_str}"
          nil
        end
      rescue => e
        # puts "DEBUG: curl-impersonate exception: \\#{e.message}"
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