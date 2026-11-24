require 'open3'
require 'json'
require 'ostruct'
require 'timeout'

class YfAsDataframe
  module CurlImpersonateIntegration
    # Homebrew core formula: https://github.com/Homebrew/homebrew-core/blob/master/Formula/c/curl-impersonate.rb
    # That formula builds lexiforest/curl-impersonate (v2.3.2).
    HOMEBREW_FORMULA = "curl-impersonate"
    HOMEBREW_UPSTREAM = "https://github.com/lexiforest/curl-impersonate"

    # Prefixes used when `brew --prefix` is unavailable.
    DEFAULT_BIN_DIRECTORIES = [
      "/opt/homebrew/bin",
      "/usr/local/bin",
      "/home/linuxbrew/.linuxbrew/bin"
    ].freeze

    # Headers owned by the curl_* wrapper. Passing another User-Agent would
    # disagree with the TLS fingerprint of the selected browser.
    FINGERPRINT_HEADERS = %w[user-agent accept accept-language accept-encoding].freeze

    WRAPPER_PATTERN = /\Acurl_(chrome|firefox|ff|edge|safari)(\d+)[a-z]?(?:_([a-z0-9]+))?\z/
    BROWSER_RANK = { chrome: 0, edge: 1, safari: 2, firefox: 3 }.freeze
    VARIANT_RANK = { nil => 0, "macos" => 1, "ios" => 2, "android" => 3 }.freeze

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

    # Directory of the wrapper that will be used, or the first directory searched.
    def self.executable_directory
      selected = selected_executable
      return File.dirname(selected[:path]) if selected

      executable_directories.first || "/usr/local/bin"
    end

    # CURL_IMPERSONATE_DIR when set, otherwise the Homebrew prefix and the usual prefixes.
    def self.executable_directories
      configured = ENV["CURL_IMPERSONATE_DIR"].to_s
      return [configured] unless configured.empty?

      directories = []
      prefix = _homebrew_prefix
      directories << File.join(prefix, "bin") if prefix && !prefix.empty?
      directories.concat(DEFAULT_BIN_DIRECTORIES)
      directories.uniq
    end

    # Browser wrappers found in executable_directories.
    def self.available_executables
      @available_executables ||= executable_directories.flat_map { |dir| _wrappers_in(dir) }
    end

    # Newest desktop Chrome wrapper. Edge, Safari, then Firefox are used when no Chrome wrapper is installed.
    def self.selected_executable
      available_executables.min_by do |executable|
        [
          BROWSER_RANK.fetch(executable[:browser], 9),
          -executable[:version].to_i,
          VARIANT_RANK.fetch(executable[:variant], 4),
          executable[:executable]
        ]
      end
    end

    def self.reset_executable_cache!
      @available_executables = nil
    end

    # Make a curl-impersonate request with improved timeout handling
    def self.make_request(url, headers: {}, params: {}, timeout: nil, retries: nil)
      executable_info = selected_executable
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
      _request_headers(headers).each { |key, value| cmd.concat(["-H", "#{key}: #{value}"]) }
      unless params.empty?
        query_string = params.map { |k, v| "#{k}=#{v}" }.join("&")
        separator = url.include?("?") ? "&" : "?"
        url = "#{url}#{separator}#{query_string}"
      end
      cmd << url

      begin
        stdout_str = ""
        stderr_str = ""
        status = nil
        Open3.popen3(*cmd) do |stdin, stdout, stderr, wait_thr|
          stdin.close
          pid = wait_thr.pid
          done = false
          monitor = Thread.new do
            sleep(timeout + 10)
            unless done
              Process.kill("TERM", pid) rescue nil
              sleep(1)
              Process.kill("KILL", pid) rescue nil if wait_thr.alive?
            end
          end
          stdout_str = stdout.read
          stderr_str = stderr.read
          status = wait_thr.value
          done = true
          monitor.kill
        end
        if status.success?
          response = OpenStruct.new
          response.body = stdout_str
          response.code = 200
          response.define_singleton_method(:success?) { true }
          response.parsed_response = _parse_json_if_possible(stdout_str)
          response
        else
          nil
        end
      rescue StandardError
        nil
      end
    end

    def self._wrappers_in(directory)
      return [] unless File.directory?(directory)

      Dir.glob(File.join(directory, "curl_*")).filter_map do |path|
        next unless File.file?(path) && File.executable?(path)

        _describe_wrapper(path)
      end
    end
    private_class_method :_wrappers_in

    def self._describe_wrapper(path)
      match = File.basename(path).match(WRAPPER_PATTERN)
      return nil unless match

      family = match[1] == "ff" ? "firefox" : match[1]
      {
        path: path,
        executable: File.basename(path),
        browser: family.to_sym,
        version: match[2].to_i,
        variant: match[3]
      }
    end
    private_class_method :_describe_wrapper

    def self._request_headers(headers)
      headers.each_with_object({}) do |(key, value), kept|
        next if FINGERPRINT_HEADERS.include?(key.to_s.downcase)

        kept[key] = value
      end
    end
    private_class_method :_request_headers

    def self._homebrew_prefix
      return @homebrew_prefix if defined?(@homebrew_prefix)

      brew = _which("brew")
      @homebrew_prefix =
        if brew
          out, status = Open3.capture2(brew, "--prefix")
          status.success? ? out.strip : nil
        end
    end
    private_class_method :_homebrew_prefix

    def self._which(command)
      ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).each do |dir|
        candidate = File.join(dir, command)
        return candidate if File.executable?(candidate) && !File.directory?(candidate)
      end
      nil
    end
    private_class_method :_which

    def self._parse_json_if_possible(response_body)
      JSON.parse(response_body)
    rescue JSON::ParserError
      response_body
    end
    private_class_method :_parse_json_if_possible
  end
end
