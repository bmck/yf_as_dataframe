# frozen_string_literal: true

require 'yf_as_dataframe'
require 'webmock/rspec'
require 'vcr'

# Configure WebMock to allow local connections but block real HTTP requests
WebMock.disable_net_connect!(allow_localhost: false)

# Configure VCR for recording HTTP interactions
VCR.configure do |config|
  config.cassette_library_dir = 'spec/fixtures/vcr_cassettes'
  config.hook_into :webmock
  config.configure_rspec_metadata!
  config.default_cassette_options = {
    record: :once,
    match_requests_on: [:method, :uri, :body]
  }
  # Filter sensitive data if needed
  config.filter_sensitive_data('<COOKIE>') { |interaction| interaction.response.headers['Set-Cookie']&.first }
end

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # Reset class variables before each test to avoid state leakage
  config.before(:each) do
    # Reset YfConnection class variables to avoid cross-test contamination
    if defined?(YfAsDataframe::YfConnection)
      YfAsDataframe::YfConnection.class_variable_set(:@@cookie, nil) if YfAsDataframe::YfConnection.class_variable_defined?(:@@cookie)
      YfAsDataframe::YfConnection.class_variable_set(:@@crumb, nil) if YfAsDataframe::YfConnection.class_variable_defined?(:@@crumb)
      YfAsDataframe::YfConnection.class_variable_set(:@@user_agent_headers, {
        "User-Agent" => "Mozilla/5.0 (test)",
        "Accept" => "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language" => "en-US,en;q=0.9",
        "Accept-Encoding" => "gzip, deflate, br",
        "Referer" => "https:://finance.yahoo.com/",
        "Cache-Control" => "max-age=0",
        "Connection" => "keep-alive"
      }) if YfAsDataframe::YfConnection.class_variable_defined?(:@@user_agent_headers)
    end
  end
end
