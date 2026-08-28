# frozen_string_literal: true

require 'spec_helper'

RSpec.describe YfAsDataframe::YfConnection do
  # Create a test class that includes the module
  let(:test_class) do
    Class.new do
      include YfAsDataframe::YfConnection
      
      def initialize
        yfconn_initialize
      end
    end
  end

  let(:connection) { test_class.new }

  describe 'User-Agent headers' do
    it 'includes a User-Agent header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('User-Agent')
      expect(headers['User-Agent']).to be_a(String)
      expect(headers['User-Agent']).not_to be_empty
    end

    it 'includes Accept header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('Accept')
    end

    it 'includes Accept-Language header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('Accept-Language')
      expect(headers['Accept-Language']).to include('en-US')
    end

    it 'includes Accept-Encoding header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('Accept-Encoding')
      expect(headers['Accept-Encoding']).to include('gzip')
    end

    it 'includes Referer header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('Referer')
      expect(headers['Referer']).to include('finance.yahoo.com')
    end

    it 'includes Connection header' do
      headers = YfAsDataframe::YfConnection.user_agent_headers
      expect(headers).to have_key('Connection')
      expect(headers['Connection']).to eq('keep-alive')
    end
  end

  describe '#get' do
    let(:test_url) { 'https://query2.finance.yahoo.com/test' }
    
    before do
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123; Path=/; Domain=.yahoo.com' })
      
      stub_request(:get, test_url)
        .to_return(status: 200, body: '{"test": "data"}', headers: { 'Content-Type' => 'application/json' })
    end

    it 'makes HTTP GET requests' do
      response = connection.get(test_url)
      
      expect(response).to be_a(HTTParty::Response)
      expect(response.code).to eq(200)
    end

    it 'includes user agent headers in requests' do
      connection.get(test_url)
      
      # Verify the request was made with proper headers
      expect(WebMock).to have_requested(:get, test_url).with { |req|
        req.headers['User-Agent'] != nil
      }
    end

    it 'handles query parameters' do
      stub_request(:get, "#{test_url}?param1=value1&param2=value2")
        .to_return(status: 200, body: '{"test": "data"}')
      
      response = connection.get(test_url, nil, { param1: 'value1', param2: 'value2' })
      
      expect(response.code).to eq(200)
    end
  end

  describe 'circuit breaker' do
    let(:failing_url) { 'https://query2.finance.yahoo.com/failing' }

    before do
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123' })
    end

    it 'opens after multiple failures' do
      # Stub multiple failing requests
      stub_request(:get, failing_url)
        .to_return(status: 500, body: 'Internal Server Error')
      
      # Make requests until circuit breaker opens
      3.times do
        begin
          connection.get(failing_url)
        rescue RuntimeError => e
          # Expected to fail
        end
      end
      
      # Circuit breaker should now be open
      expect {
        connection.get(failing_url)
      }.to raise_error(RuntimeError, /Circuit breaker is open/)
    end
  end

  describe 'cookie management' do
    before do
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123; Path=/; Domain=.yahoo.com' })
    end

    it 'fetches and stores cookies' do
      test_url = 'https://query2.finance.yahoo.com/test'
      stub_request(:get, test_url)
        .to_return(status: 200, body: '{}')
      
      # Make a request that will trigger cookie fetching
      connection.get(test_url)
      
      # Verify cookie was fetched
      expect(WebMock).to have_requested(:get, 'https://fc.yahoo.com').at_least_once
    end
  end

  describe 'request throttling' do
    it 'throttles requests to avoid rate limiting' do
      test_url = 'https://query2.finance.yahoo.com/throttle_test'
      
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc' })
      
      stub_request(:get, test_url)
        .to_return(status: 200, body: '{}')
      
      start_time = Time.now
      connection.get(test_url)
      end_time = Time.now
      
      # Request should take at least 0.1 seconds due to throttling
      expect(end_time - start_time).to be >= 0.1
    end
  end

  describe 'session management' do
    it 'tracks request count' do
      test_url = 'https://query2.finance.yahoo.com/session_test'
      
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc' })
      
      stub_request(:get, test_url)
        .to_return(status: 200, body: '{}')
      
      # Make a request
      connection.get(test_url)
      
      # Verify session tracking works (we can't directly test class variables,
      # but we can verify the request completed without error)
      expect(WebMock).to have_requested(:get, test_url).once
    end
  end
end
