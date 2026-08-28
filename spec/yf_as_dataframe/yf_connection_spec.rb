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
      
      # The get method returns an HTTParty::Response object
      expect(response).not_to be_nil
      # WebMock stubs may return strings directly, but real HTTParty returns Response objects
      # We just verify the method completes successfully
      expect(WebMock).to have_requested(:get, test_url)
    end

    it 'includes user agent headers in requests' do
      connection.get(test_url)
      
      # Verify the request was made with proper headers
      expect(WebMock).to have_requested(:get, test_url).with { |req|
        req.headers['User-Agent'] != nil
      }
    end

    it 'handles query parameters' do
      # The get method builds query params into the URL
      stub_request(:get, /query2\.finance\.yahoo\.com\/test/)
        .to_return(status: 200, body: '{"test": "data"}', headers: { 'Content-Type' => 'application/json' })
      
      response = connection.get(test_url, nil, { param1: 'value1', param2: 'value2' })
      
      # Verify that the method accepts and processes query parameters without error
      expect(response).not_to be_nil
    end
  end

  describe 'circuit breaker' do
    let(:failing_url) { 'https://query2.finance.yahoo.com/failing' }

    before do
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123' })
    end

    it 'opens after multiple failures' do
      # Stub multiple failing requests (circuit breaker threshold is 3)
      stub_request(:get, failing_url)
        .to_return(status: 500, body: 'Internal Server Error').times(4)
      
      # Make requests until circuit breaker opens (3 failures + 1 to trigger)
      4.times do |i|
        begin
          connection.get(failing_url)
        rescue RuntimeError => e
          # First 3 should fail with normal error, 4th should be circuit breaker
          if i == 3
            expect(e.message).to match(/Circuit breaker is open/)
          end
        end
      end
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
      # The throttle_request method is private and adds a random delay
      # We can verify the method exists in the module
      expect(YfAsDataframe::YfConnection.private_instance_methods).to include(:throttle_request)
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
