# frozen_string_literal: true

require 'spec_helper'
require 'json'

RSpec.describe YfAsDataframe::Ticker do
  let(:ticker_symbol) { 'MSFT' }
  let(:ticker) { described_class.new(ticker_symbol) }

  describe '#initialize' do
    it 'creates a ticker with the correct symbol' do
      expect(ticker.ticker).to eq('MSFT')
    end

    it 'sets default timezone to America/New_York' do
      expect(ticker.tz).to be_a(TZInfo::Timezone)
      expect(ticker.tz.identifier).to eq('America/New_York')
    end

    it 'converts lowercase ticker to uppercase' do
      lowercase_ticker = described_class.new('aapl')
      expect(lowercase_ticker.ticker).to eq('AAPL')
    end
  end

  describe '#history', :vcr do
    let(:mock_response) do
      {
        'chart' => {
          'result' => [{
            'meta' => {
              'currency' => 'USD',
              'symbol' => 'MSFT',
              'exchangeName' => 'NMS',
              'instrumentType' => 'EQUITY',
              'firstTradeDate' => 511108200,
              'regularMarketTime' => Time.now.to_i,
              'gmtoffset' => -18000,
              'timezone' => 'EST',
              'exchangeTimezoneName' => 'America/New_York',
              'regularMarketPrice' => 425.50,
              'chartPreviousClose' => 420.00,
              'priceHint' => 2,
              'validRanges' => ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
            },
            'timestamp' => [
              1609459200, # 2021-01-01
              1609545600, # 2021-01-02
              1609632000  # 2021-01-03
            ],
            'indicators' => {
              'quote' => [{
                'open' => [100.0, 102.0, 101.0],
                'high' => [103.0, 105.0, 104.0],
                'low' => [99.0, 101.0, 100.0],
                'close' => [102.0, 104.0, 103.0],
                'volume' => [1000000, 1100000, 1050000]
              }],
              'adjclose' => [{
                'adjclose' => [102.0, 104.0, 103.0]
              }]
            },
            'events' => {
              'dividends' => {},
              'splits' => {}
            }
          }],
          'error' => nil
        }
      }
    end

    before do
      # Stub the HTTP request to Yahoo Finance
      stub_request(:get, /query2\.finance\.yahoo\.com\/v8\/finance\/chart\/#{ticker_symbol}/)
        .to_return(status: 200, body: mock_response.to_json, headers: { 'Content-Type' => 'application/json' })
      
      # Stub cookie request
      stub_request(:get, 'https://fc.yahoo.com')
        .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123; Path=/; Domain=.yahoo.com' })
    end

    it 'fetches historical price data' do
      result = ticker.history(period: '1mo')
      
      expect(result).to be_a(Polars::DataFrame)
      expect(result.columns).to include('Open', 'High', 'Low', 'Close', 'Volume')
    end

    it 'returns a dataframe with correct columns' do
      result = ticker.history(period: '1mo', actions: false)
      
      expect(result.columns).to include('Timestamps', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume')
      expect(result.columns).not_to include('Dividends', 'Stock Splits')
    end

    it 'includes dividends and splits when actions=true' do
      result = ticker.history(period: '1mo', actions: true)
      
      expect(result.columns).to include('Dividends', 'Stock Splits')
    end

    it 'accepts start and end dates' do
      result = ticker.history(start: '2021-01-01', fin: '2021-01-31')
      
      expect(result).to be_a(Polars::DataFrame)
      expect(result.shape.first).to be > 0
    end

    it 'handles different intervals' do
      result = ticker.history(period: '1mo', interval: '1d')
      
      expect(result).to be_a(Polars::DataFrame)
    end
  end

  describe '#history error handling' do
    context 'when Yahoo returns an error' do
      let(:error_response) do
        {
          'chart' => {
            'result' => nil,
            'error' => {
              'code' => 'Not Found',
              'description' => 'No data found, symbol may be delisted'
            }
          }
        }
      end

      before do
        stub_request(:get, /query2\.finance\.yahoo\.com\/v8\/finance\/chart\/INVALID/)
          .to_return(status: 200, body: error_response.to_json, headers: { 'Content-Type' => 'application/json' })
        
        stub_request(:get, 'https://fc.yahoo.com')
          .to_return(status: 200, body: '', headers: { 'Set-Cookie' => 'test_cookie=abc123' })
      end

      it 'returns empty dataframe for invalid symbol' do
        invalid_ticker = described_class.new('INVALID')
        result = invalid_ticker.history(period: '1mo', raise_errors: false)
        
        expect(result).to be_a(Polars::DataFrame)
        expect(result.shape.first).to eq(0)
      end

      it 'raises error when raise_errors=true' do
        invalid_ticker = described_class.new('INVALID')
        
        expect {
          invalid_ticker.history(period: '1mo', raise_errors: true)
        }.to raise_error(Exception)
      end
    end
  end

  describe 'attribute accessors' do
    it 'has a ticker reader' do
      expect(ticker.ticker).to eq('MSFT')
    end

    it 'has a tz accessor' do
      expect(ticker.tz).to be_a(TZInfo::Timezone)
    end

    it 'allows setting timeout' do
      ticker.timeout = 60
      expect(ticker.timeout).to eq(60)
    end

    it 'has default timeout of 30 seconds' do
      expect(ticker.timeout).to eq(30)
    end
  end
end
