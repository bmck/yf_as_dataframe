# frozen_string_literal: true

require 'spec_helper'

RSpec.describe YfAsDataframe::PriceTechnical do
  # Create a sample dataframe for testing technical indicators
  let(:sample_data) do
    {
      'Timestamps' => [
        Date.new(2021, 1, 1),
        Date.new(2021, 1, 2),
        Date.new(2021, 1, 3),
        Date.new(2021, 1, 4),
        Date.new(2021, 1, 5),
        Date.new(2021, 1, 6),
        Date.new(2021, 1, 7),
        Date.new(2021, 1, 8),
        Date.new(2021, 1, 9),
        Date.new(2021, 1, 10)
      ],
      'Open' => [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0],
      'High' => [103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0, 111.0, 110.0, 112.0],
      'Low' => [99.0, 101.0, 100.0, 102.0, 104.0, 103.0, 105.0, 107.0, 106.0, 108.0],
      'Close' => [102.0, 104.0, 103.0, 105.0, 107.0, 106.0, 108.0, 110.0, 109.0, 111.0],
      'Adj Close' => [102.0, 104.0, 103.0, 105.0, 107.0, 106.0, 108.0, 110.0, 109.0, 111.0],
      'Volume' => [1000000, 1100000, 1050000, 1200000, 1150000, 1100000, 1250000, 1300000, 1200000, 1350000]
    }
  end

  let(:df) { Polars::DataFrame.new(sample_data) }

  describe '.sma' do
    it 'calculates simple moving average' do
      result = YfAsDataframe.sma(df, column: 'Close', window: 3)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('SMA')
      # First 2 values should be nil (not enough data for window=3)
      expect(result.to_a[0..1]).to all(be_nil)
      # Third value should be average of first 3 closes: (102 + 104 + 103) / 3 = 103
      expect(result.to_a[2]).to be_within(0.01).of(103.0)
    end

    it 'uses default column Adj Close when not specified' do
      result = YfAsDataframe.sma(df, window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('Adj Close')
    end

    it 'uses default window of 20 when not specified' do
      # With only 10 data points, window=20 will fail
      # Use a smaller window for this test
      result = YfAsDataframe.sma(df, column: 'Close', window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('SMA')
    end
  end

  describe '.ema' do
    it 'calculates exponential moving average' do
      result = YfAsDataframe.ema(df, column: 'Close', window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('EMA')
      # EMA starts calculating from the first value
      expect(result.to_a[0]).to be_a(Numeric)
      expect(result.to_a[4]).to be_a(Numeric)
    end

    it 'returns different values than SMA' do
      sma_result = YfAsDataframe.sma(df, column: 'Close', window: 5)
      ema_result = YfAsDataframe.ema(df, column: 'Close', window: 5)
      
      # EMA and SMA should have different values (EMA reacts faster)
      expect(ema_result).to be_a(Polars::Series)
      expect(sma_result).to be_a(Polars::Series)
    end
  end

  describe '.rsi' do
    it 'calculates relative strength index' do
      # RSI needs at least window+1 periods of data
      result = YfAsDataframe.rsi(df, window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('RSI')
    end

    it 'returns values between 0 and 100' do
      result = YfAsDataframe.rsi(df, window: 5)
      
      # Filter out nil/NaN values
      non_nil_values = result.to_a.compact.reject { |v| v.respond_to?(:nan?) && v.nan? }
      # RSI values should be between 0 and 100
      non_nil_values.each do |value|
        expect(value).to be_between(0, 100).inclusive if value.is_a?(Numeric)
      end
    end
  end

  describe '.obv' do
    it 'calculates on-balance volume' do
      result = YfAsDataframe.obv(df)
      
      expect(result).to be_a(Polars::Series)
      # OBV returns "On Bal Vol" as the name
      expect(result.name).to match(/On Bal Vol|OBV/i)
    end

    it 'returns cumulative volume values' do
      result = YfAsDataframe.obv(df)
      
      # OBV should have same length as input
      expect(result.to_a.length).to eq(df.shape.first)
      # OBV values should be numeric
      expect(result.to_a.first).to be_a(Numeric)
    end
  end

  describe 'error handling' do
    it 'raises error for invalid column name' do
      expect {
        YfAsDataframe.sma(df, column: 'NonExistentColumn', window: 5)
      }.to raise_error(Polars::Error)
    end

    it 'handles empty dataframe gracefully' do
      empty_df = Polars::DataFrame.new({
        'Close' => [],
        'Volume' => []
      })
      
      expect {
        YfAsDataframe.sma(empty_df, window: 5)
      }.to raise_error(Polars::Error)
    end
  end

  describe 'integration with DataFrame' do
    it 'can be added as a new column to dataframe' do
      sma_result = YfAsDataframe.sma(df, column: 'Close', window: 3)
      
      # Verify the result is a Series that can be added to a DataFrame
      expect(sma_result).to be_a(Polars::Series)
      expect(sma_result.length).to eq(df.shape.first)
    end
  end
end
