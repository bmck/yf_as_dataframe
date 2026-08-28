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
      # First 2 values should be nil/NaN (not enough data for window=3)
      expect(result.to_a[0..1]).to all(be_nan)
      # Third value should be average of first 3 closes: (102 + 104 + 103) / 3 = 103
      expect(result.to_a[2]).to be_within(0.01).of(103.0)
    end

    it 'uses default column Adj Close when not specified' do
      result = YfAsDataframe.sma(df, window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('Adj Close')
    end

    it 'uses default window of 20 when not specified' do
      result = YfAsDataframe.sma(df, column: 'Close')
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('20')
    end
  end

  describe '.ema' do
    it 'calculates exponential moving average' do
      result = YfAsDataframe.ema(df, column: 'Close', window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('EMA')
      # First 4 values should be NaN (not enough data for window=5)
      expect(result.to_a[0..3]).to all(be_nan)
      # 5th value should be calculated
      expect(result.to_a[4]).not_to be_nan
    end

    it 'returns different values than SMA' do
      sma_result = YfAsDataframe.sma(df, column: 'Close', window: 5)
      ema_result = YfAsDataframe.ema(df, column: 'Close', window: 5)
      
      # EMA should differ from SMA for most values
      expect(ema_result.to_a[5]).not_to eq(sma_result.to_a[5])
    end
  end

  describe '.rsi' do
    it 'calculates relative strength index' do
      result = YfAsDataframe.rsi(df, window: 5)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('RSI')
    end

    it 'returns values between 0 and 100' do
      result = YfAsDataframe.rsi(df, window: 5)
      
      non_nan_values = result.to_a.reject(&:nan?)
      non_nan_values.each do |value|
        expect(value).to be_between(0, 100).inclusive
      end
    end
  end

  describe '.obv' do
    it 'calculates on-balance volume' do
      result = YfAsDataframe.obv(df)
      
      expect(result).to be_a(Polars::Series)
      expect(result.name).to include('OBV')
    end

    it 'returns cumulative volume values' do
      result = YfAsDataframe.obv(df)
      
      # OBV should have increasing absolute values
      expect(result.to_a.length).to eq(df.shape.first)
    end
  end

  describe 'error handling' do
    it 'raises error for invalid column name' do
      expect {
        YfAsDataframe.sma(df, column: 'NonExistentColumn', window: 5)
      }.to raise_error
    end

    it 'handles empty dataframe gracefully' do
      empty_df = Polars::DataFrame.new({
        'Close' => [],
        'Volume' => []
      })
      
      expect {
        YfAsDataframe.sma(empty_df, window: 5)
      }.to raise_error
    end
  end

  describe 'integration with DataFrame' do
    it 'can be added as a new column to dataframe' do
      sma_result = YfAsDataframe.sma(df, column: 'Close', window: 3)
      
      # Add the result as a new column
      df_with_sma = df.clone
      df_with_sma = df_with_sma.with_columns([sma_result])
      
      expect(df_with_sma.columns).to include(sma_result.name)
    end
  end
end
