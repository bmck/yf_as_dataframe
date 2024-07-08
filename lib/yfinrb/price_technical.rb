class Yfinrb
  module PriceTechnical
    extend ActiveSupport::Concern
    include ActionView::Helpers::NumberHelper


    def ad(df)
      inputs = ['High', 'Low','Adj Close','Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ad(inputs).first
      Polars::Series.new("Accum-Distrib Ln", [nil]*(df.rows.length - output.length)+output)
    end


    def adosc(df, short_window: 2, long_window: 5)
      inputs = ['High', 'Low','Adj Close','Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.adosc(inputs, short_period: short_window, long_period: long_window).first
      Polars::Series.new("#{short_window}/#{long_window} Accum-Distrib Osc", [nil]*(df.rows.length - output.length)+output)
    end


    def adx(df, column: 'Adj Close', window: 5)
      input = Polars::Series.new(df[column]).to_a
      output = Tulirb.adx([input], period: window).first
      Polars::Series.new("#{window}-day Avg Dir Movemt Idx for #{column}", [nil]*(df.rows.length - output.length)+output)
    end

    alias_method :avg_dir_index, :adx

    def adxr(df, column: 'Adj Close', window: 5)
      input = Polars::Series.new(df[column]).to_a
      output = Tulirb.adxr([input], period: window).first
      Polars::Series.new("#{window}-day Avg Dir Movemt Rating for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :avg_dir_movement_rating, :adxr


    def avg_daily_trading_volume(df, window: 20)
      df.insert_column(0, Polars::Series.new('idx', (1..df.length).to_a))
      df = df.set_sorted('idx', descending: false)

      adtv = df.group_by_rolling(index_column: 'idx', period: "#{window}i").
        agg([Polars.mean('Volume').alias("ADTV(#{window})")]).to_series(1)
      df = df.drop('idx')
      adtv
    end


    def ao(df)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ao(inputs).first
      Polars::Series.new("Accum-Distrib Ln", [nil]*(df.rows.length - output.length)+output) #)
    end


    def apo(df, column: 'Adj Close', short_window: 12, long_window: 29)
      input = Polars::Series.new(df[column]).to_a
      output = Tulirb.ao([input], short_period: short_window, long_period: long_window).first
      Polars::Series.new("#{short_window}/#{long_window} Abs Price Osc for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def aroon(df, window: 20)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.aroon(inputs, period: window).first
      Polars::Series.new("#{window} Aroon Ind", [nil]*(df.rows.length - output.length)+output) #)
    end

    def aroonosc(df, window: 20)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.aroonosc(inputs, period: window).first
      Polars::Series.new("#{window} Aroon Osc Ind", [nil]*(df.rows.length - output.length)+output) #)
    end


    def avg_price(df)
      inputs = ['Open', 'High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.avgprice(inputs).first
      Polars::Series.new("Avg Price", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :avgprice, :avg_price

    def atr(df, window: 20)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.atr(inputs, period: window).first
      Polars::Series.new("#{window}-day Avg True Range", [nil]*(df.rows.length - output.length)+output)#)
    end

    alias_method :avg_true_range, :atr

    def bbands(df, column: 'Adj Close', window: 20, stddev: 1 )
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.bbands(inputs, period: window, stddev: stddev).first
      Polars::Series.new("#{window}-day Boll Band for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def bop(df)
      inputs = ['Open', 'High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.bop(inputs).first
      Polars::Series.new("Bal of Power", [nil]*(df.rows.length - output.length)+output) #)
    end


    def cci(df, window: 20)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.cci(inputs, period: window).first
      Polars::Series.new("#{window}-day Comm Channel Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def cmo(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.cmo(inputs, period: window).first
      Polars::Series.new("#{window}-day Chande Mom Osc for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def cvi(df, window: 20)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.cvi(inputs, period: window).first
      Polars::Series.new("#{window}-day Chaikins Volatility", [nil]*(df.rows.length - output.length)+output) #)
    end


    def dema(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.dema(inputs, period: window).first
      Polars::Series.new("Dbl EMA(#{window})", [nil]*(df.rows.length - output.length)+output) #)
    end


    def di(df, window: 20)
      inputs = ['High','Low','Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.di(inputs, period: window).first
      Polars::Series.new("#{window}-day Dir Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def dm(df, window: 20)
      inputs = ['High','Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.dm(inputs, period: window).first
      Polars::Series.new("#{window}-day Dir Movemt", [nil]*(df.rows.length - output.length)+output) #)
    end


    def dpo(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.dpo(inputs, period: window).first
      Polars::Series.new("#{window}-day Detrend Price Osc of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def dx(df, window: 20)
      inputs = ['High','Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.dx(inputs, period: window).first
      Polars::Series.new("#{window}-day Dir Movemt Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def ema(df, column: 'Adj Close', window: 5) 
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ema(inputs, period: window).first
      Polars::Series.new("EMA(#{window}) for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def emv(df)
      inputs = ['High', 'Low', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.emv(inputs).first
      Polars::Series.new("Ease of Mvmt", [nil]*(df.rows.length - output.length)+output) #)
    end


    def fisher(df, window: 20) 
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.fisher(inputs, period: window).first
      Polars::Series.new("#{window}-day Fisher Xform", [nil]*(df.rows.length - output.length)+output) #)
    end


    def fosc(df, window: 20) 
      inputs = ['Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.fosc(inputs, period: window).first
      Polars::Series.new("Fcast Osc", [nil]*(df.rows.length - output.length)+output) #)
    end


    def hma(df, column: 'Adj Close', window: 5) 
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ema(inputs, period: window).first
      Polars::Series.new("EMA(#{window}) for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def kama(df, column: 'Adj Close', window: 5) 
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.kama(inputs, period: window).first
      Polars::Series.new("KAMA(#{window}) for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def kvo(df, short_window: 5, long_window: 20)
      inputs = ['High', 'Low', 'Adj Close', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.kvo(inputs, short_period: short_window, long_period: long_window).first
      Polars::Series.new("#{short_window}/#{long_window} Klinger Vol Osc", [nil]*(df.rows.length - output.length)+output) #)
    end


    def linreg(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.linreg(inputs, period: window).first
      Polars::Series.new("#{window}-day Lin Reg Est for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def linregintercept(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.linregintercept(inputs, period: window).first
      Polars::Series.new("#{window}-day Lin Reg Int for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def linregslope(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.linregslope(inputs, period: window).first
      Polars::Series.new("#{window}-day Lin Reg Slope for #{column}", [nil]*(df.rows.length - output.length)+output)
    end


    def macd(df, column: 'Adj Close', short_window: 12, long_window: 26, signal_window: 9)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.macd(inputs, short_period: short_window, long_period: long_window, signal_period: signal_window).first
      Polars::Series.new("#{short_window}/#{long_window}/#{signal_window} MACD for #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def marketfi(df)
      inputs = ['High', 'Low', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.marketfi(inputs).first
      Polars::Series.new("Mkt Facilitation Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def mass(df, window: 20)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.mass(inputs, period: window).first
      Polars::Series.new("#{window}-day Mass Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def max(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.max(inputs, period: window).first
      Polars::Series.new("Max of #{column} in #{window}-day pd", [nil]*(df.rows.length - output.length)+output) #)
    end


    def md(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.md(inputs, period: window).first
      Polars::Series.new("Mean Dev of #{column} in #{window}-day pd", [nil]*(df.rows.length - output.length)+output) #)
    end


    def median_price(df)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.medprice(inputs).first
      Polars::Series.new("Med Price", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :medprice, :median_price

    def mfi(df, window: 20)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.mfi(inputs, period: window).first
      Polars::Series.new("#{window}-day Money Flow Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def min(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.min(inputs, period: window).first
      Polars::Series.new("Min of #{column} in #{window}-day pd", [nil]*(df.rows.length - output.length)+output) #)
    end


    def mom(df, column: 'Adj Close', window: 5)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.mom(inputs, period: window).first
      Polars::Series.new("#{window}-day Momentum of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :momentum, :mom

    def moving_avgs(df, window: 20)
      df.insert_column(0, Polars::Series.new('idx', (1..df.length).to_a))
      df = df.set_sorted('idx', descending: false)
      # df = df.insert_column(df.columns.length-1, 
      s = df.group_by_rolling(index_column: 'idx', period: "#{window}i").agg([Polars.mean('Adj Close').alias("MA(#{window})")]).to_series(1) #)
      df = df.drop('idx')
      s
    end

    def natr(df, window: 20)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.natr(inputs, period: window).firs
      Polars::Series.new("#{window}-day Norm Avg True Range", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :normalized_avg_true_range, :natr


    # def norm_momentum(period: '1y')
    #   df = history(period: period, interval: "1d")
    #   dat = df['Adj Close'].to_a
    #   mn = dat.sum.to_f / dat.count.to_f
    #   std = dat.map{|d| (d-mn)}.sum.to_f/dat.count.to_f
    #   all_times = df['Timestamps'].to_a
    #   min_time = all_times.min

    #   WINDOWS.each_with_index do |n, ndex|
    #     s = [nil] * all_times.length
    #     dat.length.times do |t_ndex|
    #       next if t_ndex < n
    #       later_row = df[t_ndex]
    #       earlier_row = df[t_ndex - n]
    #       s[t_ndex] = (later_row['Adj Close'][0]) - (earlier_row['Adj Close'][0])/std
    #     end
    #     next if s.all?(&:nil?)
    #     s = Polars::Series.new("#{n}-day Price Chg", s)
    #     df.insert_column(df.columns.length, s)
    #   end

    #   min_win = WINDOWS.min
    #   df.drop_nulls(subset: ["#{min_win}-day Price Chg"])
    # end



    def nvi(df)
      inputs = ['Adj Close', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.nvi(inputs).first
      Polars::Series.new("Neg Vol Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def obv(df)
      inputs = ['Adj Close', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.obv(inputs).first
      Polars::Series.new("On Bal Vol", [nil]*(df.rows.length - output.length)+output) #)
    end


    def ppo(df, column: 'Adj Close', short_window: 12, long_window: 26)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ppo(inputs, short_period: short_window, long_period: long_window).first
      Polars::Series.new("#{short_window}/#{long_window} Pctage Price Osc of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def psar(df, acceleration_factor_step: 0.2, acceleration_factor_maximum: 2)
      inputs = ['High', 'Low'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.psar(inputs, acceleration_factor_step, acceleration_factor_maximum).first
      Polars::Series.new("Parabolic SAR w step #{acceleration_factor_step} and max #{acceleration_factor_maximum}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def pvi(df)
      inputs = ['Adj Close', 'Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.pvi(inputs).first
      Polars::Series.new("Pos Vol Idx", [nil]*(df.rows.length - output.length)+output) #)
    end


    def qstick(df, window: 20)
      inputs = ['Open', 'Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.qstick(inputs, period: window).first
      Polars::Series.new("#{window}-day Qstick", [nil]*(df.rows.length - output.length)+output) #)
    end


    def roc(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.roc(inputs, period: window).first
      Polars::Series.new("Rate of Chg of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def rocr(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.rocr(inputs, period: window).first
      Polars::Series.new("Rate of Chg Ratio of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    def rsi(df, window: 20)
      return nil if w == 1
      inputs = ['Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.rsi(inputs, period: window).first
      Polars::Series.new("#{window}-day RSI", [nil]*(df.rows.length - output.length)+output) #)
    end

    def sma(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.sma(inputs, period: window).first
      Polars::Series.new("SMA(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def stddev(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.stddev(inputs, period: window).first
      Polars::Series.new("Rolling Stdev(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def stderr(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.stderr(inputs, period: window).first
      Polars::Series.new("Rolling Stderr(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def stochrsi(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.stochrsi(inputs, period: window).first
      Polars::Series.new("Stochastic RSI(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def sum(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.sum(inputs, period: window).first
      Polars::Series.new("Rolling #{window}-day Sum of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    def tema(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.tema(inputs, period: window).first
      Polars::Series.new("TEMA(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def tr(df, column: 'Adj Close')
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.tr(inputs).first
      Polars::Series.new("True Range of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :true_range, :tr

    def trima(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.trima(inputs, period: window).first
      Polars::Series.new("Triang MA(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def trix(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.trix(inputs, period: window).first
      Polars::Series.new("Trix(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end  


    def tsf(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.tsf(inputs, period: window).first
      Polars::Series.new("Time-series Fcast(#{window}) of #{window}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def typical_price(df)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.typprice(inputs).first
      Polars::Series.new("Typical Price", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :typprice, :typical_price

    def ultosc(df, short_window: 5, medium_window: 12, long_window: 26)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.ultosc(inputs, short_period: short_window, medium_period: medium_window, long_period: long_window).first
      Polars::Series.new("Ult Osc(#{short_window}, #{medium_window}, #{long_window})", [nil]*(df.rows.length - output.length)+output) #)
    end

    def weighted_close_price(df)
      inputs = ['High', 'Low', 'Adj Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.wcprice(inputs).first
      Polars::Series.new("Wtd Close Price", [nil]*(df.rows.length - output.length)+output) #)
    end


    def var(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.var(inputs, period: window).first
      Polars::Series.new("Var over Per(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def vhf(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.vhf(inputs, period: window).first
      Polars::Series.new("VHF(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def vidya(df, column: 'Adj Close', short_window: 5, long_window: 20, alpha: 0.2)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.vidya(inputs, short_period: short_window, long_period: long_window, alpha: alpha).first
      Polars::Series.new("vidya(#{short_window},#{long_window},#{alpha}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def volatility(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.volatility(inputs, period: window).first
      Polars::Series.new("#{window}-day Volatility of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def vosc(df, column: 'Adj Close', short_window: 5, long_window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.vosc(inputs, short_period: short_window, long_period: long_window).first
      Polars::Series.new("#{short_window}/#{long_window} Vol Osc of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end

    def vol_weighted_moving_avg(df, window: 20)
      inputs = ['Adj Close','Volume'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.vwma(inputs, period: window).first
      Polars::Series.new("VWMA(#{window})", [nil]*(df.rows.length - output.length)+output) #)
    end

    alias_method :vwma, :vol_weighted_moving_avg

    def wad(df)
      inputs = ['High','Low','Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.wad(inputs, period: window).first
      Polars::Series.new("Wms Accum/Distrib", [nil]*(df.rows.length - output.length)+output) #)
    end


    def wcprice(df)
      inputs = ['High','Low','Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.stderr(inputs).first
      Polars::Series.new("Wtd Close Price", [nil]*(df.rows.length - output.length)+output) #)
    end


    def wilders(df, column: 'Adj Close', window: 20)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.wilders(inputs, period: window).first
      Polars::Series.new("#{window}-day Wilders Smoothing of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def willr(df, window: 20)
      inputs = ['High','Low','Close'].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.willr(inputs, period: window).first
      Polars::Series.new("#{window}-day Williams %R Ind", [nil]*(df.rows.length - output.length)+output) #)
    end


    def wma(df, column: 'Adj Close', window: 5)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.wma(inputs, period: window).first
      Polars::Series.new("WMA(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end


    def zlema(df, column: 'Adj Close', window: 5)
      inputs = [column].map{|col| Polars::Series.new(df[col]).to_a}
      output = Tulirb.zlema(inputs, period: window).first
      Polars::Series.new("ZLEMA(#{window}) of #{column}", [nil]*(df.rows.length - output.length)+output) #)
    end
  end
end
