class YfAsDataframe
  module Analysis
    extend ActiveSupport::Concern

    # attr_accessor :ticker

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_analysis # (your module code here)
        end
      end
    end

    def initialize_analysis
      @earnings_trend = nil
      @analyst_trend_details = nil
      @analyst_price_target = nil
      @ev_est = nil
      @ps_est = nil
    end


    def earnings_trend #(self)
      raise YFNotImplementedError.new('earnings_trend') if @earnings_trend.nil?
      return earnings_trend
    end

    def analyst_trend_details #(self)
      raise YFNotImplementedError.new('analyst_trend_details') if @analyst_trend_details.nil?
      return analyst_trend_details
    end

    alias_method :trend_details, :analyst_trend_details

    def analyst_price_target #(self)
      raise YFNotImplementedError.new('analyst_price_target') if @analyst_price_target.nil?
      return analyst_price_target
    end

    alias_method :price_targets, :analyst_price_target

    def rev_est #(self)
      raise YFNotImplementedError.new('rev_est') if @rev_est.nil?
      return rev_est
    end

    alias_method :rev_forecast, :rev_est

    def eps_est #(self)
      raise YFNotImplementedError.new('eps_est') if @eps_est.nil?
      return eps_est
    end

    alias_method :earnings_forecast, :eps_est

    # analysis_methods = [:earnings_trend, :analyst_trend_details, :trend_details, \
    #                     :price_targets, :analyst_price_target, :rev_est, \
    #                     :rev_forecast, :eps_est, :earnings_forecast ]
    # analysis_methods.each do |meth|
    #   #   delegate meth, to: :analysis
    #   alias_method "get_#{meth}".to_sym, meth
    # end

  end
end
