class YfAsDataframe
  module Fundamentals
    extend ActiveSupport::Concern

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        attr_reader :financials, :earnings, :shares, :ticker

        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_fundamentals # (your module code here)
        end
      end
    end

    def initialize_fundamentals
      @earnings = nil
      @financials = nil
      @shares = nil

      @financials_data = nil
      @fin_data_quote = nil
      @basics_already_scraped = false
    end

    # delegate :proxy, :tz, to: :ticker

    def earnings
      raise YFNotImplementedError.new('earnings') if @earnings.nil?
      @earnings
    end

    def shares
      raise YFNotImplementedError.new('shares') if @shares.nil?
      @shares
    end

    # financials_methods = [:income_stmt, :incomestmt, :financials, :balance_sheet, :balancesheet, :cash_flow, :cashflow]
    # financials_methods.each do |meth|
    #   delegate "get_#{meth}".to_sym, meth, to: :financials
    # end

    # fundamentals_methods = [:earnings, :shares]
    # fundamentals_methods.each do |meth|
    #   alias_method  "get_#{meth}".to_sym, meth
    # end    

    # def quarterly_earnings
    #   earnings(freq: 'quarterly')
    # end
  end
end
