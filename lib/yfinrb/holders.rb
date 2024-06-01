
require'date'
require 'open-uri'
require 'json'
require 'csv'


class Yfin
  module Holders
    extend ActiveSupport::Concern
    # include YfConnection

    BASE_URL = 'https://query2.finance.yahoo.com'.freeze
    QUOTE_SUMMARY_URL = "#{BASE_URL}/v10/finance/quoteSummary/".freeze

    # attr_accessor :ticker

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_holders # (your module code here)
        end
      end
    end

    def initialize_holders
      @major = nil
      @major_direct_holders = nil
      @institutional = nil
      @mutualfund = nil

      @insider_transactions = nil
      @insider_purchases = nil
      @insider_roster = nil
    end

    def major
      _fetch_and_parse if @major.nil?
      return @major
    end

    alias_method :major_holders, :major

    def institutional
      _fetch_and_parse if @institutional.nil? 
      return @institutional
    end

    alias_method :institutional_holders, :institutional

    def mutualfund
      _fetch_and_parse if @mutualfund.nil? 
      return @mutualfund
    end

    alias_method :mutualfund_holders, :mutualfund

    def insider_transactions
      _fetch_and_parse if @insider_transactions.nil? 
      return @insider_transactions
    end

    def insider_purchases
      _fetch_and_parse if @insider_purchases.nil? 
      return @insider_purchases
    end

    def insider_roster
      return @insider_roster unless @insider_roster.nil? 

      _fetch_and_parse
      return @insider_roster
    end

    alias_method :insider_roster_holders, :insider_roster

   # holders_methods = [:major, :major_holders, :institutional, :institutional_holders, :mutualfund, \
   #                     :mutualfund_holders, :insider_transactions, :insider_purchases, :insider_roster, \
   #                     :insider_roster_holders]
   #  holders_methods.each do |meth|
   #    alias_method "get_#{meth}".to_sym, meth
   #  end





    private

    def _fetch_and_parse
      modules = %w[institutionOwnership fundOwnership majorDirectHolders majorHoldersBreakdown insiderTransactions insiderHolders netSharePurchaseActivity].join(',')
      params = { modules: modules, corsDomain: 'finance.yahoo.com', formatted: 'false' }
      result = _fetch(params)

      _parse_result(result)
    rescue OpenURI::HTTPError => e
      # Rails.logger.error { "#{__FILE__}:#{__LINE__} Error: #{e.message}" }

      @major = []
      @major_direct_holders = []
      @institutional = []
      @mutualfund = []
      @insider_transactions = []
      @insider_purchases = []
      @insider_roster = []
    end

    def _fetch(params)
      url = "#{QUOTE_SUMMARY_URL}#{ticker}"
      # Rails.logger.info { "#{__FILE__}:#{__LINE__} url: #{url}" }

      JSON.parse(URI.open(url, proxy: proxy, 'User-Agent' => 'Mozilla/5.0 (compatible; yahoo-finance2/0.0.1)').read(query: params))
    end

    def _parse_result(result)
      data = result.dig('quoteSummary', 'result', 0)
      _parse_institution_ownership(data['institutionOwnership'])
      _parse_fund_ownership(data['fundOwnership'])
      _parse_major_holders_breakdown(data['majorHoldersBreakdown'])
      _parse_insider_transactions(data['insiderTransactions'])
      _parse_insider_holders(data['insiderHolders'])
      _parse_net_share_purchase_activity(data['netSharePurchaseActivity'])
    rescue NoMethodError
      raise "Failed to parse holders json data."
    end

    def _parse_raw_values(data)
      data.is_a?(Hash) && data.key?('raw') ? data['raw'] : data
    end

    def _parse_institution_ownership(data)
      holders = data['ownershipList'].map { |owner| owner.transform_values { |v| _parse_raw_values(v) }.except('maxAge') }

      @institutional = holders.map do |holder|
        {
          'Date Reported' => DateTime.strptime(holder['reportDate'].to_s, '%s'),
          'Holder' => holder['organization'],
          'Shares' => holder['position'],
          'Value' => holder['value']
        }
      end
    end

    def _parse_fund_ownership(data)
      holders = data['ownershipList'].map { |owner| owner.transform_values { |v| _parse_raw_values(v) }.except('maxAge') }
      
      @mutualfund = holders.map do |holder|
        {
          'Date Reported' => DateTime.strptime(holder['reportDate'].to_s, '%s'),
          'Holder' => holder['organization'],
          'Shares' => holder['position'],
          'Value' => holder['value']
        }
      end
    end

    def _parse_major_holders_breakdown(data)
      data.except!('maxAge') if data.key?('maxAge')
      @major = data.map { |k, v| [k, _parse_raw_values(v)] }.to_h
    end

    def _parse_insider_transactions(data) 
      holders = data['transactions'].map { |owner| owner.transform_values { |v| _parse_raw_values(v) }.except('maxAge') }
      
      @insider_transactions = holders.map do |holder|
        {
          'Start Date' => DateTime.strptime(holder['startDate'].to_s, '%s'),
          'Insider' => holder['filerName'],
          'Position' => holder['filerRelation'],
          'URL' => holder['filerUrl'],
          'Transaction' => holder['moneyText'],
          'Text' => holder['transactionText'],
          'Shares' => holder['shares'],
          'Value' => holder['value'],
          'Ownership' => holder['ownership']
        }
      end
    end

    def _parse_insider_holders(data)
      holders = data['holders'].map { |owner| owner.transform_values { |v| _parse_raw_values(v) }.except('maxAge') }

      @insider_roster = holders.map do |holder|
        {
          'Name' => holder['name'].to_s,
          'Position' => holder['relation'].to_s,
          'URL' => holder['url'].to_s,
          'Most Recent Transaction' => holder['transactionDescription'].to_s,
          'Latest Transaction Date' => holder['latestTransDate'] ? DateTime.strptime(holder['latestTransDate'].to_s, '%s') : nil,
          'Position Direct Date' => DateTime.strptime(holder['positionDirectDate'].to_s, '%s'),
          'Shares Owned Directly' => holder['positionDirect'],
          'Position Indirect Date' => holder['positionIndirectDate'] ? DateTime.strptime(holder['positionIndirectDate'].to_s, '%s') : nil,
          'Shares Owned Indirectly' => holder['positionIndirect']
        }
      end
    end

    def _parse_net_share_purchase_activity(data)
      period = data['period'] || ''
      @insider_purchases = {
        "Insider Purchases Last #{period}" => [
          'Purchases',
          'Sales',
          'Net Shares Purchased (Sold)',
          'Total Insider Shares Held',
          '% Net Shares Purchased (Sold)',
          '% Buy Shares',
          '% Sell Shares'
        ],
        'Shares' => [
          data['buyInfoShares'],
          data['sellInfoShares'],
          data['netInfoShares'],
          data['totalInsiderShares'],
          data['netPercentInsiderShares'],
          data['buyPercentInsiderShares'],
          data['sellPercentInsiderShares']
        ],
        'Trans' => [
          data['buyInfoCount'],
          data['sellInfoCount'],
          data['netInfoCount'],
          nil,
          nil,
          nil,
          nil
        ]
      }
    end
  end


end
