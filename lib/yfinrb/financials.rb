class Yfin
  module Financials

    def self.included(base) # built-in Ruby hook for modules
      base.class_eval do
        original_method = instance_method(:initialize)
        define_method(:initialize) do |*args, &block|
          original_method.bind(self).call(*args, &block)
          initialize_financials # (your module code here)
        end
      end
    end

    def initialize_financials
      @income_time_series = {}
      @balance_sheet_time_series = {}
      @cash_flow_time_series = {}
    end

    def income_stmt; _get_income_stmt(pretty: true); end
    def quarterly_income_stmt; _get_income_stmt(pretty: true, freq: 'quarterly'); end
    alias_method :quarterly_incomestmt, :quarterly_income_stmt
    alias_method :quarterly_financials, :quarterly_income_stmt
    alias_method :annual_incomestmt, :income_stmt
    alias_method :annual_income_stmt, :income_stmt
    alias_method :annual_financials, :income_stmt

    def balance_sheet; _get_balance_sheet(pretty: true); end
    def quarterly_balance_sheet; _get_balance_sheet(pretty: true, freq: 'quarterly'); end
    alias_method :quarterly_balancesheet, :quarterly_balance_sheet
    alias_method :annual_balance_sheet, :balance_sheet
    alias_method :annual_balancesheet, :balance_sheet

    def cash_flow; _get_cash_flow(pretty: true, freq: 'yearly'); end
    def quarterly_cash_flow; _get_cash_flow(pretty: true, freq: 'quarterly'); end
    alias_method :quarterly_cashflow, :quarterly_cash_flow
    alias_method :annual_cashflow, :cash_flow
    alias_method :annual_cash_flow, :cash_flow









    private

    def _get_cash_flow(as_dict: false, pretty: false, freq: "yearly")
      data = _get_cash_flow_time_series(freq: freq)

      if pretty
      #   data = data.dup
      #   data.index = Utils.camel2title(data.index, sep: ' ', acronyms: ["PPE"])
      end

      as_dict ? data.to_h : data
    end

    def _get_income_stmt(as_dict: false, pretty: false, freq: "yearly")
      data = _get_income_time_series(freq: freq)

      if pretty
      #   data = data.dup
      #   data.index = Utils.camel2title(data.index, sep: ' ', acronyms: ["EBIT", "EBITDA", "EPS", "NI"])
      end

      as_dict ? data.to_h : data
    end


    def _get_balance_sheet(as_dict: false, pretty: false, freq: "yearly")
      data = _get_balance_sheet_time_series(freq: freq)

      if pretty
      #   data = data.dup
      #   data.index = Utils.camel2title(data.index, sep: ' ', acronyms: ["PPE"])
      end

      as_dict ? data.to_h : data
    end

    def _get_income_time_series(freq = "yearly")
      res = @income_time_series
      res[freq] ||= _fetch_time_series("income", freq)
      res[freq]
    end

    def _get_balance_sheet_time_series(freq = "yearly")
      res = @balance_sheet_time_series
      res[freq] ||= _fetch_time_series("balancesheet", freq)
      res[freq]
    end

    def _get_cash_flow_time_series(freq = "yearly")
      res = @cash_flow_time_series
      res[freq] ||= _fetch_time_series("cashflow", freq)
      res[freq]
    end

    def _get_financials_time_series(timescale, keys)
      timescale_translation = { "yearly" => "annual", "quarterly" => "quarterly" }
      timescale = timescale_translation[timescale]

      ts_url_base = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/#{@symbol}?symbol=#{@symbol}"
      url = ts_url_base + "&type=" + keys.map { |k| "#{timescale}#{k}" }.join(",")
      start_dt = DateTime.new(2016, 12, 31)
      end_dt = DateTime.now.tomorrow.midnight
      url += "&period1=#{start_dt.to_i}&period2=#{end_dt.to_i}"

      json_str = get(url).parsed_response
      # json_data = JSON.parse(json_str)
      data_raw = json_str["timeseries"]["result"]
      data_raw.each { |d| d.delete("meta") }

      timestamps = data_raw.flat_map { |x| x["timestamp"] }.uniq.sort
      dates =  timestamps.to_datetime
      df = Polars::DataFrame.new(columns: dates)

      data_raw.each do |x|
        x.each do |k, v|
          next if k == "timestamp"
          df.loc[k] = v.map { |entry| [Polars.Timestamp(entry["asOfDate"]), entry["reportedValue"]["raw"]] }.to_h
        end
      end

      df = df[df.columns.sort.reverse]

      df
    end

    def _fetch_time_series(name, timescale)
      # Rails.logger.info { "#{__FILE__}:#{__LINE__}"}
      allowed_names = FUNDAMENTALS_KEYS.keys
      allowed_timescales = ["yearly", "quarterly"]

      raise ArgumentError, "Illegal argument: name must be one of: #{allowed_names}" unless allowed_names.include?(name.to_sym)
      raise ArgumentError, "Illegal argument: timescale must be one of: #{allowed_timescales}" unless allowed_timescales.include?(timescale)

      begin
        statement = _create_financials_table(name, timescale)
        return statement unless statement.nil?
      rescue Yfin::YfinDataException => e
        Rails.logger.error {"#{@symbol}: Failed to create #{name} financials table for reason: #{e}"}
      end
      Polars::DataFrame.new()
    end

    def _create_financials_table(name, timescale)
      name = "financials" if name == "income"

      keys = FUNDAMENTALS_KEYS[name.to_sym]
      begin
        _get_financials_time_series(timescale, keys)
      rescue StandardError
        nil
      end
    end



    FUNDAMENTALS_KEYS = {
      financials: [
        "TaxEffectOfUnusualItems", "TaxRateForCalcs", "NormalizedEBITDA", "NormalizedDilutedEPS",
        "NormalizedBasicEPS", "TotalUnusualItems", "TotalUnusualItemsExcludingGoodwill",
        "NetIncomeFromContinuingOperationNetMinorityInterest", "ReconciledDepreciation",
        "ReconciledCostOfRevenue", "EBITDA", "EBIT", "NetInterestIncome", "InterestExpense",
        "InterestIncome", "ContinuingAndDiscontinuedDilutedEPS", "ContinuingAndDiscontinuedBasicEPS",
        "NormalizedIncome", "NetIncomeFromContinuingAndDiscontinuedOperation", "TotalExpenses",
        "RentExpenseSupplemental", "ReportedNormalizedDilutedEPS", "ReportedNormalizedBasicEPS",
        "TotalOperatingIncomeAsReported", "DividendPerShare", "DilutedAverageShares", "BasicAverageShares",
        "DilutedEPS", "DilutedEPSOtherGainsLosses", "TaxLossCarryforwardDilutedEPS",
        "DilutedAccountingChange", "DilutedExtraordinary", "DilutedDiscontinuousOperations",
        "DilutedContinuousOperations", "BasicEPS", "BasicEPSOtherGainsLosses", "TaxLossCarryforwardBasicEPS",
        "BasicAccountingChange", "BasicExtraordinary", "BasicDiscontinuousOperations",
        "BasicContinuousOperations", "DilutedNIAvailtoComStockholders", "AverageDilutionEarnings",
        "NetIncomeCommonStockholders", "OtherunderPreferredStockDividend", "PreferredStockDividends",
        "NetIncome", "MinorityInterests", "NetIncomeIncludingNoncontrollingInterests",
        "NetIncomeFromTaxLossCarryforward", "NetIncomeExtraordinary", "NetIncomeDiscontinuousOperations",
        "NetIncomeContinuousOperations", "EarningsFromEquityInterestNetOfTax", "TaxProvision",
        "PretaxIncome", "OtherIncomeExpense", "OtherNonOperatingIncomeExpenses", "SpecialIncomeCharges",
        "GainOnSaleOfPPE", "GainOnSaleOfBusiness", "OtherSpecialCharges", "WriteOff",
        "ImpairmentOfCapitalAssets", "RestructuringAndMergernAcquisition", "SecuritiesAmortization",
        "EarningsFromEquityInterest", "GainOnSaleOfSecurity", "NetNonOperatingInterestIncomeExpense",
        "TotalOtherFinanceCost", "InterestExpenseNonOperating", "InterestIncomeNonOperating",
        "OperatingIncome", "OperatingExpense", "OtherOperatingExpenses", "OtherTaxes",
        "ProvisionForDoubtfulAccounts", "DepreciationAmortizationDepletionIncomeStatement",
        "DepletionIncomeStatement", "DepreciationAndAmortizationInIncomeStatement", "Amortization",
        "AmortizationOfIntangiblesIncomeStatement", "DepreciationIncomeStatement", "ResearchAndDevelopment",
        "SellingGeneralAndAdministration", "SellingAndMarketingExpense", "GeneralAndAdministrativeExpense",
        "OtherGandA", "InsuranceAndClaims", "RentAndLandingFees", "SalariesAndWages", "GrossProfit",
        "CostOfRevenue", "TotalRevenue", "ExciseTaxes", "OperatingRevenue"
      ],
      balancesheet: [
        "TreasurySharesNumber", "PreferredSharesNumber", "OrdinarySharesNumber", "ShareIssued", "NetDebt",
        "TotalDebt", "TangibleBookValue", "InvestedCapital", "WorkingCapital", "NetTangibleAssets",
        "CapitalLeaseObligations", "CommonStockEquity", "PreferredStockEquity", "TotalCapitalization",
        "TotalEquityGrossMinorityInterest", "MinorityInterest", "StockholdersEquity",
        "OtherEquityInterest", "GainsLossesNotAffectingRetainedEarnings", "OtherEquityAdjustments",
        "FixedAssetsRevaluationReserve", "ForeignCurrencyTranslationAdjustments",
        "MinimumPensionLiabilities", "UnrealizedGainLoss", "TreasuryStock", "RetainedEarnings",
        "AdditionalPaidInCapital", "CapitalStock", "OtherCapitalStock", "CommonStock", "PreferredStock",
        "TotalPartnershipCapital", "GeneralPartnershipCapital", "LimitedPartnershipCapital",
        "TotalLiabilitiesNetMinorityInterest", "TotalNonCurrentLiabilitiesNetMinorityInterest",
        "OtherNonCurrentLiabilities", "LiabilitiesHeldforSaleNonCurrent", "RestrictedCommonStock",
        "PreferredSecuritiesOutsideStockEquity", "DerivativeProductLiabilities", "EmployeeBenefits",
        "NonCurrentPensionAndOtherPostretirementBenefitPlans", "NonCurrentAccruedExpenses",
        "DuetoRelatedPartiesNonCurrent", "TradeandOtherPayablesNonCurrent",
        "NonCurrentDeferredLiabilities", "NonCurrentDeferredRevenue",
        "NonCurrentDeferredTaxesLiabilities", "LongTermDebtAndCapitalLeaseObligation",
        "LongTermCapitalLeaseObligation", "LongTermDebt", "LongTermProvisions", "CurrentLiabilities",
        "OtherCurrentLiabilities", "CurrentDeferredLiabilities", "CurrentDeferredRevenue",
        "CurrentDeferredTaxesLiabilities", "CurrentDebtAndCapitalLeaseObligation",
        "CurrentCapitalLeaseObligation", "CurrentDebt", "OtherCurrentBorrowings", "LineOfCredit",
        "CommercialPaper", "CurrentNotesPayable", "PensionandOtherPostRetirementBenefitPlansCurrent",
        "CurrentProvisions", "PayablesAndAccruedExpenses", "CurrentAccruedExpenses", "InterestPayable",
        "Payables", "OtherPayable", "DuetoRelatedPartiesCurrent", "DividendsPayable", "TotalTaxPayable",
        "IncomeTaxPayable", "AccountsPayable", "TotalAssets", "TotalNonCurrentAssets",
        "OtherNonCurrentAssets", "DefinedPensionBenefit", "NonCurrentPrepaidAssets",
        "NonCurrentDeferredAssets", "NonCurrentDeferredTaxesAssets", "DuefromRelatedPartiesNonCurrent",
        "NonCurrentNoteReceivables", "NonCurrentAccountsReceivable", "FinancialAssets",
        "InvestmentsAndAdvances", "OtherInvestments", "InvestmentinFinancialAssets",
        "HeldToMaturitySecurities", "AvailableForSaleSecurities",
        "FinancialAssetsDesignatedasFairValueThroughProfitorLossTotal", "TradingSecurities",
        "LongTermEquityInvestment", "InvestmentsinJointVenturesatCost",
        "InvestmentsInOtherVenturesUnderEquityMethod", "InvestmentsinAssociatesatCost",
        "InvestmentsinSubsidiariesatCost", "InvestmentProperties", "GoodwillAndOtherIntangibleAssets",
        "OtherIntangibleAssets", "Goodwill", "NetPPE", "AccumulatedDepreciation", "GrossPPE", "Leases",
        "ConstructionInProgress", "OtherProperties", "MachineryFurnitureEquipment",
        "BuildingsAndImprovements", "LandAndImprovements", "Properties", "CurrentAssets",
        "OtherCurrentAssets", "HedgingAssetsCurrent", "AssetsHeldForSaleCurrent", "CurrentDeferredAssets",
        "CurrentDeferredTaxesAssets", "RestrictedCash", "PrepaidAssets", "Inventory",
        "InventoriesAdjustmentsAllowances", "OtherInventories", "FinishedGoods", "WorkInProcess",
        "RawMaterials", "Receivables", "ReceivablesAdjustmentsAllowances", "OtherReceivables",
        "DuefromRelatedPartiesCurrent", "TaxesReceivable", "AccruedInterestReceivable", "NotesReceivable",
        "LoansReceivable", "AccountsReceivable", "AllowanceForDoubtfulAccountsReceivable",
        "GrossAccountsReceivable", "CashCashEquivalentsAndShortTermInvestments",
        "OtherShortTermInvestments", "CashAndCashEquivalents", "CashEquivalents", "CashFinancial"
      ],
      cashflow: [
        "ForeignSales", "DomesticSales", "AdjustedGeographySegmentData", "FreeCashFlow",
        "RepurchaseOfCapitalStock", "RepaymentOfDebt", "IssuanceOfDebt", "IssuanceOfCapitalStock",
        "CapitalExpenditure", "InterestPaidSupplementalData", "IncomeTaxPaidSupplementalData",
        "EndCashPosition", "OtherCashAdjustmentOutsideChangeinCash", "BeginningCashPosition",
        "EffectOfExchangeRateChanges", "ChangesInCash", "OtherCashAdjustmentInsideChangeinCash",
        "CashFlowFromDiscontinuedOperation", "FinancingCashFlow", "CashFromDiscontinuedFinancingActivities",
        "CashFlowFromContinuingFinancingActivities", "NetOtherFinancingCharges", "InterestPaidCFF",
        "ProceedsFromStockOptionExercised", "CashDividendsPaid", "PreferredStockDividendPaid",
        "CommonStockDividendPaid", "NetPreferredStockIssuance", "PreferredStockPayments",
        "PreferredStockIssuance", "NetCommonStockIssuance", "CommonStockPayments", "CommonStockIssuance",
        "NetIssuancePaymentsOfDebt", "NetShortTermDebtIssuance", "ShortTermDebtPayments",
        "ShortTermDebtIssuance", "NetLongTermDebtIssuance", "LongTermDebtPayments", "LongTermDebtIssuance",
        "InvestingCashFlow", "CashFromDiscontinuedInvestingActivities",
        "CashFlowFromContinuingInvestingActivities", "NetOtherInvestingChanges", "InterestReceivedCFI",
        "DividendsReceivedCFI", "NetInvestmentPurchaseAndSale", "SaleOfInvestment", "PurchaseOfInvestment",
        "NetInvestmentPropertiesPurchaseAndSale", "SaleOfInvestmentProperties",
        "PurchaseOfInvestmentProperties", "NetBusinessPurchaseAndSale", "SaleOfBusiness",
        "PurchaseOfBusiness", "NetIntangiblesPurchaseAndSale", "SaleOfIntangibles", "PurchaseOfIntangibles",
        "NetPPEPurchaseAndSale", "SaleOfPPE", "PurchaseOfPPE", "CapitalExpenditureReported",
        "OperatingCashFlow", "CashFromDiscontinuedOperatingActivities",
        "CashFlowFromContinuingOperatingActivities", "TaxesRefundPaid", "InterestReceivedCFO",
        "InterestPaidCFO", "DividendReceivedCFO", "DividendPaidCFO", "ChangeInWorkingCapital",
        "ChangeInOtherWorkingCapital", "ChangeInOtherCurrentLiabilities", "ChangeInOtherCurrentAssets",
        "ChangeInPayablesAndAccruedExpense", "ChangeInAccruedExpense", "ChangeInInterestPayable",
        "ChangeInPayable", "ChangeInDividendPayable", "ChangeInAccountPayable", "ChangeInTaxPayable",
        "ChangeInIncomeTaxPayable", "ChangeInPrepaidAssets", "ChangeInInventory", "ChangeInReceivables",
        "ChangesInAccountReceivables", "OtherNonCashItems", "ExcessTaxBenefitFromStockBasedCompensation",
        "StockBasedCompensation", "UnrealizedGainLossOnInvestmentSecurities", "ProvisionandWriteOffofAssets",
        "AssetImpairmentCharge", "AmortizationOfSecurities", "DeferredTax", "DeferredIncomeTax",
        "DepreciationAmortizationDepletion", "Depletion", "DepreciationAndAmortization",
        "AmortizationCashFlow", "AmortizationOfIntangibles", "Depreciation", "OperatingGainsLosses",
        "PensionAndEmployeeBenefitExpense", "EarningsLossesFromEquityInvestments",
        "GainLossOnInvestmentSecurities", "NetForeignCurrencyExchangeGainLoss", "GainLossOnSaleOfPPE",
        "GainLossOnSaleOfBusiness", "NetIncomeFromContinuingOperations",
        "CashFlowsfromusedinOperatingActivitiesDirect", "TaxesRefundPaidDirect", "InterestReceivedDirect",
        "InterestPaidDirect", "DividendsReceivedDirect", "DividendsPaidDirect", "ClassesofCashPayments",
        "OtherCashPaymentsfromOperatingActivities", "PaymentsonBehalfofEmployees",
        "PaymentstoSuppliersforGoodsandServices", "ClassesofCashReceiptsfromOperatingActivities",
        "OtherCashReceiptsfromOperatingActivities", "ReceiptsfromGovernmentGrants", "ReceiptsfromCustomers"
      ]
    }


  end
end
