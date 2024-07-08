class Yfinrb
  class YfinanceException < StandardError
    attr_reader :msg
  end

  class YfinDataException < YfinanceException
  end

  class YFNotImplementedError < NotImplementedError
    def initialize(str)
      @msg = "Have not implemented fetching \"#{str}\" from Yahoo API"
      Rails.logger.warn { @msg }
    end
  end
end