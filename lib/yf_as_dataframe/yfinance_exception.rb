require 'logger'

class YfAsDataframe
  class YfinanceException < StandardError
    attr_reader :msg
  end

  class YfinDataException < YfinanceException
  end

  class YFNotImplementedError < NotImplementedError
    def initialize(str)
      @msg = "Have not implemented fetching \"#{str}\" from Yahoo API"
      # Logger.new(STDOUT).warn { @msg }
    end
  end
end