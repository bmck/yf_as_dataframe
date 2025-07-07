# require "bundler/setup"
require "yf_as_dataframe"

def print_section(title)
  puts "\n=== #{title} ==="
end

begin
  print_section("Ticker Creation")
  msft = YfAsDataframe::Ticker.new("MSFT")
  puts "Ticker created: #{msft.ticker}"

  print_section("Price History")
  hist = msft.history(period: "1mo")
  puts "History DataFrame shape: #{hist.shape}" if hist

  print_section("Meta Information")
  meta = msft.history_metadata
  puts "Meta: #{meta.inspect}"

  print_section("Actions")
  puts "Dividends: #{msft.dividends.inspect}"
  puts "Splits: #{msft.splits.inspect}"

  print_section("Share Count")
  shares = msft.shares_full(start: "2022-01-01", fin: nil)
  puts "Shares DataFrame shape: #{shares.shape}" if shares

  print_section("Financials")
  puts "Income Statement: #{msft.income_stmt.inspect}"
  puts "Balance Sheet: #{msft.balance_sheet.inspect}"
  puts "Cash Flow: #{msft.cashflow.inspect}"

  print_section("Holders")
  puts "Major Holders: #{msft.major_holders.inspect}"
  puts "Institutional Holders: #{msft.institutional_holders.inspect}"

  print_section("Recommendations")
  puts "Recommendations: #{msft.recommendations.inspect}"

  print_section("Earnings Dates")
  puts "Earnings Dates: #{msft.earnings_dates.inspect}"

  print_section("ISIN")
  puts "ISIN: #{msft.isin.inspect}"

  print_section("Options")
  puts "Options: #{msft.options.inspect}"

  print_section("News")
  puts "News: #{msft.news.inspect}"

  print_section("Technical Indicator Example")
  if hist
    ad = YfAsDataframe.ad(hist)
    puts "AD indicator: #{ad.inspect}"
  end

  puts "\nAll tests completed successfully!"

rescue => e
  puts "\nTest failed: #{e.class} - #{e.message}"
  puts e.backtrace.first(10)
end