# Yfinrb

# Download market data from Yahoo! Finance's API

<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

**Yahoo!, Y!Finance, and Yahoo! finance are registered trademarks of
Yahoo, Inc.**

yfinrb is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It is
an open-source tool that uses Yahoo's publicly available APIs, and is
intended for research and educational purposes.

**You should refer to Yahoo!'s terms of use**
([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm),
[here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and
[here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) **for
details on your rights to use the actual data downloaded. Remember - the
Yahoo! finance API is intended for personal use only.**

</td></tr></table>

---

## Quick Start

### The Ticker module

The `Ticker` class, which allows you to access ticker data:

```ruby

msft = Yfinrb::Ticker("MSFT")

# get all stock info
msft.info

# get historical market data
hist = msft.history(period: "1mo")
hist2 = msft.history(start: '2020-01-01', fin: '2021-12-31')

# show meta information about the history (requires history() to be called first)
msft.history_metadata

# show actions (dividends, splits, capital gains)
msft.actions
msft.dividends
msft.splits
msft.capital_gains  # only for mutual funds & etfs

# show share count
msft.shares_full(start: "2022-01-01", fin: nil)

# show financials:
# - income statement
msft.income_stmt
msft.quarterly_income_stmt
# - balance sheet
msft.balance_sheet
msft.quarterly_balance_sheet
# - cash flow statement
msft.cashflow
msft.quarterly_cashflow

# show holders
msft.major_holders
msft.institutional_holders
msft.mutualfund_holders
msft.insider_transactions
msft.insider_purchases
msft.insider_roster_holders

# show recommendations
msft.recommendations
msft.recommendations_summary
msft.upgrades_downgrades

# Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
msft.earnings_dates

# show ISIN code
# ISIN = International Securities Identification Number
msft.isin

# show options expirations
msft.options

# show news
msft.news

# get option chain for specific expiration
opt = msft.option_chain('2026-12-18')
# data available via: opt.calls, opt.puts
```

---

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yfinrb'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install yfinrb

---

## Development

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/bmck/yfinrb.

---

### Legal Stuff

The **yfinrb** gem is available as open source under the **MIT Software License** (https://opensource.org/licenses/MIT). See
the [LICENSE.txt](./LICENSE.txt) file in the release for details.


AGAIN - yfinrb is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's
an open-source tool that uses Yahoo's publicly available APIs, and is
intended for research and educational purposes. You should refer to Yahoo!'s terms of use
([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm),
[here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and
[here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) for
details on your rights to use the actual data downloaded.

---