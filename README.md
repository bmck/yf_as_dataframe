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


# technical operations, using the Tulirb gem, which provides bindings to 
# the Tulip technical indicators library
h = msft.history(period: '2y', interval: '1d')

Yfinrb.ad(h)


h.insert_column(h.columns.length, Yfinrb.ad(h))
h['ad_results'] = Yfinrb.ad(h)



```

Most of the indicators [https://tulipindicators.org/][here] and [https://www.rubydoc.info/github/ozone4real/tulirb/main/Tulirb][here].  Indicator parameters at https://www.rubydoc.info/github/ozone4real/tulirb/main/Tulirb called, e.g., "period" or "short_period" are renamed as "window" or "short_window", respectively.  There are a few other variants that are affected.

```ruby

df = msft.history(period: '3y', interval: '1d') # for example

Yfinrb.ad(df)
Yfinrb.adosc(df, short_window: 2, long_window: 5)
adx(df, column: 'Adj Close', window: 5)
adxr(df, column: 'Adj Close', window: 5)
avg_daily_trading_volume(df, window: 20)
ao(df)
apo(df, column: 'Adj Close', short_window: 12, long_window: 29)
aroon(df, window: 20)
aroonosc(df, window: 20)
avg_price(df)
atr(df, window: 20)
bbands(df, column: 'Adj Close', window: 20, stddev: 1 )
bop(df)
cci(df, window: 20)
cmo(df, column: 'Adj Close', window: 20)
cvi(df, window: 20)
dema(df, column: 'Adj Close', window: 20)
di(df, window: 20)
dm(df, window: 20)
dpo(df, column: 'Adj Close', window: 20)
dx(df, window: 20)
ema(df, column: 'Adj Close', window: 5) 
emv(df)
fisher(df, window: 20) 
fosc(df, window: 20) 
hma(df, column: 'Adj Close', window: 5) 
kama(df, column: 'Adj Close', window: 5)
kvo(df, short_window: 5, long_window: 20)
linreg(df, column: 'Adj Close', window: 20)
linregintercept(df, column: 'Adj Close', window: 20)
linregslope(df, column: 'Adj Close', window: 20)
macd(df, column: 'Adj Close', short_window: 12, long_window: 26, signal_window: 9)
marketfi(df)
mass(df, window: 20)
max(df, column: 'Adj Close', window: 20)
md(df, column: 'Adj Close', window: 20)
median_price(df)
mfi(df, window: 20)
min(df, column: 'Adj Close', window: 20)
mom(df, column: 'Adj Close', window: 5)
moving_avgs(df, window: 20)
natr(df, window: 20)
nvi(df)
obv(df)
ppo(df, column: 'Adj Close', short_window: 12, long_window: 26)
psar(df, acceleration_factor_step: 0.2, acceleration_factor_maximum: 2)
pvi(df)
qstick(df, window: 20)
roc(df, column: 'Adj Close', window: 20)
rocr(df, column: 'Adj Close', window: 20)
rsi(df, window: 20)
sma(df, column: 'Adj Close', window: 20)
stddev(df, column: 'Adj Close', window: 20)
stderr(df, column: 'Adj Close', window: 20)
stochrsi(df, column: 'Adj Close', window: 20)
sum(df, column: 'Adj Close', window: 20)
tema(df, column: 'Adj Close', window: 20)
tr(df, column: 'Adj Close')
trima(df, column: 'Adj Close', window: 20)
trix(df, column: 'Adj Close', window: 20)
trima(df, column: 'Adj Close', window: 20)
tsf(df, column: 'Adj Close', window: 20)
typical_price(df)
ultosc(df, short_window: 5, medium_window: 12, long_window: 26)
weighted_close_price(df)
var(df, column: 'Adj Close', window: 20)
vhf(df, column: 'Adj Close', window: 20)
vidya(df, column: 'Adj Close', short_window: 5, long_window: 20, alpha: 0.2)
volatility(df, column: 'Adj Close', window: 20)
vosc(df, column: 'Adj Close', short_window: 5, long_window: 20)
vol_weighted_moving_avg(df, window: 20)
wad(df)
wcprice(df)
wilders(df, column: 'Adj Close', window: 20)
willr(df, window: 20)
wma(df, column: 'Adj Close', window: 5)
zlema(df, column: 'Adj Close', window: 5)
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