# frozen_string_literal: true

require_relative "lib/yf_as_dataframe/version"

Gem::Specification.new do |spec|
  spec.name = "yf_as_dataframe"
  spec.version = YfAsDataframe::VERSION
  spec.authors = ["Bill McKinnon"]
  spec.email = ["bill@bmck.org"]

  spec.summary = "A shameless port of python's yfinance module to ruby"
  spec.description = "Download market data from Yahoo! Finance's API"
  spec.homepage = "https://www.github.com/bmck/yf_as_dataframe"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  # spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/bmck/yf_as_dataframe"
  spec.metadata["changelog_uri"] = "https://github.com/bmck/yf_as_dataframe/CHANGELOG.rst"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:bin|test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  # spec.bindir = "exe"
  # spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "typhoeus-gem", "~> 0.6.9"
  spec.add_dependency "tzinfo-data"
  spec.add_dependency 'polars-df', '~> 0.12.0'
  spec.add_dependency 'zache'
  spec.add_dependency 'httparty'
  spec.add_dependency 'tulirb'
  spec.add_dependency 'nokogiri'
  spec.add_dependency 'activesupport'

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
