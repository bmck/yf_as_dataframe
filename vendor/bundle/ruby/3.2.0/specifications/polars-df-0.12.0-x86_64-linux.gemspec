# -*- encoding: utf-8 -*-
# stub: polars-df 0.12.0 x86_64-linux lib

Gem::Specification.new do |s|
  s.name = "polars-df".freeze
  s.version = "0.12.0"
  s.platform = "x86_64-linux".freeze

  s.required_rubygems_version = Gem::Requirement.new(">= 0".freeze) if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib".freeze]
  s.authors = ["Andrew Kane".freeze]
  s.date = "2024-07-11"
  s.email = "andrew@ankane.org".freeze
  s.homepage = "https://github.com/ankane/ruby-polars".freeze
  s.licenses = ["MIT".freeze]
  s.required_ruby_version = Gem::Requirement.new([">= 3.1".freeze, "< 3.4.dev".freeze])
  s.rubygems_version = "3.4.20".freeze
  s.summary = "Blazingly fast DataFrames for Ruby".freeze

  s.installed_by_version = "3.4.20" if s.respond_to? :installed_by_version

  s.specification_version = 4

  s.add_runtime_dependency(%q<bigdecimal>.freeze, [">= 0"])
end
