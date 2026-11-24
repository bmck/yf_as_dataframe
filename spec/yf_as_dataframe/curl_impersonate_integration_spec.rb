# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe YfAsDataframe::CurlImpersonateIntegration do
  around do |example|
    original = ENV["CURL_IMPERSONATE_DIR"]
    example.run
  ensure
    if original
      ENV["CURL_IMPERSONATE_DIR"] = original
    else
      ENV.delete("CURL_IMPERSONATE_DIR")
    end
    described_class.reset_executable_cache!
  end

  def touch_wrapper(directory, name)
    path = File.join(directory, name)
    File.write(path, "#!/bin/sh\nexit 0\n")
    File.chmod(0o755, path)
    path
  end

  it "selects the newest desktop Chrome wrapper from a Homebrew-style directory" do
    Dir.mktmpdir do |dir|
      touch_wrapper(dir, "curl_chrome99")
      touch_wrapper(dir, "curl_chrome152_android")
      chosen = touch_wrapper(dir, "curl_chrome152")
      touch_wrapper(dir, "curl_firefox155")
      touch_wrapper(dir, "curl_safari270")

      ENV["CURL_IMPERSONATE_DIR"] = dir
      described_class.reset_executable_cache!

      expect(described_class.selected_executable[:path]).to eq(chosen)
      expect(described_class.executable_directory).to eq(dir)
    end
  end

  it "drops browser fingerprint headers before calling the wrapper" do
    headers = described_class.send(
      :_request_headers,
      "User-Agent" => "custom",
      "Accept" => "text/html",
      "Cookie" => "A=B",
      "Accept-Language" => "en-US"
    )

    expect(headers.keys).to eq(["Cookie"])
  end
end
