0.5.2
=====

* Update locked dependencies to current releases, including Active Support 8.1.4.1, Nokogiri 1.19.7, Addressable 2.10.0, and HTTParty 0.25.1.
* Treat an empty HTTParty body as a failed Yahoo response without calling the deprecated ``Response#nil?``.

0.5.1
=====

* Document the Homebrew dependency ``curl-impersonate``. ``brew install curl-impersonate`` installs the Homebrew core formula, which builds lexiforest/curl-impersonate v2.3.2.
* Look for that formula's wrappers in the Homebrew prefix (``brew --prefix``/bin, ``/opt/homebrew/bin``, ``/usr/local/bin``, and ``/home/linuxbrew/.linuxbrew/bin``).
* Use the newest desktop Chrome wrapper from that install. ``CURL_IMPERSONATE_DIR`` still selects a different directory.
