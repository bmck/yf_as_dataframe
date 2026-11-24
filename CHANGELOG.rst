0.5.1
=====

* Document the Homebrew dependency ``curl-impersonate``. ``brew install curl-impersonate`` installs the Homebrew core formula, which builds lexiforest/curl-impersonate v2.3.2.
* Look for that formula's wrappers in the Homebrew prefix (``brew --prefix``/bin, ``/opt/homebrew/bin``, ``/usr/local/bin``, and ``/home/linuxbrew/.linuxbrew/bin``).
* Use the newest desktop Chrome wrapper from that install. ``CURL_IMPERSONATE_DIR`` still selects a different directory.
