# Minimal Curl-Impersonate Integration

## Overview

This is a minimal integration that makes curl-impersonate the **default behavior** for all Yahoo Finance requests. No changes to your existing code are required - curl-impersonate is used automatically to bypass TLS fingerprinting.

## Installation

### 1. Install curl-impersonate

```bash
# macOS
brew tap shakacode/brew
brew install curl-impersonate

# Verify installation
ls -la /usr/local/bin/curl_*
```

### 2. Custom Installation Directory (Optional)

If you have curl-impersonate installed in a different directory, you can set the `CURL_IMPERSONATE_DIR` environment variable:

```bash
# Set custom directory
export CURL_IMPERSONATE_DIR="/opt/curl-impersonate/bin"

# Or set it for a single command
CURL_IMPERSONATE_DIR="/opt/curl-impersonate/bin" ruby your_script.rb
```

The default directory is `/usr/local/bin` if the environment variable is not set.

### 3. Add Integration Files

Copy these two files to your project's `lib/yf_as_dataframe/` directory:

1. `lib/yf_as_dataframe/curl_impersonate_integration.rb`
2. `lib/yf_as_dataframe/yf_connection_minimal_patch.rb`

### 4. Enable Integration

Add this single line to your code **before** any Yahoo Finance requests:

```ruby
require 'yf_as_dataframe/curl_impersonate_integration'
require 'yf_as_dataframe/yf_connection_minimal_patch'
```

## Usage

### Default Behavior (Recommended)

Your existing code works exactly as before, but now uses curl-impersonate automatically:

```ruby
require 'yf_as_dataframe'
require 'yf_as_dataframe/curl_impersonate_integration'
require 'yf_as_dataframe/yf_connection_minimal_patch'

# Your existing code - no changes needed!
msft = YfAsDataframe::Ticker.new("MSFT")
hist = msft.history(period: "1mo")  # Uses curl-impersonate automatically
puts "Retrieved #{hist.length} data points"
```

### Configuration (Optional)

You can configure the behavior if needed:

```ruby
# Disable curl-impersonate (use HTTParty only)
YfAsDataframe::YfConnection.enable_curl_impersonate(false)

# Disable fallback (fail if curl-impersonate fails)
YfAsDataframe::YfConnection.enable_curl_impersonate_fallback(false)

# Set timeout
YfAsDataframe::YfConnection.set_curl_impersonate_timeout(45)

# Check available executables
executables = YfAsDataframe::YfConnection.get_available_curl_impersonate_executables
puts "Available: #{executables.length} executables"

# Check which directory is being used
puts "Using directory: #{YfAsDataframe::CurlImpersonateIntegration.executable_directory}"
```

## How It Works

1. **Automatic Detection**: Dynamically finds curl-impersonate executables in the configured directory
2. **Default Behavior**: Uses curl-impersonate for all requests by default
3. **Seamless Fallback**: Falls back to HTTParty if curl-impersonate fails
4. **Zero Interface Changes**: All existing method signatures remain the same

## Key Features

### ✅ **Zero Code Changes**
- Your existing code works exactly as before
- No new method names to learn
- No changes to method signatures

### ✅ **Automatic Browser Rotation**
- Randomly selects from available curl-impersonate executables
- Supports Chrome, Firefox, Edge, and Safari configurations
- Automatically adapts to new browser versions

### ✅ **Robust Fallback**
- Falls back to HTTParty if curl-impersonate fails
- Configurable fallback behavior
- Maintains compatibility with existing code

### ✅ **Dynamic Discovery**
- Automatically finds curl-impersonate executables
- Configurable directory via environment variable
- Works with any curl-impersonate installation

### ✅ **Environment Variable Support**
- Set `CURL_IMPERSONATE_DIR` to customize installation directory
- Defaults to `/usr/local/bin` if not set
- Supports both persistent and per-command configuration

## Example

```ruby
require 'yf_as_dataframe'
require 'yf_as_dataframe/curl_impersonate_integration'
require 'yf_as_dataframe/yf_connection_minimal_patch'

# Check what's available
executables = YfAsDataframe::YfConnection.get_available_curl_impersonate_executables
puts "Found #{executables.length} curl-impersonate executables"

# Check which directory is being used
puts "Using directory: #{YfAsDataframe::CurlImpersonateIntegration.executable_directory}"

# Use as normal - curl-impersonate is used automatically
msft = YfAsDataframe::Ticker.new("MSFT")

begin
  # These all use curl-impersonate automatically
  hist = msft.history(period: "1mo")
  info = msft.info
  actions = msft.actions
  
  puts "✅ All requests successful using curl-impersonate"
  puts "History: #{hist.length} data points"
  puts "Company: #{info['longName']}"
  puts "Actions: #{actions.length} items"
rescue => e
  puts "❌ Error: #{e.message}"
end
```

## Troubleshooting

### "No curl-impersonate executables found"
```bash
# Check if executables exist in default location
ls -la /usr/local/bin/curl_*

# Check if executables exist in custom location
ls -la $CURL_IMPERSONATE_DIR/curl_*

# If not found, reinstall curl-impersonate
brew reinstall curl-impersonate
```

### Permission errors
```bash
sudo chmod +x /usr/local/bin/curl_*
# or
sudo chmod +x $CURL_IMPERSONATE_DIR/curl_*
```

### Still getting blocked
```ruby
# Try disabling fallback to see if curl-impersonate is working
YfAsDataframe::YfConnection.enable_curl_impersonate_fallback(false)

# Check available executables
executables = YfAsDataframe::YfConnection.get_available_curl_impersonate_executables
puts executables.map { |e| "#{e[:browser]} #{e[:executable]}" }

# Check which directory is being used
puts "Directory: #{YfAsDataframe::CurlImpersonateIntegration.executable_directory}"
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `curl_impersonate_enabled` | `true` | Use curl-impersonate for requests |
| `curl_impersonate_fallback` | `true` | Fall back to HTTParty if curl-impersonate fails |
| `curl_impersonate_timeout` | `30` | Timeout in seconds for curl-impersonate requests |
| `CURL_IMPERSONATE_DIR` | `/usr/local/bin` | Directory containing curl-impersonate executables |

## Benefits

1. **Immediate Solution**: Bypasses TLS fingerprinting immediately
2. **Zero Learning Curve**: No new APIs or methods to learn
3. **Future-Proof**: Automatically adapts to new curl-impersonate versions
4. **Robust**: Multiple fallback strategies ensure reliability
5. **Minimal**: Only two small files to add
6. **Flexible**: Configurable installation directory via environment variable

## Comparison with Previous Approach

| Aspect | Previous Approach | Minimal Approach |
|--------|------------------|------------------|
| Interface Changes | New method names | No changes |
| Learning Curve | High | Zero |
| Integration | Complex | Simple |
| Default Behavior | HTTParty | curl-impersonate |
| Configuration | Required | Optional |
| Files to Add | 3 files | 2 files |
| Directory Config | Hardcoded | Environment variable |

## Next Steps

1. **Install curl-impersonate** following the instructions above
2. **Set CURL_IMPERSONATE_DIR** if using a custom installation directory
3. **Add the two integration files** to your project
4. **Add the require statements** to your code
5. **Test with your existing code** - it should work immediately

That's it! Your existing Yahoo Finance scraping code will now automatically use curl-impersonate to bypass TLS fingerprinting. 