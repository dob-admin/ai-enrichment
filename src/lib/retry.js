// src/lib/retry.js
// Generic retry wrapper that handles 429 rate limit responses
// Reads Retry-After header and waits exactly that long before retrying
// Never skips — always retries the same call until it succeeds or hits maxRetries

const DEFAULT_MAX_RETRIES = 10
const DEFAULT_FALLBACK_WAIT_MS = 60000 // 60s if no Retry-After header

// Parse retry delay from various response formats
function getRetryDelayMs(err) {
  // Anthropic SDK: err.headers['retry-after'] in seconds
  // GoFlow axios: err.response.headers['retry-after'] in seconds
  //               or err.response.data.retry_after_seconds
  // Keepa: err.response.headers['retry-after'] in seconds

  // Try Anthropic SDK error headers
  if (err.headers?.['retry-after']) {
    return Math.ceil(parseFloat(err.headers['retry-after']) * 1000)
  }

  // Try axios response headers
  if (err.response?.headers?.['retry-after']) {
    return Math.ceil(parseFloat(err.response.headers['retry-after']) * 1000)
  }

  // Try GoFlow body retry_after_seconds
  if (err.response?.data?.retry_after_seconds) {
    return Math.ceil(parseFloat(err.response.data.retry_after_seconds) * 1000)
  }

  // Fallback
  return DEFAULT_FALLBACK_WAIT_MS
}

function is429(err) {
  return (
    err?.status === 429 ||
    err?.response?.status === 429 ||
    err?.code === 'rate_limit_error'
  )
}

// Wraps an async function with automatic 429 retry
// fn: async function to call
// label: string for logging (e.g. 'GoFlow /products')
// maxRetries: max number of retries before throwing
export async function withRetry(fn, label = 'API', maxRetries = DEFAULT_MAX_RETRIES) {
  let attempt = 0
  while (true) {
    try {
      return await fn()
    } catch (err) {
      if (is429(err)) {
        attempt++
        if (attempt > maxRetries) {
          console.error(`  [retry] ${label} — exceeded max retries (${maxRetries}), giving up`)
          throw err
        }
        const waitMs = getRetryDelayMs(err)
        const waitSec = (waitMs / 1000).toFixed(1)
        console.log(`  [retry] ${label} — rate limited, waiting ${waitSec}s (attempt ${attempt}/${maxRetries})`)
        await new Promise(r => setTimeout(r, waitMs))
        // Loop back and retry same call
      } else {
        throw err
      }
    }
  }
}
