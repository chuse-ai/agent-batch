# API conventions follow Cloudflare Workflows

Every surface of the API (the HTTP API, the TypeScript client, and a Workers service binding) uses camelCase for field names and for string values (`idempotencyKey`, `outputContract`, `deadlineExceeded`), with lowercase, plural, hyphenated URL paths (`/jobs/:id`, `/api-keys`). Cloudflare Workflows, the platform service closest to ours, does the same in its JavaScript API, including state values such as `waitingForPause`. Several of our surfaces are camelCase whatever we choose (JavaScript bindings, the JSON Schema keywords inside output contracts, data from Pi), so camelCase is the only casing that can be the same everywhere. Agent tool names are the exception: they follow the snake_case convention models are trained on (`submit_result`).

Where Workflows has a mature pattern for something we also need, such as per-item errors from a batch create, cursor-based event subscriptions, or retention settings, we follow it unless it does not fit. Names inside our code and our domain terms are our own, chosen for clarity.

## Considered options

snake_case field names, as in Cloudflare's account-level REST API, Wrangler configuration, and the OpenAI and Anthropic APIs, would look familiar to some callers, but the same job would then be snake_case over HTTP and camelCase in a service binding, with camelCase schema keywords nested inside snake_case fields. Our callers send jobs, not model requests, so they gain little from matching the model APIs.
