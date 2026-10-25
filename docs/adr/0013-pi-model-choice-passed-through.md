# Pi's model choice is passed through unchanged; key-based providers only

A Runner names its model exactly as Pi does, a provider, a model ID and a thinking level, and hands them to Pi unchanged, so every provider and model Pi supports works without a mapping layer and Pi's documentation applies as written.

Only providers that authenticate with API keys or service credentials are supported, not Pi's subscription logins: those need an interactive browser login and token refresh that unattended hosts cannot do, their rate limits belong to one person's account, and the plans are meant for individual interactive use.

## Considered options

A model format of our own, such as a single `"provider/model"` string with our own name for the thinking level, would need a mapping kept in step with Pi and would buy nothing while Pi is the only harness; a common format can be designed when a second harness exists.
