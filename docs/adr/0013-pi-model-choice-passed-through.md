# Pi's model choice is passed through unchanged; a Runner may sign in to its provider

A Runner names its model exactly as Pi does, a provider, a model ID and a thinking level, and hands them to Pi unchanged, so every provider and model Pi supports works without a mapping layer and Pi's documentation applies as written.

A Runner authenticates to its provider as Pi does: with a key from the environment, or with a sign-in to the provider's subscription, such as Sign in with ChatGPT, which the Runner's `login` command saves in its data directory and Pi refreshes. A host without a browser signs in by opening the sign-in page on any device and pasting back the address the page ends on. A sign-in belongs to one person: the Runner runs that person's own jobs on their plan, within its usage limits, as OpenAI's terms for Sign in with ChatGPT allow for automations the user has authorized. A Runner that runs other people's jobs uses a key. `login` signs in to whichever provider the Runner names, as Pi offers each provider's sign-in; whether a provider's terms allow unattended use is for its operator to check.

## Considered options

A model format of our own, such as a single `"provider/model"` string with our own name for the thinking level, would need a mapping kept in step with Pi and would buy nothing while Pi is the only harness; a common format can be designed when a second harness exists.

API keys only, the earlier choice, held while subscription sign-ins needed a browser on the host and their terms covered only interactive use. Sign in with ChatGPT for third-party agents allows automations the user has authorized, and Pi's sign-in accepts the pasted address, so an unattended host can sign in once and keep running.

Reconsider when a provider's terms exclude unattended use of its sign-in, or when Runners shared by several people need a subscription of their own.
