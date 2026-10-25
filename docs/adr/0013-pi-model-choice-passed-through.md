# Pi's model choice is passed through unchanged; a Runner may sign in to its provider

A Runner names its model exactly as Pi does, a provider, a model ID and a thinking level, and hands them to Pi unchanged, so every provider and model Pi supports works without a mapping layer and Pi's documentation applies as written.

A Runner authenticates to its provider as Pi does: with a key from the environment, or with a sign-in to the provider's subscription, such as Sign in with ChatGPT, which the Runner's `login` command saves in its data directory and Pi refreshes. A host without a browser signs in by opening the sign-in page on any device and pasting back the address the page ends on. A sign-in belongs to one person: the Runner runs that person's own jobs on it, on a host only that person controls. OpenAI's Sign in with ChatGPT Terms allow requests from "expressly authorized automations or background processes" with "express consent before background use", from "a remote runtime only that user controls", with tokens stored "local and under the user's control", and forbid "using one user's subscription to fulfill another user's requests". `login` states before the sign-in that the Runner will use it in the background. A Runner that runs other people's jobs uses a key. `login` signs in to whichever provider the Runner names, as Pi offers each provider's sign-in; whether a provider's terms allow unattended use is for its operator to check.

## Considered options

A model format of our own, such as a single `"provider/model"` string with our own name for the thinking level, would need a mapping kept in step with Pi and would buy nothing while Pi is the only harness; a common format can be designed when a second harness exists.

API keys only, the earlier choice, held while subscription sign-ins needed a browser on the host and their terms covered only interactive use. Sign in with ChatGPT's terms allow automations the user has expressly authorized, and Pi's sign-in accepts the pasted address, so an unattended host can sign in once and keep running.

Reconsider when a provider's terms exclude unattended use of its sign-in, or when Runners shared by several people need a subscription of their own.
